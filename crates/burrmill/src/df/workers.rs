//! Statement threads, kept once a statement is done and given to the next.
//!
//! A new thread starts cold: glibc gives a finished thread's stack back to the kernel, so the
//! next one faults its pages in again, and the allocator's per-thread cache starts empty.

use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError, mpsc};

/// More idle than this and a finished thread exits; each keeps the stack it touched.
const IDLE: usize = 4;

/// A job, saying whether it panicked; a thread that saw a panic is not kept.
type Job = Box<dyn FnOnce() -> bool + Send + 'static>;

static PARKED: Mutex<Vec<mpsc::Sender<Job>>> = Mutex::new(Vec::new());

fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(PoisonError::into_inner)
}

/// A count and the condvar that says when it changed.
#[derive(Default)]
struct Signal<T> {
    value: Mutex<T>,
    changed: Condvar,
}

impl<T> Signal<T> {
    fn wait_until(&self, done: impl Fn(&mut T) -> bool) -> MutexGuard<'_, T> {
        let mut v = lock(&self.value);
        while !done(&mut v) {
            v = self.changed.wait(v).unwrap_or_else(PoisonError::into_inner);
        }
        v
    }

    fn set(&self, f: impl FnOnce(&mut T)) {
        f(&mut lock(&self.value));
        self.changed.notify_all();
    }
}

pub(super) struct Scope<'env> {
    running: Arc<Signal<usize>>,
    _env: std::marker::PhantomData<&'env mut &'env ()>,
}

pub(super) struct Handle<T>(Arc<Signal<Option<std::thread::Result<T>>>>);

impl<T> Handle<T> {
    pub(super) fn join(self) -> std::thread::Result<T> {
        self.0.wait_until(|r| r.is_some()).take().expect("set")
    }
}

/// As [`std::thread::scope`]: every job spawned in it has finished when it returns, unwinding
/// included, which is what lets a job borrow from the caller.
pub(super) fn scope<'env, R>(f: impl FnOnce(&Scope<'env>) -> R) -> R {
    struct Wait(Arc<Signal<usize>>);
    impl Drop for Wait {
        fn drop(&mut self) {
            drop(self.0.wait_until(|n| *n == 0));
        }
    }
    let s = Scope {
        running: Arc::default(),
        _env: std::marker::PhantomData,
    };
    let _wait = Wait(Arc::clone(&s.running));
    f(&s)
}

impl<'env> Scope<'env> {
    /// `f` on a parked statement thread, or on a new one when none is.
    pub(super) fn spawn<T: Send + 'env>(
        &self,
        f: impl FnOnce() -> T + Send + 'env,
    ) -> std::io::Result<Handle<T>> {
        let out: Arc<Signal<Option<std::thread::Result<T>>>> = Arc::default();
        let handle = Handle(Arc::clone(&out));
        let running = Arc::clone(&self.running);
        running.set(|n| *n += 1);
        let job: Box<dyn FnOnce() -> bool + Send + 'env> = Box::new(move || {
            let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(f));
            let panicked = r.is_err();
            out.set(|o| *o = Some(r));
            drop(out);
            running.set(|n| *n -= 1);
            panicked
        });
        // SAFETY: `scope` neither returns nor unwinds past its frame until the count is zero, and
        // a job lets go of everything it borrowed before it takes itself off the count.
        let mut job: Job = unsafe { std::mem::transmute(job) };
        while let Some(tx) = lock(&PARKED).pop() {
            match tx.send(job) {
                Ok(()) => return Ok(handle),
                Err(mpsc::SendError(back)) => job = back,
            }
        }
        let (tx, rx) = mpsc::channel::<Job>();
        tx.send(job).expect("receiver held");
        match std::thread::Builder::new()
            .name("burrmill-statement".into())
            .stack_size(super::PLAN_STACK_BYTES)
            .spawn(move || work(tx, rx))
        {
            Ok(_) => Ok(handle),
            // The job went with the closure, unrun, so it never took itself off.
            Err(e) => {
                self.running.set(|n| *n -= 1);
                Err(e)
            }
        }
    }
}

fn work(tx: mpsc::Sender<Job>, rx: mpsc::Receiver<Job>) {
    while let Ok(job) = rx.recv() {
        if job() {
            return;
        }
        let mut parked = lock(&PARKED);
        if parked.len() >= IDLE {
            return;
        }
        parked.push(tx.clone());
    }
}
