//! Statement threads, kept once a statement is done and given to the next.
//!
//! A new thread starts cold: glibc gives a finished thread's stack back to the kernel, so the
//! next one faults its pages in again, and the allocator's per-thread cache starts empty.

use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError, mpsc};

/// More idle than this and a finished thread exits; each keeps the stack it touched.
const IDLE: usize = 4;

/// A job, handed what parks its thread; it says whether it panicked, since a thread that saw a
/// panic is not kept, and parks before it reports, so the statement after it finds the thread.
type Body<'a> = Box<dyn FnOnce(&dyn Fn(bool)) + Send + 'a>;
type Job = Body<'static>;

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
        let job: Body<'env> = Box::new(move |park| {
            let r = std::panic::catch_unwind(std::panic::AssertUnwindSafe(f));
            park(r.is_err());
            out.set(|o| *o = Some(r));
            drop(out);
            running.set(|n| *n -= 1);
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
    let kept = std::cell::Cell::new(true);
    let park = |panicked: bool| {
        // A statement that failed between the two coercions left its plan, and its tables, here.
        super::coercion::forget();
        let mut parked = lock(&PARKED);
        kept.set(!panicked && parked.len() < IDLE);
        if kept.get() {
            parked.push(tx.clone());
        }
    };
    while let Ok(job) = rx.recv() {
        job(&park);
        if !kept.get() {
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_job_borrows_and_the_thread_is_kept() {
        let words = ["a".to_string(), "b".to_string()];
        let first = scope(|s| {
            s.spawn(|| (words.join(""), std::thread::current().id()))
                .unwrap()
                .join()
                .unwrap()
        });
        assert_eq!(first.0, "ab");
        // Another test may take the parked thread first, so only some later job need land on it.
        let again = (0..64).any(|_| {
            scope(|s| {
                s.spawn(|| std::thread::current().id())
                    .unwrap()
                    .join()
                    .unwrap()
            }) == first.1
        });
        assert!(again);
    }

    #[test]
    fn a_panic_comes_back_and_its_thread_is_not_kept() {
        let id = std::sync::Mutex::new(None);
        let r = scope(|s| {
            s.spawn(|| {
                *id.lock().unwrap() = Some(std::thread::current().id());
                panic!("boom")
            })
            .unwrap()
            .join()
        });
        assert!(r.is_err());
        let id = id.into_inner().unwrap();
        for _ in 0..64 {
            let other = scope(|s| {
                s.spawn(|| std::thread::current().id())
                    .unwrap()
                    .join()
                    .unwrap()
            });
            assert_ne!(Some(other), id);
        }
    }

    #[test]
    fn the_scope_waits_for_an_unjoined_job() {
        let done = std::sync::atomic::AtomicBool::new(false);
        scope(|s| {
            s.spawn(|| {
                std::thread::sleep(std::time::Duration::from_millis(50));
                done.store(true, std::sync::atomic::Ordering::SeqCst);
            })
            .unwrap();
        });
        assert!(done.load(std::sync::atomic::Ordering::SeqCst));
    }
}
