//! What a Parquet column chunk's pages say they decompress to, read from the page headers.
//!
//! parquet-rs allocates a page's `uncompressed_page_size` before it decompresses, so a header that
//! claims 2 GiB over a few bytes costs 2 GiB whatever the footer said. The crate has no limit to
//! set and keeps its header type private, hence the Thrift compact reader here: enough of it to
//! take the two sizes and step over everything else.

/// Nesting of structs and lists in a page header: its statistics sit two deep.
const MAX_DEPTH: u8 = 8;

/// The sum of `uncompressed_page_size` over the pages in `len` bytes at `start`, or `None` where
/// they do not parse as pages filling exactly that range.
pub(crate) fn decoded_size(file: &[u8], start: i64, len: i64) -> Option<u64> {
    let start = usize::try_from(start).ok()?;
    let end = start.checked_add(usize::try_from(len).ok()?)?;
    let chunk = file.get(start..end)?;
    let (mut at, mut total) = (0usize, 0u64);
    while at < chunk.len() {
        let mut r = Reader {
            buf: &chunk[at..],
            pos: 0,
        };
        let (decoded, stored) = r.page_header()?;
        at = at.checked_add(r.pos)?.checked_add(stored)?;
        if at > chunk.len() {
            return None;
        }
        total = total.checked_add(decoded)?;
    }
    Some(total)
}

struct Reader<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl Reader<'_> {
    fn byte(&mut self) -> Option<u8> {
        let b = *self.buf.get(self.pos)?;
        self.pos += 1;
        Some(b)
    }

    fn varint(&mut self) -> Option<u64> {
        let mut v = 0u64;
        for shift in (0..64).step_by(7) {
            let b = self.byte()?;
            v |= u64::from(b & 0x7f) << shift;
            if b & 0x80 == 0 {
                return Some(v);
            }
        }
        None
    }

    fn size(&mut self) -> Option<u64> {
        let v = self.varint()?;
        let n = (v >> 1) as i64 ^ -((v & 1) as i64);
        u64::try_from(i32::try_from(n).ok()?).ok()
    }

    fn advance(&mut self, n: u64) -> Option<()> {
        let to = self.pos.checked_add(usize::try_from(n).ok()?)?;
        (to <= self.buf.len()).then(|| self.pos = to)
    }

    /// `(uncompressed_page_size, compressed_page_size)`, fields 2 and 3, with the reader left
    /// after the header.
    fn page_header(&mut self) -> Option<(u64, usize)> {
        let (mut decoded, mut stored) = (None, None);
        let mut id = 0i64;
        loop {
            let head = self.byte()?;
            if head == 0 {
                break;
            }
            id = self.field_id(head, id)?;
            match (id, head & 0x0f) {
                (2, 5) => decoded = Some(self.size()?),
                (3, 5) => stored = Some(self.size()?),
                (_, kind) => self.skip(kind, 0)?,
            }
        }
        Some((decoded?, usize::try_from(stored?).ok()?))
    }

    fn field_id(&mut self, head: u8, last: i64) -> Option<i64> {
        match head >> 4 {
            0 => {
                let v = self.varint()?;
                Some((v >> 1) as i64 ^ -((v & 1) as i64))
            }
            delta => Some(last + i64::from(delta)),
        }
    }

    fn skip(&mut self, kind: u8, depth: u8) -> Option<()> {
        if depth > MAX_DEPTH {
            return None;
        }
        match kind {
            // A boolean field's value is in its type.
            1 | 2 => Some(()),
            3 => self.advance(1),
            4..=6 => self.varint().map(|_| ()),
            7 => self.advance(8),
            8 => {
                let n = self.varint()?;
                self.advance(n)
            }
            9 | 10 => {
                let head = self.byte()?;
                let n = match head >> 4 {
                    15 => self.varint()?,
                    n => u64::from(n),
                };
                for _ in 0..n {
                    match head & 0x0f {
                        // In a list a boolean is a byte.
                        1 | 2 => self.advance(1)?,
                        kind => self.skip(kind, depth + 1)?,
                    }
                }
                Some(())
            }
            12 => {
                let mut id = 0i64;
                loop {
                    let head = self.byte()?;
                    if head == 0 {
                        return Some(());
                    }
                    id = self.field_id(head, id)?;
                    self.skip(head & 0x0f, depth + 1)?;
                }
            }
            13 => self.advance(16),
            // A map has no place in a page header.
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_header_gives_its_sizes_and_its_own_length() {
        // type 0, uncompressed 300, compressed 2, crc 7, then a data page header of four i32s.
        let header = [
            0x15, 0x00, 0x15, 0xd8, 0x04, 0x15, 0x04, 0x15, 0x0e, 0x1c, 0x15, 0x02, 0x15, 0x00,
            0x15, 0x00, 0x15, 0x00, 0x00, 0x00,
        ];
        let mut file = header.to_vec();
        file.extend([0xaa, 0xbb]);
        let twice = [file.clone(), file.clone()].concat();
        assert_eq!(decoded_size(&file, 0, file.len() as i64), Some(300));
        assert_eq!(decoded_size(&twice, 0, twice.len() as i64), Some(600));
        // A page that runs past its chunk, and a chunk that ends inside a header.
        assert_eq!(decoded_size(&file, 0, file.len() as i64 - 1), None);
        assert_eq!(decoded_size(&twice, 0, file.len() as i64 + 3), None);
        assert_eq!(decoded_size(&file, -1, 4), None);
        assert_eq!(decoded_size(&file, 0, i64::MAX), None);
    }

    #[test]
    fn a_list_claiming_more_items_than_bytes_ends_the_walk() {
        // Field 5 as a list of 2^28 structs, and no bytes after the count.
        let header = [0x59, 0xfc, 0x80, 0x80, 0x80, 0x80, 0x01];
        assert_eq!(decoded_size(&header, 0, header.len() as i64), None);
    }
}
