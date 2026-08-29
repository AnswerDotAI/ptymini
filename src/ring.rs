//! Bounded byte buffer with absolute offsets.
//!
//! `start`..`end` is the retained window; `end` is the total bytes ever
//! appended. The offsets are the contract: reads page deterministically, a
//! reader behind the window learns exactly what it lost, and reading past the
//! end returns empty with the offset unmoved.

use std::collections::VecDeque;

pub struct Ring {
    pub max_bytes: usize,
    chunks: VecDeque<(u64, Vec<u8>)>,
    pub start: u64,
    pub end: u64,
}

impl Ring {
    pub fn new(max_bytes: usize) -> Ring { Ring { max_bytes, chunks: VecDeque::new(), start: 0, end: 0 } }

    pub fn len(&self) -> usize { (self.end - self.start) as usize }

    pub fn is_empty(&self) -> bool { self.len() == 0 }

    pub fn append(&mut self, data: &[u8]) {
        self.chunks.push_back((self.end, data.to_vec()));
        self.end += data.len() as u64;
        while self.len() > self.max_bytes {
            let overflow = self.len() - self.max_bytes;
            let front_len = self.chunks.front().unwrap().1.len();
            if overflow >= front_len {
                let (off, _) = self.chunks.pop_front().unwrap();
                self.start = off + front_len as u64;
            } else {
                let (off, d) = self.chunks.front_mut().unwrap();
                *d = d.split_off(overflow);
                *off += overflow as u64;
                self.start = *off;
            }
        }
    }

    /// Read from absolute `offset`: (data, new_offset, dropped). An offset
    /// behind `start` counts the missed bytes as dropped; `max_bytes_out`
    /// caps the returned data for paging.
    pub fn read_from(&self, offset: u64, max_bytes_out: Option<usize>) -> (Vec<u8>, u64, u64) {
        let dropped = self.start.saturating_sub(offset);
        let cur = offset.max(self.start);
        let mut out: Vec<u8> = Vec::new();
        for (off, d) in &self.chunks {
            if off + d.len() as u64 <= cur { continue; }
            let from = cur.saturating_sub(*off) as usize;
            let mut piece = &d[from..];
            if let Some(m) = max_bytes_out { piece = &piece[..piece.len().min(m - out.len())]; }
            if piece.is_empty() { break; }
            out.extend_from_slice(piece);
            if let Some(m) = max_bytes_out
                && out.len() >= m
            { break; }
        }
        let new_off = cur + out.len() as u64;
        (out, new_off, dropped)
    }
}
