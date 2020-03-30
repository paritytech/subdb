use blake2_rfc::blake2b::blake2b;
use parity_scale_codec::{self as codec, Encode, Decode};

pub type TableIndex = usize;
pub type EntryIndex = usize;

pub const KEY_BYTES: usize = 4;
pub const INDEX_BYTES: usize = 3;
pub const INDEX_COUNT: usize = 1 << (INDEX_BYTES * 8);
pub const INDEX_ITEM_SIZE: usize = 8;

pub trait KeyType: AsRef<[u8]> + AsMut<[u8]> + Encode + Decode + Clone + std::fmt::Debug {
	const SIZE: usize;

	fn from_data(data: &[u8]) -> Self;
}

impl KeyType for [u8; 32] {
	const SIZE: usize = 32;

	fn from_data(data: &[u8]) -> Self {
		let mut r = Self::default();
		r.copy_from_slice(&blake2b(32, &[], data).as_bytes()[..]);
		r
	}
}

pub struct SimpleWriter<'a>(pub &'a mut[u8], pub usize);
impl<'a> codec::Output for SimpleWriter<'a> {
	fn write(&mut self, d: &[u8]) {
		self.0[self.1..self.1 + d.len()].copy_from_slice(d);
		self.1 += d.len();
	}
}
