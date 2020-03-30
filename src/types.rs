use blake2_rfc::blake2b::blake2b;
use parity_scale_codec::{self as codec, Encode, Decode};

pub type TableIndex = usize;
pub type EntryIndex = usize;

pub trait KeyType: AsRef<[u8]> + AsMut<[u8]> + Encode + Decode + Eq + Clone + std::fmt::Debug {
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

impl KeyType for [u8; 1] {
	const SIZE: usize = 1;
	fn from_data(data: &[u8]) -> Self {
		[data.first().cloned().unwrap_or(0)]
	}
}

pub struct SimpleWriter<'a>(pub &'a mut[u8], pub usize);
impl<'a> codec::Output for SimpleWriter<'a> {
	fn write(&mut self, d: &[u8]) {
		self.0[self.1..self.1 + d.len()].copy_from_slice(d);
		self.1 += d.len();
	}
}
