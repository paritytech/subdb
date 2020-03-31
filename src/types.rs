use blake2_rfc::blake2b::blake2b;
use parity_scale_codec::{self as codec, Encode, Decode};

pub type TableIndex = usize;
pub type EntryIndex = usize;

pub trait KeyType: AsRef<[u8]> + AsMut<[u8]> + Encode + Decode + Eq + PartialEq + Clone + std::fmt::Debug {
	const SIZE: usize;
	fn from_data(data: &[u8]) -> Self;
}

pub trait EncodedSize: Encode {
	fn encoded_size() -> usize;
}

macro_rules! do_array {
	($n:tt $( $rest:tt )*) => {
		impl KeyType for [u8; $n] {
			const SIZE: usize = $n;
			fn from_data(data: &[u8]) -> Self {
				let mut r = Self::default();
				r.copy_from_slice(&blake2b($n, &[], data).as_bytes()[..]);
				r
			}
		}
		do_array!{ $($rest)* }
	};
	() => {}
}

do_array!(
	1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26 27 28 29 30 31 32
);

pub struct SimpleWriter<'a>(pub &'a mut[u8], pub usize);
impl<'a> codec::Output for SimpleWriter<'a> {
	fn write(&mut self, d: &[u8]) {
		self.0[self.1..self.1 + d.len()].copy_from_slice(d);
		self.1 += d.len();
	}
}
