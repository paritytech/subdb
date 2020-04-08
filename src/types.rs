use blake2_rfc::blake2b::blake2b;
use parity_scale_codec::{self as codec, Encode};
use std::fmt::Debug;

pub type TableIndex = usize;
pub type EntryIndex = usize;

pub trait KeyType: AsRef<[u8]> + AsMut<[u8]> + Default + Eq + PartialEq + Clone + Debug + Send + Sync {}

impl<
	T: AsRef<[u8]> + AsMut<[u8]> + Default + Eq + PartialEq + Clone + Debug + Send + Sync
> KeyType for T {}

pub trait EncodedSize: Encode {
	fn encoded_size() -> usize;
}

pub trait HashOutput: KeyType {
	fn from_data(data: &[u8]) -> Self;
}

#[derive(Default, Eq, PartialEq, Clone, Debug)]
pub struct Blake2Output<T>(pub T);

macro_rules! do_array {
	($n:tt $( $rest:tt )*) => {
		impl HashOutput for Blake2Output<[u8; $n]> {
			fn from_data(data: &[u8]) -> Self {
				let mut r = Self::default();
				r.as_mut().copy_from_slice(&blake2b($n, &[], data).as_bytes()[..]);
				r
			}
		}
		impl AsRef<[u8]> for Blake2Output<[u8; $n]> {
			fn as_ref(&self) -> &[u8] {
				&self.0[..]
			}
		}
		impl AsMut<[u8]> for Blake2Output<[u8; $n]> {
			fn as_mut(&mut self) -> &mut [u8] {
				&mut self.0[..]
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
