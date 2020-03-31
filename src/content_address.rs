use std::fmt;
use smallvec::{SmallVec, smallvec};
use parity_scale_codec::{self as codec, Encode, Decode, Codec};
use crate::types::{TableIndex, EntryIndex, EncodedSize};
use crate::datum_size::DatumSize;

#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Encode, Decode)]
pub struct CompactContentAddress(u32);

impl fmt::Debug for CompactContentAddress {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "{:x?} ({:?})", self.0, ContentAddress::from(*self))
	}
}

impl EncodedSize for CompactContentAddress {
	fn encoded_size() -> usize { 4 }
}

#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Debug)]
pub struct ContentAddress {
	/// The size, or possibly unsized.
	pub datum_size: DatumSize,
	/// The instance of the content table that the data is stored in.
	pub content_table: TableIndex,
	/// The index of the entry within the content table that is the data.
	pub entry_index: EntryIndex,
}

impl<'a> From<&'a ContentAddress> for CompactContentAddress {
	fn from(x: &'a ContentAddress) -> Self {
		let a = u8::from(x.datum_size) as u32;
		let b = (x.entry_index + x.datum_size.contents_entries() * x.content_table) as u32;
		Self(a | (b << 6))
	}
}

impl From<ContentAddress> for CompactContentAddress {
	fn from(x: ContentAddress) -> Self {
		From::from(&x)
	}
}

impl From<CompactContentAddress> for ContentAddress {
	fn from(x: CompactContentAddress) -> Self {
		let datum_size = DatumSize::from((x.0 % 64) as u8);
		let entries = datum_size.contents_entries();
		let rest = (x.0 >> 6) as usize;
		let content_table = rest / entries;
		let entry_index = rest % entries;
		Self { datum_size, content_table, entry_index }
	}
}

impl EncodedSize for ContentAddress {
	fn encoded_size() -> usize { 4 }
}

impl Encode for ContentAddress {
	fn encode_to<O: codec::Output>(&self, output: &mut O) {
		CompactContentAddress::from(self).encode_to(output)
	}
}

impl Decode for ContentAddress {
	fn decode<I: codec::Input>(input: &mut I) -> Result<Self, codec::Error> {
		Ok(CompactContentAddress::decode(input)?.into())
	}
}

#[test]
fn content_addresses_encode_encode_ok() {
	let a = ContentAddress { datum_size: DatumSize::Size(0), content_table: 1, entry_index: 2 };
	assert_eq!(a.datum_size.size(), Some(32));
	assert_eq!(a.datum_size.contents_entries(), 65536);
	let b = CompactContentAddress::from(&a);
	assert_eq!(b, CompactContentAddress(65538 * 64));
	let a2 = ContentAddress::from(b);
	assert_eq!(a, a2);
}
