use std::fmt;
use smallvec::{SmallVec, smallvec};
use parity_scale_codec::{self as codec, Encode, Decode};
use crate::types::{TableIndex, EntryIndex};
use crate::datum_size::DatumSize;

#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Encode, Decode)]
pub struct CompactContentAddress(u32);

impl fmt::Debug for CompactContentAddress {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "{:x?} ({:?})", self.0, ContentAddress::from(*self))
	}
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

/// An item possibly describing an entry in this database.
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Debug)]
pub struct IndexItem {
	/// The number of items currently in the database that would have been placed in this slot at
	/// preference, but had to go into a later slot do to this being occupied.
	pub skipped_count: u8,

	/// An entry, if there is one.
	pub maybe_entry: Option<IndexEntry>,
}

/// An item describing an entry in this database. It doesn't contain its data; only where to find
/// it. It fits in 8 bytes when encoded.
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd, Debug)]
pub struct IndexEntry {
	/// The number of items that had to be skipped from the slot derived from the key.
	/// Encodes to use 15 bits.
	pub key_correction: usize,

	/// Any incomplete or undefined bytes placed on the end of the key derivable from the index
	/// position to build it into the short-form key (currently 4 bytes but eventually this will be
	/// configurable)
	pub key_suffix: SmallVec<[u8; 4]>,

	/// Always 32-bit.
	pub address: ContentAddress,
}

impl IndexItem {
	/// None if the slot is empty.
	pub fn decode<I: codec::Input>(input: &mut I, suffix_len: usize) -> Result<Self, codec::Error> {
		let maybe_key_correction = <u16>::decode(input)?;
		let skipped_count = input.read_byte()?;

		if maybe_key_correction & 0b1000_0000_0000_0000u16 == 0 {
			// Unoccupied. Skip the rest of it and return none.
			for _ in 0..suffix_len + 4 { input.read_byte()?; }
			return Ok(Self { skipped_count, maybe_entry: None })
		}

		let key_correction = (maybe_key_correction & !0b1000_0000_0000_0000u16) as usize;

		let mut key_suffix = smallvec![0; suffix_len];
		input.read(&mut key_suffix[..])?;

		let address = ContentAddress::decode(input)?;
		let entry = IndexEntry { key_correction, key_suffix, address };

		Ok(Self { skipped_count, maybe_entry: Some(entry) })
	}

	pub fn encode_to<O: codec::Output>(&self, output: &mut O, suffix_len: usize) {
		if let Some(ref entry) = self.maybe_entry {
			// We set the MSB to indicate that the slot is taken.
			((entry.key_correction as u16) | 0b1000_0000_0000_0000u16).encode_to(output);
			output.push_byte(self.skipped_count);
			output.write(entry.key_suffix.as_ref());
			entry.address.encode_to(output);
		} else {
			output.push_byte(0);
			output.push_byte(0);
			output.push_byte(self.skipped_count);
			for _ in 0..suffix_len + 4 { output.push_byte(0); }
		}
	}}

#[test]
fn index_item_encodes_decodes_correctly() {
	let item = IndexItem {
		skipped_count: 0,
		maybe_entry: Some(IndexEntry {
			key_correction: 0,
			key_suffix: SmallVec::from(&[45][..]),
			address: ContentAddress {
				datum_size: DatumSize::Size(0),
				content_table: 0,
				entry_index: 0
			}
		}),
	};
	let mut encoded = Vec::<u8>::new();
	let e = item.encode_to(&mut encoded, 1);
	let item2 = IndexItem::decode(&mut &encoded[..], 1).unwrap();
	assert_eq!(item, item2);
}