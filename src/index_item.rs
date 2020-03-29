use parity_scale_codec::{self as codec, Encode, Decode};
use crate::types::{TableIndex, EntryIndex, KEY_BYTES};
use crate::datum_size::DatumSize;

/// An item describing an entry in this database. It doesn't contain its data; only where to find
/// it. It fits in 8 bytes when encoded.
#[derive(Clone, Eq, PartialEq, Ord, PartialOrd)]
pub struct IndexItem {
	/// The first four bytes of the key (which should itself just be the result of a cryptographic
	/// hash).
	pub key: [u8; KEY_BYTES],
	/// The instance of the content table that the data is stored in.
	pub content_table: TableIndex,
	/// The index of the entry within the content table that is the data.
	pub entry_index: EntryIndex,
	/// The size, or possibly unsized.
	pub datum_size: DatumSize,
	/// If true then there was a collision in a previous write and this key was skipped over.
	pub skipped: bool,
}

impl IndexItem {
	pub fn is_empty(&self) -> bool {
		self.key == [0u8; 4] && self.content_table == 0 && self.entry_index == 0 && self.datum_size == DatumSize::Size(0)
	}
}

impl Decode for IndexItem {
	fn decode<I: codec::Input>(input: &mut I) -> Result<Self, codec::Error> {
		let key = <[u8; KEY_BYTES]>::decode(input)?;
		let size_location = u32::decode(input)?;
		let datum_size = DatumSize::from((size_location % 128) as u8);
		let skipped = (size_location & 128) != 0;
		let rest = (size_location / 256) as usize;
		let entry_index = rest % datum_size.contents_entries();
		let content_table = rest / datum_size.contents_entries();
		Ok(Self { key, datum_size, skipped, content_table, entry_index })
	}
}

impl Encode for IndexItem {
	fn encode_to<O: codec::Output>(&self, output: &mut O) {
		self.key.encode_to(output);
		let size_location = u8::from(self.datum_size) as u32
			+ if self.skipped { 128 } else { 0 }
			+ 256 * (
			self.entry_index + self.datum_size.contents_entries() * self.entry_index
		) as u32;
		size_location.encode_to(output);
	}
}
