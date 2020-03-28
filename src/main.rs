use std::fs::OpenOptions;
use std::path::PathBuf;
use memmap::{MmapMut, MmapOptions};
use std::mem::size_of;
use blake2_rfc::blake2b::blake2b;
use parity_scale_codec::{self as codec, Encode, Decode};

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub enum DatumSize {
	Oversize,
	Size(u8),
}
impl DatumSize {
	/// The size of a datum, or `None` if the datum is oversized.
	pub fn size(&self) -> Option<usize> {
		match *self {
			DatumSize::Oversize => None,
			DatumSize::Size(size) => {
				assert!(size < 127);
				let exp = size as usize / 8;
				let tweak = size as usize % 8;
				let base = 32usize << exp;
				Some(base + base / 8 * tweak)
			}
		}
	}

	/// The nearest datum size for `s`.
	pub fn nearest(s: usize) -> Self {
		if s <= 32 {
			return DatumSize::Size(0)
		}
		if s > 1835008 {
			return DatumSize::Oversize
		}
		let exp = size_of::<usize>() as usize * 8 - s.leading_zeros() as usize - 6;
		let base = 32usize << exp;
		let incr = base / 8;
		let incrs = (s - base + incr - 1) / incr;
		DatumSize::Size((exp * 8 + incrs) as u8)
	}

	/// How many entries should be in a contents table whose items are this size?
	pub fn contents_entries(&self) -> usize {
		// max total size per contents table = 16MB
		// max number of items in contents table = 65536
		if let Some(size) = self.size() {
			(16777216 / size).max(65536).min(1)
		} else {
			return 1
		}
	}

	/// How big should the data part of the contents file be?
	///
	/// `None` if the contents are oversize - in this case, it's just one item.
	pub fn contents_size(&self) -> Option<usize> {
		self.size().map(|s| s * self.contents_entries())
	}
}

impl From<u8> for DatumSize {
	fn from(x: u8) -> Self {
		if x < 127 {
			DatumSize::Size(x)
		} else {
			DatumSize::Oversize
		}
	}
}

impl From<DatumSize> for u8 {
	fn from(x: DatumSize) -> u8 {
		match x {
			DatumSize::Oversize => 127,
			DatumSize::Size(x) => x,
		}
	}
}

#[test]
fn datum_size_works() {
	assert_eq!(DatumSize::from(0).size().unwrap(), 32);
	assert_eq!(DatumSize::from(1).size().unwrap(), 36);
	assert_eq!(DatumSize::from(2).size().unwrap(), 40);
	assert_eq!(DatumSize::from(7).size().unwrap(), 60);
	assert_eq!(DatumSize::from(8).size().unwrap(), 64);
	assert_eq!(DatumSize::from(9).size().unwrap(), 72);
	assert_eq!(DatumSize::from(15).size().unwrap(), 120);
	assert_eq!(DatumSize::from(16).size().unwrap(), 128);
	assert_eq!(DatumSize::from(17).size().unwrap(), 144);
	assert_eq!(DatumSize::from(24).size().unwrap(), 256);
	assert_eq!(DatumSize::from(32).size().unwrap(), 512);
	assert_eq!(DatumSize::from(40).size().unwrap(), 1_024);
	assert_eq!(DatumSize::from(48).size().unwrap(), 2_048);
	assert_eq!(DatumSize::from(56).size().unwrap(), 4_096);
	assert_eq!(DatumSize::from(64).size().unwrap(), 8_192);
	assert_eq!(DatumSize::from(72).size().unwrap(), 16_384);
	assert_eq!(DatumSize::from(80).size().unwrap(), 32_768);
	assert_eq!(DatumSize::from(88).size().unwrap(), 65_536);
	assert_eq!(DatumSize::from(96).size().unwrap(), 131_072);
	assert_eq!(DatumSize::from(104).size().unwrap(), 262_144);
	assert_eq!(DatumSize::from(126).size().unwrap(), 1_835_008);
	assert_eq!(DatumSize::from(127).size(), None);

	assert_eq!(DatumSize::nearest(0).size().unwrap(), 32);
	assert_eq!(DatumSize::nearest(29).size().unwrap(), 32);
	assert_eq!(DatumSize::nearest(30).size().unwrap(), 32);
	assert_eq!(DatumSize::nearest(31).size().unwrap(), 32);
	assert_eq!(DatumSize::nearest(32).size().unwrap(), 32);
	assert_eq!(DatumSize::nearest(33).size().unwrap(), 36);
	assert_eq!(DatumSize::nearest(34).size().unwrap(), 36);
	assert_eq!(DatumSize::nearest(35).size().unwrap(), 36);
	assert_eq!(DatumSize::nearest(36).size().unwrap(), 36);
	assert_eq!(DatumSize::nearest(37).size().unwrap(), 40);
	assert_eq!(DatumSize::nearest(38).size().unwrap(), 40);
	assert_eq!(DatumSize::nearest(39).size().unwrap(), 40);
	assert_eq!(DatumSize::nearest(40).size().unwrap(), 40);
	assert_eq!(DatumSize::nearest(62).size().unwrap(), 64);
	assert_eq!(DatumSize::nearest(63).size().unwrap(), 64);
	assert_eq!(DatumSize::nearest(64).size().unwrap(), 64);
	assert_eq!(DatumSize::nearest(65).size().unwrap(), 72);
	assert_eq!(DatumSize::nearest(66).size().unwrap(), 72);
	assert_eq!(DatumSize::nearest(67).size().unwrap(), 72);
	assert_eq!(DatumSize::nearest(68).size().unwrap(), 72);
	assert_eq!(DatumSize::nearest(69).size().unwrap(), 72);
	assert_eq!(DatumSize::nearest(70).size().unwrap(), 72);
	assert_eq!(DatumSize::nearest(71).size().unwrap(), 72);
	assert_eq!(DatumSize::nearest(72).size().unwrap(), 72);
	assert_eq!(DatumSize::nearest(73).size().unwrap(), 80);
	assert_eq!(DatumSize::nearest(1_835_007).size().unwrap(), 1_835_008);
	assert_eq!(DatumSize::nearest(1_835_008).size().unwrap(), 1_835_008);
	assert_eq!(DatumSize::nearest(1_835_009).size(), None);
}

pub trait KeyType: AsRef<[u8]> + AsMut<[u8]> {
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

pub struct SubDb<K> {
	path: PathBuf,
	index_file: std::fs::File,
	index: MmapMut,
	_dummy: std::marker::PhantomData<K>,
}

pub type TableIndex = usize;
pub type EntryIndex = usize;

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

const KEY_BYTES: usize = 4;
const INDEX_BYTES: usize = 3;
const INDEX_COUNT: usize = 1 << (INDEX_BYTES * 8);
const INDEX_ITEM_SIZE: usize = 8;

struct SimpleWriter<'a>(&'a mut[u8], usize);
impl<'a> codec::Output for SimpleWriter<'a> {
	fn write(&mut self, d: &[u8]) {
		self.0[self.1..self.1 + d.len()].copy_from_slice(d);
		self.1 += d.len();
	}
}

// TODO: versioning.

impl<K: KeyType> SubDb<K> {

	fn mutate_entry<R>(&mut self, index: usize, f: impl FnOnce(&mut IndexItem) -> R) -> R {
		let data = &mut self.index[index * INDEX_ITEM_SIZE..(index + 1) * INDEX_ITEM_SIZE];
		let mut entry = IndexItem::decode(&mut &data[..]).expect("Database corrupted?!");
		let r = f(&mut entry);
		entry.encode_to(&mut SimpleWriter(data, 0));
		r
	}

	fn read_entry(&self, index: usize) -> IndexItem {
		let data = &self.index[index * INDEX_ITEM_SIZE..(index + 1) * INDEX_ITEM_SIZE];
		IndexItem::decode(&mut &data[..]).expect("Database corrupted?!")
	}

	fn write_entry(&mut self, index: usize, entry: IndexItem) {
		let data = &mut self.index[index * INDEX_ITEM_SIZE..(index + 1) * INDEX_ITEM_SIZE];
		entry.encode_to(&mut SimpleWriter(data, 0));
	}

	/// Finds the next place to put a piece of data of the given size. Doesn't actually write
	/// anything yet.
	fn find_place(&self, datum_size: DatumSize) -> (TableIndex, EntryIndex) {
		//TODO

		(0, 0)
	}

	pub fn new(path: PathBuf) -> Self {
		assert!(!path.is_file(), "Path must be a directory or not exist.");
		if !path.is_dir() {
			std::fs::create_dir_all(path.clone()).expect("Path must be writable.");
		}
		let mut index_file_name = path.clone();
		index_file_name.push("index.subdb");
		let index_file = OpenOptions::new()
			.read(true)
			.write(true)
			.create(true)
			.open(&index_file_name)
			.expect("Path must be writable.");
		index_file.set_len((INDEX_COUNT * INDEX_ITEM_SIZE) as u64).expect("Path must be writable.");
		let mut index = unsafe {
			MmapMut::map_mut(&index_file).expect("Path must be writable.")
		};
		Self { path, index, index_file, _dummy: Default::default() }
	}

	pub fn commit(&mut self) {
		self.index.flush();
	}

	fn index_of(hash: &K) -> usize {
		hash.as_ref().iter()
			.take(INDEX_BYTES)
			.fold(0, |a, &i| (a << 8) + i as usize)
	}

	// NOTE: the `skipped` flag needs to stick around, even when an item is removed.

	pub fn get(&self, hash: &K) -> Option<Vec<u8>> {
		let mut index = Self::index_of(hash);
		let maybe_entry: Option<IndexItem> = loop {
			let e: IndexItem = self.read_entry(index);
			if !e.is_empty() && &e.key == &hash.as_ref()[0..KEY_BYTES] {
				// Same item (almost certainly) - just need to bump the ref count on the
				// data.
				break Some(e)
			}
			// Check for a past collision...
			if !e.skipped {
				// No collision - item not there.
				return None
			}
		};

		maybe_entry.map(|entry| {
			// TODO: Lookup data from `entry`
			entry.encode()
		})
	}

	pub fn put(&mut self, data: &[u8]) -> K {
		let key = K::from_data(data);
		self.put_with_hash(&key, data);
		key
	}

	pub fn remove(&mut self, hash: &K) {
		unimplemented!()
	}

	pub fn put_with_hash(&mut self, hash: &K, data: &[u8]) {
		let index = Self::index_of(hash);

		let datum_size = DatumSize::nearest(data.len());
		let (content_table, entry_index) = self.find_place(datum_size);
		let mut key: [u8; KEY_BYTES] = Default::default();
		key.copy_from_slice(&hash.as_ref()[0..KEY_BYTES]);
		let mut item = IndexItem { key, skipped: false, datum_size, content_table, entry_index };

		let mut final_index = index;
		let already_there = loop {
			match self.mutate_entry(final_index, |e| {
				if !e.is_empty() {
					if &e.key == &item.key {
						// Same item (almost certainly) - just need to bump the ref count on the
						// data.
						item = e.clone();
						return Some(true)
					} else {
						// Collision - highly unlikely, but whatever... flag the item as skipped.
						e.skipped = true;
						return None
					}
				}
				// Nothing there - insert the new item.
				*e = item.clone();
				Some(false)
			}) {
				Some(x) => break(x),
				None => continue,
			}
		};

		if !already_there {
			// TODO: Create the new item at the place in `item` and put data.
		} else {
			// TODO: Bump the refcount of the place in `item`.
		}
	}
}

fn main() {
	let mut db = SubDb::<[u8; 32]>::new(PathBuf::from("/tmp/test"));
//	let key = db.put(b"Hello world!");
	let key = <[u8; 32] as KeyType>::from_data(b"Hello world!");
	dbg!(&key);
	let value = db.get(&key);
	dbg!(&value);
	let value = db.get(&Default::default());
	dbg!(&value);
	db.commit();
}