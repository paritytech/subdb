use std::path::PathBuf;
use std::fs::{File, OpenOptions};
use memmap::MmapMut;
use parity_scale_codec::{Encode, Decode};
use smallvec::SmallVec;
use log::{info, trace};

use crate::datum_size::DatumSize;
use crate::types::{
	KeyType, EntryIndex, TableIndex, SimpleWriter
};
use crate::index_item::{IndexItem, IndexEntry, ContentAddress};
use crate::table::{Table, TableItemIndex, RefCount, TableItemCount};

/// Error type.
#[derive(Debug, derive_more::Display, derive_more::From)]
pub enum Error {
	/// An I/O error.
	#[display(fmt="I/O error: {}", _0)]
	Io(std::io::Error),

	/// Metadata is bad.
	#[display(fmt="Bad metadata")]
	BadMetadata,

	/// Unsupported version.
	#[display(fmt="Unsupported version")]
	UnsupportedVersion,
}
impl std::error::Error for Error {}

/// The options builder.
pub struct Options {
	path: PathBuf,
	key_bytes: usize,
	index_bits: usize,
}

type Version = u32;
const CURRENT_VERSION: Version = 1;

impl Options {
	/// Create a new instance.
	pub fn new() -> Self {
		Self {
			key_bytes: 4,
			index_bits: 16,
			path: Default::default(),
		}
	}

	/// Create a new instance, providing a path.
	pub fn from_path(path: PathBuf) -> Self {
		Self {
			key_bytes: 4,
			index_bits: 16,
			path
		}
	}

	/// Set the number of bytes to use for the index key (default: 4).
	pub fn key_bytes(mut self, key_bytes: usize) -> Self {
		self.key_bytes = key_bytes;
		self.index_bits = self.index_bits.min(key_bytes * 8);
		self
	}

	/// Set the number of bits to use for the index (default: 24).
	pub fn index_bits(mut self, index_bits: usize) -> Self {
		self.index_bits = index_bits;
		self.key_bytes = self.key_bytes.max(index_bits / 8);
		self
	}

	/// Set the path in which the database should be opened.
	pub fn path(mut self, path: PathBuf) -> Self {
		self.path = path;
		self
	}

	/// Open the database or create one with the configured options if it doesn't yet exist.
	pub fn open<K: KeyType>(self) -> Result<Database<K>, Error> {
		Database::open(self.path, self.key_bytes, self.index_bits)
	}
}

pub struct Database<K: KeyType> {
	#[allow(dead_code)]
	path: PathBuf,
	#[allow(dead_code)]
	index_file: File,
	index: MmapMut,
	sized_tables: Vec<Vec<Table<K>>>,
	suffix_len: usize,
	key_bytes: usize,
	index_mask: usize,
	index_full_bytes: usize,

	item_count: usize,
	item_size: usize,
//	oversize_tables: HashMap<usize, Table<K>>,
	_dummy: std::marker::PhantomData<K>,
}

impl<K: KeyType> Drop for Database<K> {
	fn drop(&mut self) {
		self.commit();
	}
}

impl<K: KeyType> Database<K> {
	/// Alters an index item in the index table store according to the given `f` function.
	fn mutate_item<R>(&mut self, index: usize, f: impl FnOnce(&mut IndexItem) -> R) -> R {
		let data = &mut self.index[index * self.item_size..(index + 1) * self.item_size];
		let mut entry = IndexItem::decode(&mut &data[..], self.suffix_len)
			.expect("Database corrupted?!");
		let r = f(&mut entry);
		entry.encode_to(&mut SimpleWriter(data, 0), self.suffix_len);
		r
	}

	/// Reads and returns an index item from the index table store.
	fn read_item(&self, index: usize) -> IndexItem {
		let data = &self.index[index * self.item_size..(index + 1) * self.item_size];
		let r = IndexItem::decode(&mut &data[..], self.suffix_len).expect("Database corrupted?!");
		trace!(target: "index", "read_item({}): {} -> {:?}", index, hex::encode(data), r);
		r
	}

	/// Writes a given index item to the index table store.
	fn write_item(&mut self, index: usize, entry: IndexItem) {
		let data = &mut self.index[index * self.item_size..(index + 1) * self.item_size];
		entry.encode_to(&mut SimpleWriter(data, 0), self.suffix_len);
		trace!(target: "index", "write_item({}): {:?} -> {}", index, entry, hex::encode(data));
	}

	/// Creates a new content table of `datum_size`.
	fn new_table(&mut self, datum_size: DatumSize) -> (TableIndex, &mut Table<K>) {
		let s = <u8>::from(datum_size);
		let table_index = self.sized_tables[s as usize].len();
		let table_path = self.table_path(s, table_index);
		self.sized_tables[s as usize].push(Table::open(table_path, datum_size));
		(table_index, &mut self.sized_tables[s as usize][table_index])
	}

	/// Generates the file name of a content table with `size_class` and `table_index`.
	fn table_name(size_class: u8, table_index: TableIndex) -> String {
		format!("{}-{}.content", size_class, table_index)
	}

	/// Generates the path for a content table with `size_class` and `table_index`.
	fn table_path(&self, size_class: u8, table_index: TableIndex) -> PathBuf {
		let mut table_path = self.path.clone();
		table_path.push(&Self::table_name(size_class, table_index));
		table_path
	}

	/// Determines the `index` (first location where it should be found in the index table) and
	/// the `key_suffix` for a given key `hash`.
	fn index_suffix_of(&self, hash: &K) -> (usize, SmallVec<[u8; 4]>) {
		let index = u64::decode(&mut hash.as_ref())
			.expect("Hash must be at least a u64") as usize;
		(index & self.index_mask, hash.as_ref()[self.index_full_bytes..self.key_bytes].into())
	}

	/// Attempt to run a function `f` on the probable `IndexEntry` found which represents the
	/// entry for `hash` in the index.
	///
	/// NOTE: It does *not* check that the `hash` really is in this index. Everything is done to
	/// ensure the highest probability that it is, but it is theoretically possible (if rather
	/// unrealistic) that it might be some other value (under expected loads this should be a
	/// roughly 1 in 4 billion chance, but hey - you never know). If it turns out not to be, then
	/// the function `f` may end up getting called multiple times. This will probably never happen
	/// outside of testing/toy environments.
	fn with_item_try<R>(
		&self,
		hash: &K,
		mut f: impl FnMut(IndexEntry) -> Result<R, ()>
	) -> Option<R> {
		let (mut index, suffix) = self.index_suffix_of(hash);
		trace!(target: "index", "Finding item; primary index {}; suffix: {:?}", index, suffix);
		for correction in 0.. {
			let item = self.read_item(index);
			trace!(target: "index", "Checking {:?}", item);
			if let Some(entry) = item.maybe_entry {
				if entry.key_correction == correction && entry.key_suffix == suffix {
					// Almost certainly the correct item.
					trace!(target: "index", "Found probable item: {:?}", entry);
					// Actually ensure it's the correct item.
					if let Ok(result) = f(entry) {
						return Some(result);
					}
				}
			}
			// Check for a past collision...
			if item.skipped_count == 0 {
				// No collision - item not there.
				return None
			}
			index = (index + 1) % self.item_count;
		}
		unreachable!()
	}

	/// Get the raw reference to an item's content value, optionally checking its hash to ensure
	/// it's the right item.
	fn item_ref(&self, address: &ContentAddress, check_hash: Option<&K>) -> Result<&[u8], ()> {
		match address.datum_size {
			DatumSize::Oversize => unimplemented!(),
			DatumSize::Size(s) => {
				self.sized_tables[s as usize][address.content_table]
					.item_ref(address.entry_index as TableItemIndex, check_hash)
			}
		}
	}

	/// Get the reference count for an item, optionally checking its hash to ensure
	/// it's the right item.
	fn item_ref_count(&self, address: &ContentAddress, check_hash: Option<&K>) -> Result<RefCount, ()> {
		match address.datum_size {
			DatumSize::Oversize => unimplemented!(),
			DatumSize::Size(s) => {
				self.sized_tables[s as usize][address.content_table]
					.item_ref_count(address.entry_index as TableItemIndex, check_hash)
			}
		}
	}

	/// Allocate space to store an item's contents and return its content address.
	///
	/// - `datum_size` is the size class of the item.
	/// - `key` is the hash key of the item.
	/// - `actual_size` is its real size, never more than `datum_size.size()`.
	fn allocate(&mut self, datum_size: DatumSize, key: &K, actual_size: usize) -> ContentAddress {
		match datum_size {
			DatumSize::Oversize => unimplemented!(),
			DatumSize::Size(s) => {
				for (content_table, table) in self.sized_tables[s as usize].iter_mut().enumerate() {
					if let Some(entry_index) = table.allocate(key, actual_size) {
						return ContentAddress { datum_size, content_table, entry_index: entry_index as EntryIndex };
					}
				}
				// Out of space - would create a new table
				let (content_table, table) = self.new_table(datum_size);
				let entry_index = table.allocate(key, actual_size).expect("Freshly created");
				ContentAddress { datum_size, content_table, entry_index: entry_index as EntryIndex }
			}
		}
	}

	/// Increment the references for an item given its content `address` and optionally checking
	/// that its key hash is the expected `check_hash`.
	fn bump(&mut self, address: &ContentAddress, check_hash: Option<&K>) -> Result<RefCount, ()> {
		match address.datum_size {
			DatumSize::Oversize => unimplemented!(),
			DatumSize::Size(s) => {
				self.sized_tables[s as usize][address.content_table]
					.bump(address.entry_index as TableItemIndex, check_hash)
			}
		}
	}

	/// Decrement the references for an item given its content `address` and optionally checking
	/// that its key hash is the expected `check_hash`. If they are decremented to zero then the
	/// storage used for the item will be freed.
	fn free(&mut self, address: &ContentAddress, check_hash: Option<&K>) -> Result<RefCount, ()> {
		match address.datum_size {
			DatumSize::Oversize => unimplemented!(),
			DatumSize::Size(s) => {
				self.sized_tables[s as usize][address.content_table]
					.free(address.entry_index as TableItemIndex, check_hash)
			}
		}
	}

	/// Open a database if it already exists and create a new one if not.
	pub fn open(path: PathBuf, mut key_bytes: usize, mut index_bits: usize) -> Result<Self, Error> {
		assert!(!path.is_file(), "Path must be a directory or not exist.");
		if !path.is_dir() {
			std::fs::create_dir_all(path.clone())?;
		}

		{
			// Sort out metadata.

			let mut metadata_file_name = path.clone();
			metadata_file_name.push("metadata.subdb");
			let already_existed = metadata_file_name.is_file();

			#[derive(Encode, Decode)]
			struct MetadataV1 {
				key_bytes: u32,
				index_bits: u32,
			}

			if already_existed {
				// Read metadata.
				let metadata = std::fs::read(metadata_file_name)?;
				let mut input = &metadata[..];

				let magic = <[u8; 4]>::decode(&mut input).map_err(|_| Error::BadMetadata)?;
				if &magic != b"SBDB" {
					return Err(Error::BadMetadata);
				}
				let version = Version::decode(&mut input).map_err(|_| Error::BadMetadata)?;
				if version != CURRENT_VERSION {
					return Err(Error::UnsupportedVersion);
				}
				let fields = MetadataV1::decode(&mut input).map_err(|_| Error::BadMetadata)?;
				key_bytes = fields.key_bytes as usize;
				index_bits = fields.index_bits as usize;

				info!("Opening existing SubDB [{} bytes/{}-bit]", key_bytes, index_bits);
			} else {
				// Write metadata.
				let fields = MetadataV1 {
					key_bytes: key_bytes as u32,
					index_bits: index_bits as u32,
				};
				(b"SBDB", CURRENT_VERSION, fields)
					.using_encoded(|e| std::fs::write(metadata_file_name, e))?;
				info!("Creating new SubDB [{} bytes/{}-bit]", key_bytes, index_bits);
			}
		}

		let mut index_file_name = path.clone();
		index_file_name.push("index.subdb");
		let index_file = OpenOptions::new()
			.read(true)
			.write(true)
			.create(true)
			.open(&index_file_name)
			.expect("Path must be writable.");

		let index_full_bytes = index_bits / 8;
		let suffix_len = key_bytes - index_full_bytes;
		let index_mask = ((1u128 << index_bits as u128) - 1) as usize;
		let item_size = 2 + 1 + 4 + suffix_len;
		let item_count = 1 << index_bits;

		index_file.set_len((item_count * item_size) as u64).expect("Path must be writable.");
		let index = unsafe {
			MmapMut::map_mut(&index_file).expect("Path must be writable.")
		};

		let sized_tables = (0u8..127).map(|size| (0usize..)
			.map(|table_index| {
				let mut table_path = path.clone();
				table_path.push(&Self::table_name(size, table_index));
				table_path
			})
			.take_while(|table_path| table_path.is_file())
			.map(|table_path| Table::open(table_path, DatumSize::from(size)))
			.collect()
		).collect();

		Ok(Self {
			path, index, index_file, sized_tables, key_bytes, suffix_len, index_mask,
			index_full_bytes, item_size, item_count, _dummy: Default::default()
		})
	}

	pub fn commit(&mut self) {
		self.index.flush().expect("Flush errored?");
		for tables in self.sized_tables.iter_mut() {
			for table in tables.iter_mut() {
				table.commit();
			}
		}
	}

	pub fn info(&self) -> Vec<((DatumSize, usize), (TableItemCount, TableItemCount, usize))> {
		self.sized_tables.iter()
			.enumerate()
			.map(|(z, tables)| (DatumSize::from(z as u8), tables))
			.flat_map(|(datum_size, tables)| tables.iter()
				.enumerate()
				.map(|(index, table)| ((datum_size, index), (table.available(), table.used(), table.bytes_used())))
				.collect::<Vec<_>>()
			)
			.collect()
	}

	pub fn get(&self, hash: &K) -> Option<Vec<u8>> {
		self.with_item_try(hash, |entry| self.item_ref(&entry.address, Some(hash)))
			.map(|d| d.to_vec())
	}

	pub fn get_ref_count(&self, hash: &K) -> RefCount {
		self.with_item_try(hash, |entry| self.item_ref_count(&entry.address, Some(hash)))
			.unwrap_or(0)
	}

	pub fn put(&mut self, data: &[u8]) -> K {
		let key = K::from_data(data);
		self.put_with_hash(&key, data);
		key
	}

	pub fn put_with_hash(&mut self, hash: &K, data: &[u8]) {
		let (index, key_suffix) = self.index_suffix_of(hash);
		let mut key_correction = 0;
		let mut insert_index = index;
		trace!(target: "index", "Inserting data {:?} with primary index {:?}",
			std::str::from_utf8(data).map_or_else(|_| hex::encode(data), |s| s.to_owned()),
			insert_index
		);
		loop {
			let mut item = self.read_item(insert_index);
			if let Some(ref mut e) = item.maybe_entry {
				if &e.key_suffix == &key_suffix && e.key_correction == key_correction {
					// Same item (almost certainly) - just need to bump the ref count on the
					// data.
					// We check that this is actually the right item, though.
					if self.bump(&e.address, Some(hash)).is_ok() {
						trace!(target: "index", "Bumped.");
						break;
					}
				}
			} else {
				// Nothing there - insert the new item.
				let datum_size = DatumSize::nearest(data.len());
				let address = self.allocate(datum_size, hash, data.len());
				match address.datum_size {
					DatumSize::Oversize => unimplemented!(),
					DatumSize::Size(s) => {
						self.sized_tables[s as usize][address.content_table]
							.item_mut(address.entry_index as TableItemIndex)
					}
				}.copy_from_slice(data);
				item.maybe_entry = Some(IndexEntry {
					key_suffix,
					address,
					key_correction,
				});
				trace!(target: "index", "Written {:?} at index {:?}", item, insert_index);
				self.write_item(insert_index, item);
				break;
			}
			// Collision - flag the item as skipped and continue around loop.
			trace!(target: "index", "Collision at index {:?} with {:?}", insert_index, item);
			item.skipped_count += 1;
			self.write_item(insert_index, item);
			key_correction += 1;
			insert_index = (insert_index + 1) % self.item_count;
		}
	}

	pub fn remove(&mut self, hash: &K) -> Result<RefCount, ()> {
		let (orig_index, suffix) = self.index_suffix_of(hash);
		let mut index = orig_index;
		trace!(target: "index", "Removing item; primary index {}; suffix: {:?}", index, suffix);
		for correction in 0.. {
			let item = self.read_item(index);
			trace!(target: "index", "Checking {:?}", item);
			if let Some(entry) = item.maybe_entry {
				if entry.key_correction == correction && entry.key_suffix == suffix {
					// Almost certainly the correct item.
					if let Ok(refs_left) = self.free(&entry.address, Some(hash)) {
						trace!(target: "index", "Found and freed item: {:?}", entry);
						if refs_left == 0 {
							let item = IndexItem {
								skipped_count: item.skipped_count,
								maybe_entry: None,
							};
							trace!(target: "index", "Expunging index: {:?} {:?}", index, item);
							self.write_item(index, item);
							for i in orig_index..orig_index + correction {
								trace!(target: "index", "Decrementing skipped trail for {}", i % self.item_count);
								self.mutate_item(
									i % self.item_count,
									|item| item.skipped_count = item.skipped_count.checked_sub(1)
										.expect("Skip count underflow. Database corruption?"));
							}
						}
						return Ok(refs_left)
					}
				}
			}
			// Check for a past collision...
			if item.skipped_count == 0 {
				// No collision - item not there.
				return Err(())
			}
			index = (index + 1) % self.item_count;
		}
		unreachable!()
	}
}
