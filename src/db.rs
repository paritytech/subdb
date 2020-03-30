use std::path::PathBuf;
use std::fs::{File, OpenOptions};
use std::collections::{HashMap, BTreeMap};
use memmap::MmapMut;
use parity_scale_codec::{Encode, Decode};
use smallvec::SmallVec;

use crate::datum_size::DatumSize;
use crate::types::{
	KeyType, EntryIndex, TableIndex, SimpleWriter
};
use crate::index_item::{IndexItem, IndexEntry, ContentAddress};
use crate::table::{Table, TableItemIndex, RefCount, TableItemCount};

pub struct SubDb<K: KeyType> {
	#[allow(dead_code)] path: PathBuf,
	#[allow(dead_code)] index_file: File,
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

impl<K: KeyType> Drop for SubDb<K> {
	fn drop(&mut self) {
		self.commit();
	}
}

impl<K: KeyType> SubDb<K> {

	#[allow(dead_code)] fn mutate_item<R>(&mut self, index: usize, f: impl FnOnce(&mut IndexItem) -> R) -> R {
		let data = &mut self.index[index * self.item_size..(index + 1) * self.item_size];
		let mut entry = IndexItem::decode(&mut &data[..], self.suffix_len)
			.expect("Database corrupted?!");
		let r = f(&mut entry);
		entry.encode_to(&mut SimpleWriter(data, 0), self.suffix_len);
		r
	}

	#[allow(dead_code)] fn read_item(&self, index: usize) -> IndexItem {
		let data = &self.index[index * self.item_size..(index + 1) * self.item_size];
		let r = IndexItem::decode(&mut &data[..], self.suffix_len).expect("Database corrupted?!");
		println!("read_item({}): {} -> {:?}", index, hex::encode(data), r);
		r
	}

	#[allow(dead_code)] fn write_item(&mut self, index: usize, entry: IndexItem) {
		let data = &mut self.index[index * self.item_size..(index + 1) * self.item_size];
		entry.encode_to(&mut SimpleWriter(data, 0), self.suffix_len);
		println!("write_item({}): {:?} -> {}", index, entry, hex::encode(data));
	}

	/// Finds the next place to put a piece of data of the given size. Doesn't actually write
	/// anything yet.
	fn find_place(&self, datum_size: DatumSize) -> ContentAddress {
		match datum_size {
			DatumSize::Oversize => unimplemented!(),
			DatumSize::Size(s) => {
				for (content_table, table) in self.sized_tables[s as usize].iter().enumerate() {
					if let Some(entry_index) = table.next_free() {
						return ContentAddress { datum_size, content_table, entry_index: entry_index as EntryIndex };
					}
				}
				// Out of space - would create a new table
				let content_table = self.sized_tables[s as usize].len() as TableIndex;
				ContentAddress { datum_size, content_table, entry_index: 0 }
			}
		}
	}

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

	fn new_table(&mut self, datum_size: DatumSize) -> (TableIndex, &mut Table<K>) {
		let s = <u8>::from(datum_size);
		let table_index = self.sized_tables[s as usize].len();
		let table_path = self.table_path(s, table_index);
		self.sized_tables[s as usize].push(Table::open(table_path, datum_size));
		(table_index, &mut self.sized_tables[s as usize][table_index])
	}

	pub fn open(path: PathBuf, key_bytes: usize, index_bits: usize) -> Self {
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

		Self {
			path, index, index_file, sized_tables, key_bytes, suffix_len, index_mask,
			index_full_bytes, item_size, item_count, _dummy: Default::default()
		}
	}

	fn table_name(size: u8, table_index: TableIndex) -> String {
		format!("{}-{}.content", size, table_index)
	}

	fn table_path(&self, size: u8, table_index: TableIndex) -> PathBuf {
		let mut table_path = self.path.clone();
		table_path.push(&Self::table_name(size, table_index));
		table_path
	}

	pub fn commit(&mut self) {
		self.index.flush().expect("Flush errored?");
		for tables in self.sized_tables.iter_mut() {
			for table in tables.iter_mut() {
				table.commit();
			}
		}
	}

	fn index_suffix_of(&self, hash: &K) -> (usize, SmallVec<[u8; 4]>) {
		let index = u64::decode(&mut hash.as_ref())
			.expect("Hash must be at least a u64") as usize;
		(index & self.index_mask, hash.as_ref()[self.index_full_bytes..self.key_bytes].into())
	}

	// NOTE: the `skipped_count` sticks around, even when an item is removed.

	fn find(&self, hash: &K) -> Option<IndexEntry> {
		let (mut index, suffix) = self.index_suffix_of(hash);
		println!("Finding item; primary index {}; suffix: {:?}", index, suffix);
		for correction in 0.. {
			let item = self.read_item(index);
			println!("Checking {:?}", item);
			if let Some(entry) = item.maybe_entry {
				if entry.key_correction == correction && entry.key_suffix == suffix {
					// Almost certainly the correct item.
					// TODO: actually ensure it's the correct item.
					println!("Found item: {:?}", entry);
					return Some(entry)
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

	#[allow(dead_code)] pub fn get(&self, hash: &K) -> Option<Vec<u8>> {
		self.find(hash).map(|entry| self.item_ref(&entry.address).to_vec())
	}

	#[allow(dead_code)] pub fn get_ref_count(&self, hash: &K) -> RefCount {
		self.find(hash).map_or(0, |entry| self.item_ref_count(&entry.address))
	}

	#[allow(dead_code)] pub fn put(&mut self, data: &[u8]) -> K {
		let key = K::from_data(data);
		self.put_with_hash(&key, data);
		key
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

	#[allow(dead_code)] pub fn remove(&mut self, hash: &K) -> Result<RefCount, ()> {
		let (orig_index, suffix) = self.index_suffix_of(hash);
		let mut index = orig_index;
		println!("Removing item; primary index {}; suffix: {:?}", index, suffix);
		for correction in 0.. {
			let item = self.read_item(index);
			println!("Checking {:?}", item);
			if let Some(entry) = item.maybe_entry {
				if entry.key_correction == correction && entry.key_suffix == suffix {
					// Almost certainly the correct item.
					if let Ok(refs_left) = self.free(&entry.address, Some(hash)) {
						println!("Found and freed item: {:?}", entry);
						if refs_left == 0 {
							let item = IndexItem {
								skipped_count: item.skipped_count,
								maybe_entry: None,
							};
							println!("Expunging index: {:?} {:?}", index, item);
							self.write_item(index, item);
							for i in orig_index..orig_index + correction {
								println!("Decrementing skipped trail for {}", i % self.item_count);
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

	fn item_mut(&mut self, address: &ContentAddress) -> &mut[u8] {
		match address.datum_size {
			DatumSize::Oversize => unimplemented!(),
			DatumSize::Size(s) => {
				self.sized_tables[s as usize][address.content_table]
					.item_mut(address.entry_index as TableItemIndex)
			}
		}
	}

	fn item_ref(&self, address: &ContentAddress) -> &[u8] {
		match address.datum_size {
			DatumSize::Oversize => unimplemented!(),
			DatumSize::Size(s) => {
				self.sized_tables[s as usize][address.content_table]
					.item_ref(address.entry_index as TableItemIndex)
			}
		}
	}

	fn item_ref_count(&self, address: &ContentAddress) -> RefCount {
		match address.datum_size {
			DatumSize::Oversize => unimplemented!(),
			DatumSize::Size(s) => {
				self.sized_tables[s as usize][address.content_table]
					.item_ref_count(address.entry_index as TableItemIndex)
			}
		}
	}

	fn checked_bump(&mut self, address: &ContentAddress, hash: &K) -> Result<RefCount, ()> {
		match address.datum_size {
			DatumSize::Oversize => unimplemented!(),
			DatumSize::Size(s) => {
				self.sized_tables[s as usize][address.content_table]
					.checked_bump(address.entry_index as TableItemIndex, hash)
			}
		}
	}

	fn free(&mut self, address: &ContentAddress, hash: Option<&K>) -> Result<RefCount, ()> {
		match address.datum_size {
			DatumSize::Oversize => unimplemented!(),
			DatumSize::Size(s) => {
				self.sized_tables[s as usize][address.content_table]
					.free(address.entry_index as TableItemIndex, hash)
			}
		}
	}

	pub fn put_with_hash(&mut self, hash: &K, data: &[u8]) {
		let (index, key_suffix) = self.index_suffix_of(hash);
		let mut key_correction = 0;
		let mut insert_index = index;
		println!("Inserting data {:?} with primary index {:?}",
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
					if self.checked_bump(&e.address, hash).is_ok() {
						println!("Bumped.");
						break;
					}
				}
			} else {
				// Nothing there - insert the new item.
				let datum_size = DatumSize::nearest(data.len());
				let address = self.allocate(datum_size, hash, data.len());
				self.item_mut(&address).copy_from_slice(data);
				item.maybe_entry = Some(IndexEntry {
					key_suffix,
					address,
					key_correction,
				});
				println!("Written {:?} at index {:?}", item, insert_index);
				self.write_item(insert_index, item);
				break;
			}
			// Collision - flag the item as skipped and continue around loop.
			println!("Collision at index {:?} with {:?}", insert_index, item);
			item.skipped_count += 1;
			self.write_item(insert_index, item);
			key_correction += 1;
			insert_index = (insert_index + 1) % self.item_count;
		}
	}
}
