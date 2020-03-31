use std::path::PathBuf;
use std::fs::{File, OpenOptions};
use memmap::MmapMut;
use parity_scale_codec::Decode;
use smallvec::SmallVec;
use log::trace;

use crate::types::{KeyType, SimpleWriter};
use crate::index_item::{IndexItem, IndexEntry, ContentAddress};
use crate::Error;

// TODO: make generic over ContentAddress

pub struct Index<K: KeyType> {
	#[allow(dead_code)]
	file: File,
	index: MmapMut,

	suffix_len: usize,
	key_bytes: usize,
	index_mask: usize,
	index_full_bytes: usize,

	item_count: usize,
	item_size: usize,
	_dummy: std::marker::PhantomData<K>,
}

impl<K: KeyType> Drop for Index<K> {
	fn drop(&mut self) {
		self.commit();
	}
}

impl<K: KeyType> Index<K> {

	/// Open a database if it already exists and create a new one if not.
	pub fn open(filename: PathBuf, key_bytes: usize, index_bits: usize) -> Result<Self, Error> {
		let file = OpenOptions::new()
			.read(true)
			.write(true)
			.create(true)
			.open(&filename)
			.expect("Path must be writable.");

		let index_full_bytes = index_bits / 8;
		let suffix_len = key_bytes - index_full_bytes;
		let index_mask = ((1u128 << index_bits as u128) - 1) as usize;
		let item_size = 2 + 1 + 4 + suffix_len;
		let item_count = 1 << index_bits;

		file.set_len((item_count * item_size) as u64).expect("Path must be writable.");
		let index = unsafe {
			MmapMut::map_mut(&file).expect("Path must be writable.")
		};

		Ok(Self {
			index, file, key_bytes, suffix_len, index_mask,
			index_full_bytes, item_size, item_count, _dummy: Default::default()
		})
	}

	pub fn commit(&mut self) {
		self.index.flush().expect("Flush errored?");
	}

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
	pub fn with_item_try<R>(
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

	pub fn edit_in<R>(
		&mut self,
		hash: &K,
		mut f: impl FnMut(Option<&ContentAddress>) -> Result<(Option<ContentAddress>, R), ()>,
	) -> R {
		let (primary_index, key_suffix) = self.index_suffix_of(hash);
		let mut key_correction = 0;
		let mut try_index = primary_index;
		trace!(target: "index", "    Primary index {:?}", try_index);
		loop {
			let mut item = self.read_item(try_index);
			if let Some(ref mut e) = item.maybe_entry {
				if &e.key_suffix == &key_suffix && e.key_correction == key_correction {
					if let Ok(result) = f(Some(&e.address)) {
						return result.1
					}
				}
			} else {
				let (maybe_address, result) = f(None)
					.expect("May not return an Err when provided with None");
				if let Some(address) = maybe_address {
					item.maybe_entry = Some(IndexEntry {
						key_suffix,
						address,
						key_correction,
					});
					trace!(target: "index", "Written {:?} at index {:?}", item, try_index);
					self.write_item(try_index, item);
				} else {
					// Undo changing those skipped counts.
					self.decrement_skip_counts(primary_index, key_correction);
				}
				return result;
			}
			// Collision - flag the item as skipped and continue around loop.
			trace!(target: "index", "Collision at index {:?} with {:?}", try_index, item);
			item.skipped_count += 1;
			self.write_item(try_index, item);
			key_correction += 1;
			try_index = (try_index + 1) % self.item_count;
		}
	}

	fn decrement_skip_counts(&mut self, begin: usize, count: usize) {
		for i in begin..begin + count {
			trace!(target: "index", "Unincrementing skipped trail for {}", i % self.item_count);
			self.mutate_item(
				i % self.item_count,
				|item| item.skipped_count = item.skipped_count.checked_sub(1)
					.expect("Skip count underflow. Database corruption?"));
		}
	}

	pub fn edit_out<R>(
		&mut self,
		hash: &K,
		mut if_maybe_found: impl FnMut(ContentAddress) -> Result<(Option<Option<ContentAddress>>, R), ()>,
	) -> Result<R, ()> {
		let (primary_index, suffix) = self.index_suffix_of(hash);
		let mut try_index = primary_index;
		trace!(target: "index", "Removing item; primary index {}; suffix: {:?}", try_index, suffix);
		for correction in 0.. {
			let item = self.read_item(try_index);
			trace!(target: "index", "Checking {:?}", item);
			if let Some(entry) = item.maybe_entry {
				if entry.key_correction == correction && entry.key_suffix == suffix {
					// Almost certainly the correct item.
					match if_maybe_found(entry.address) {
						Err(()) => {}
						Ok((None, result)) => return Ok(result),
						Ok((Some(Some(address)), result)) => {
							let item = IndexItem {
								skipped_count: item.skipped_count,
								maybe_entry: Some(IndexEntry { address, .. entry }),
							};
							self.write_item(try_index, item);
							return Ok(result);
						}
						Ok((Some(None), result)) => {
							let item = IndexItem {
								skipped_count: item.skipped_count,
								maybe_entry: None,
							};
							trace!(target: "index", "Expunging index: {:?} {:?}", try_index, item);
							self.write_item(try_index, item);
							self.decrement_skip_counts(primary_index, correction);
							return Ok(result);
						}
					}
				}
			}
			// Check for a past collision...
			if item.skipped_count == 0 {
				// No collision - item not there.
				return Err(())
			}
			try_index = (try_index + 1) % self.item_count;
		}
		unreachable!()
	}
}
