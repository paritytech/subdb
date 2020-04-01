use std::path::PathBuf;
use std::fs::{OpenOptions};
use std::fmt::Debug;
use std::convert::TryInto;
use memmap::MmapMut;
use parity_scale_codec::Codec;
use smallvec::SmallVec;
use log::trace;

use crate::types::{KeyType, SimpleWriter, EncodedSize};
use crate::index_item::{IndexItem, IndexEntry};
use crate::Error;

pub struct Index<K, V> {
	index: MmapMut,

	suffix_len: usize,
	key_bytes: usize,
	index_mask: usize,
	index_bits: usize,
	index_full_bytes: usize,

	item_count: usize,
	item_size: usize,

	skipped_count_watermark: u8,
	key_correction_watermark: usize,
	_dummy: std::marker::PhantomData<(K, V)>,
}

impl<K, V> Drop for Index<K, V> {
	fn drop(&mut self) {
		self.commit();
	}
}

impl<K, V> Index<K, V> {
	pub fn commit(&mut self) {
		self.index.flush().expect("Flush errored?");
	}
}

impl<K: KeyType, V: Codec + EncodedSize + Debug> Index<K, V> {
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
		let item_size = 2 + 1 + V::encoded_size() + suffix_len;
		let item_count = 1 << index_bits;

		file.set_len((item_count * item_size) as u64).expect("Path must be writable.");
		let index = unsafe {
			MmapMut::map_mut(&file).expect("Path must be writable.")
		};

		Ok(Self {
			index, key_bytes, suffix_len, index_mask, skipped_count_watermark: 0,
			key_correction_watermark: 0,
			index_bits, index_full_bytes, item_size, item_count, _dummy: Default::default()
		})
	}

	/// Open a database if it already exists and create a new one if not.
	pub fn anonymous(key_bytes: usize, index_bits: usize) -> Result<Self, Error> {
		let index_full_bytes = index_bits / 8;
		let suffix_len = key_bytes - index_full_bytes;
		let index_mask = ((1u128 << index_bits as u128) - 1) as usize;
		let item_size = 2 + 1 + V::encoded_size() + suffix_len;
		let item_count = 1 << index_bits;

		let index = MmapMut::map_anon(item_count * item_size).expect("Out of memory?");

		Ok(Self {
			index, key_bytes, suffix_len, index_mask, skipped_count_watermark: 0,
			key_correction_watermark: 0,
			index_bits, index_full_bytes, item_size, item_count, _dummy: Default::default()
		})
	}

	/// Alters an index item in the index table store according to the given `f` function.
	fn mutate_item<R>(&mut self, index: usize, f: impl FnOnce(&mut IndexItem<V>) -> R) -> R {
		let data = &mut self.index[index * self.item_size..(index + 1) * self.item_size];
		let mut entry = IndexItem::decode(&mut &data[..], self.suffix_len)
			.expect("Database corrupted?!");
		let r = f(&mut entry);
		entry.encode_to(&mut SimpleWriter(data, 0), self.suffix_len);
		r
	}

	/// Reads and returns an index item from the index table store.
	fn read_item(&self, index: usize) -> IndexItem<V> {
		let data = &self.index[index * self.item_size..(index + 1) * self.item_size];
		let r = IndexItem::decode(&mut &data[..], self.suffix_len).expect("Database corrupted?!");
		trace!(target: "index", "read_item({}): {} -> {:?}", index, hex::encode(data), r);
		r
	}

	/// Writes a given index item to the index table store.
	fn write_item(&mut self, index: usize, entry: IndexItem<V>) {
		let data = &mut self.index[index * self.item_size..(index + 1) * self.item_size];
		entry.encode_to(&mut SimpleWriter(data, 0), self.suffix_len);
		trace!(target: "index", "write_item({}): {:?} -> {}", index, entry, hex::encode(data));
	}

	/// Determines the `index` (first location where it should be found in the index table) and
	/// the `key_suffix` for a given key `hash`.
	///
	/// It's up to the caller to ensure that `hash` is big enough. It needs to be both at least
	/// `self.key_bytes` and at least the next power of two from the `index_bits` divided by 8.
	/// If the `hash.len()` is at least 8 then you'll probably be fine.
	fn index_suffix_of(&self, hash: &[u8]) -> (usize, SmallVec<[u8; 4]>) {
		let index = match self.index_bits {
			0 => 0,
			1..=8 => hash[0] as usize,
			9..=16 => u16::from_le_bytes(hash[..2].try_into().expect("hash len must be >=2")) as usize,
			17..=32 => u32::from_le_bytes(hash[..4].try_into().expect("hash len must be >=4")) as usize,
			32..=64 => u64::from_le_bytes(hash[..8].try_into().expect("hash len must be >=8")) as usize,
			_ => unimplemented!("Too big an index!"),
		};
		(index & self.index_mask, hash[self.index_full_bytes..self.key_bytes].into())
	}

	/// Determines the first part of the hash/key from the index and the key-suffix. A partial
	/// reversion of `index_suffix_of`.
	fn key_prefix(&self, index: usize, suffix: &[u8]) -> SmallVec<[u8; 8]> {
		let mut prefix: SmallVec<[u8; 8]> = match self.index_full_bytes {
			0 => SmallVec::new(),
			1 => (index as u8).to_le_bytes().as_ref()[..self.index_full_bytes].into(),
			2 => (index as u16).to_le_bytes().as_ref()[..self.index_full_bytes].into(),
			3 | 4 => (index as u32).to_le_bytes().as_ref()[..self.index_full_bytes].into(),
			5 | 6 | 7 | 8 => (index as u64).to_le_bytes().as_ref()[..self.index_full_bytes].into(),
			_ => unimplemented!("Too big an index!"),
		};
		prefix.extend_from_slice(suffix);
		prefix
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
		mut f: impl FnMut(IndexEntry<V>) -> Result<R, ()>
	) -> Option<R> {
		let (mut index, suffix) = self.index_suffix_of(hash.as_ref());
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
		f: impl FnMut(Option<&V>) -> Result<(Option<V>, R), ()>,
	) -> Result<R, Error> {
		let (primary_index, key_suffix) = self.index_suffix_of(hash.as_ref());
		self.edit_in_position(primary_index, key_suffix, f)
	}

	fn edit_in_position<R>(
		&mut self,
		primary_index: usize,
		key_suffix: SmallVec<[u8; 4]>,
		mut f: impl FnMut(Option<&V>) -> Result<(Option<V>, R), ()>,
	) -> Result<R, Error> {
		let mut key_correction = 0;
		let mut try_index = primary_index;
		trace!(target: "index", "    Primary index {:?}", try_index);
		const MAX_CORRECTION: usize = 32768;
		for _ in 0..MAX_CORRECTION.min(self.item_count) {
			let mut item = self.read_item(try_index);
			if let Some(ref mut e) = item.maybe_entry {
				if &e.key_suffix == &key_suffix && e.key_correction == key_correction {
					if let Ok(result) = f(Some(&e.address)) {
						return Ok(result.1)
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
				return Ok(result);
			}
			// Collision - flag the item as skipped and continue around loop.
			trace!(target: "index", "Collision at index {:?} with {:?}", try_index, item);

			item.skipped_count = if let Some(n) = item.skipped_count.checked_add(1) { n } else { break };
			self.skipped_count_watermark = self.skipped_count_watermark.max(item.skipped_count);
			self.write_item(try_index, item);
			key_correction += 1;
			self.key_correction_watermark = self.key_correction_watermark.max(key_correction);
			try_index = (try_index + 1) % self.item_count;
		}

		// If we're here, then the index must be getting full: either we've had to increment an
		// item's skipped count too much (because it was a preferential space to more than 255 other
		// items), or we've had to stray too many items far from the primary index (more than 32767
		// or the number of items in the index).
		//
		// We will bump the size of the index and retry.
		Err(Error::IndexFull)
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
		mut if_maybe_found: impl FnMut(V) -> Result<(Option<Option<V>>, R), ()>,
	) -> Result<R, ()> {
		let (primary_index, suffix) = self.index_suffix_of(hash.as_ref());
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

	pub fn from_existing(filename: PathBuf, source: &Self, key_bytes: usize, index_bits: usize) -> Result<Self, Error> {
		// Open new index.
		let mut result = Index::open(filename, key_bytes, index_bits)?;

		if key_bytes <= source.key_bytes {
			for i in 0..source.item_count {
				let item = source.read_item(i);
				if let Some(entry) = item.maybe_entry {
					let index = (i + source.item_count - entry.key_correction) % source.item_count;
					let mut partial_key = source.key_prefix(index, &entry.key_suffix);
					// we put zeros on the end since they won't affect LE representations and we extend
					// in order to guarantee that it's big enough for `index_suffix_of`.
					assert!(partial_key.len() >= result.key_bytes);
					partial_key.resize(8, 0);
					let (index, key_suffix) = result.index_suffix_of(partial_key.as_ref());
					let mut the_address = Some(entry.address);
					result.edit_in_position(index & result.index_mask, key_suffix, |maybe_same| {
						if maybe_same.is_some() {
							Err(())
						} else {
							Ok((Some(the_address.take().expect("This branch can only be called once")), ()))
						}
					})?;
				}
			}
		} else {
			unimplemented!();
		}
		Ok(result)
	}

	pub fn next_size(&self) -> (usize, usize) {
		let index_bits = self.index_bits + 1;
		let key_bytes = self.key_bytes.max((self.index_bits + 7) / 8);
		(key_bytes, index_bits)
	}

	pub fn take_watermarks(&mut self) -> (u8, usize) {
		let r = (self.skipped_count_watermark, self.key_correction_watermark);
		self.skipped_count_watermark = 0;
		self.key_correction_watermark = 0;
		r
	}
}
