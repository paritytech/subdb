use std::path::PathBuf;
use std::fs::OpenOptions;
use std::mem::size_of;
use std::cell::{RefCell, RefMut};
use memmap::{MmapMut, MmapOptions};
use parity_scale_codec::{self as codec, Encode, Decode};
use crate::types::{KeyType, SimpleWriter};
use crate::datum_size::DatumSize;
use std::ops::DerefMut;
use log::trace;

/// How many references a storage table item has.
pub type RefCount = u16;

/// Where in a storage table an item is.
pub type TableItemIndex = u16;

/// How many table items; must be able to store a range from 0 to TableItemIndex::max_value() + 1
/// inclusive, therefore needs the next biggest type up.
pub type TableItemCount = u32;

/// A time index for our LRU system.
pub type LruIndex = u64;

pub struct Table<K> {
	path: PathBuf,
	data: MmapMut,
	header_data: MmapMut,
	header: TableHeader,
	item_header_size: usize,
	item_size: usize,
	item_count: TableItemCount,
	value_size: usize,
	correction_factor: CorrectionFactor,

	maps: Vec<RefCell<Option<(MmapMut, LruIndex)>>>,
	lru_index: RefCell<LruIndex>,
	mapped: RefCell<usize>,

	_dummy: std::marker::PhantomData<K>,
}

/// Rather unsafe.
#[derive(Clone, Copy, Encode, Decode, Debug)]
struct TableHeader {
	/// The number of items used. Never more than `touched_count`.
	used: TableItemCount,
	/// Ignore if used == touched_count; otherwise it is the next free item.
	next_free: TableItemIndex,
	/// The number of unique slots that have been allocated at some point. Never more than
	/// `item_count`.
	///
	/// Item indices equal to this and less than `item_count` may be allocated in addition to the
	/// linked list starting at `next_free`.
	touched_count: TableItemCount,
}

#[derive(Copy, Clone, Eq, PartialEq)]
enum CorrectionFactor {
	None,
	U8,
	U16,
	U32,
}

#[derive(Clone, Debug)]
enum ItemHeader<K: Encode + Decode> {
	Allocated {
		/// Number of times this item has been inserted, without a corresponding remove, into the
		/// database.
		ref_count: RefCount,
		size_correction: u32,
		key: K,
	},
	Free(
		/// If `used < touched_count`, then the next free item's index. If the two are equal, then
		/// this is undefined.
		TableItemIndex,
	),
}

impl<K: Encode + Decode + Eq> ItemHeader<K> {
	fn as_next_free(&self) -> TableItemIndex {
		match self {
			ItemHeader::Free(next_free) => *next_free,
			ItemHeader::Allocated {..} => panic!("Free expected. Database corruption?"),
		}
	}

	fn as_allocation(&self, check_hash: Option<&K>) -> Result<(RefCount, usize), ()> {
		match self {
			ItemHeader::Allocated { ref_count, size_correction, key } => {
				if check_hash.map_or(true, |hash| hash == key) {
					Ok((*ref_count, *size_correction as usize))
				} else {
					Err(())
				}
			},
			ItemHeader::Free(_) => panic!("Allocated expected. Database corruption?"),
		}
	}

	#[allow(dead_code)]
	fn to_maybe_key(self) -> Option<K> {
		match self {
			ItemHeader::Allocated { key, .. } => Some(key),
			ItemHeader::Free(_) => None,
		}
	}

	fn decode<I: codec::Input>(input: &mut I, correction_factor: CorrectionFactor) -> Result<Self, codec::Error> {
		let ref_count = RefCount::decode(input)?;
		let size_correction = match correction_factor {
			CorrectionFactor::None => 0u32,
			CorrectionFactor::U8 => u8::decode(input)? as u32,
			CorrectionFactor::U16 => u16::decode(input)? as u32,
			CorrectionFactor::U32 => u32::decode(input)?,
		};
		Ok(if ref_count > 0 {
			Self::Allocated { ref_count, size_correction, key: K::decode(input)? }
		} else {
			Self::Free(TableItemIndex::decode(input)?)
		})
	}

	fn encode_to<O: codec::Output>(&self, output: &mut O, correction_factor: CorrectionFactor) {
		match self {
			ItemHeader::Allocated { ref_count, size_correction, key} => {
				assert!(*ref_count > 0);

				ref_count.encode_to(output);
				match correction_factor {
					CorrectionFactor::None => {},
					CorrectionFactor::U8 => (*size_correction as u8).encode_to(output),
					CorrectionFactor::U16 => (*size_correction as u16).encode_to(output),
					CorrectionFactor::U32 => (*size_correction as u32).encode_to(output),
				}
				key.encode_to(output);
			}
			ItemHeader::Free(index) => {
				(RefCount::default(), index).encode_to(output);
			}
		}
	}
}

impl<K: KeyType> Table<K> {
	pub fn commit(&mut self) {
		self.data.flush().expect("I/O Error");
	}

	pub fn open(path: PathBuf, datum_size: DatumSize) -> Self {
		assert!(!path.exists() || path.is_file(), "Path must either not exist or be a file.");

		let file = OpenOptions::new()
			.read(true)
			.write(true)
			.create(true)
			.open(&path)
			.expect("Path must be writable.");
		let len = file.metadata().expect("File must be readable").len();
		let value_size = datum_size.size().unwrap_or(0);
		let correction_factor = match datum_size.size_range().unwrap_or(0) {
			0 => CorrectionFactor::None,
			1..=255 => CorrectionFactor::U8,
			256..=65535 => CorrectionFactor::U16,
			_ => CorrectionFactor::U32,
		};
		let item_count = datum_size.contents_entries() as TableItemCount;
		let item_header_size = size_of::<RefCount>() + size_of::<u32>() + K::SIZE.max(size_of::<TableItemIndex>());
		let item_size = value_size + item_header_size;
		let table_header_size = size_of::<TableHeader>();
		let total_size = table_header_size + item_size * item_count as usize;

		assert!(len == 0 || len == total_size as u64, "File exists but length is unexpected");
		file.set_len(total_size as u64).expect("Path must be writable.");

		let header_data = unsafe {
			MmapOptions::new()
				.len(table_header_size)
				.map_mut(&file)
				.expect("Path must be writable.")
		};
		let data = unsafe {
			MmapOptions::new()
				.offset(table_header_size as u64)
				.map_mut(&file)
				.expect("Path must be writable.")
		};
		let header = TableHeader::decode(&mut header_data.as_ref())
			.expect("Invalid table header. Database corruption?");
		trace!(target: "table", "Read header: {:?}", header);
		let maps_count = if value_size == 0 { header.touched_count as usize } else { 0 };
		let mut maps = Vec::new();
		maps.resize_with(maps_count,|| RefCell::new(None));
		trace!(target: "table", "Maps is now: {} items: {:?}", maps.len(), maps);

		Self {
			path, data, header_data, header, item_count, item_size, item_header_size, value_size, correction_factor,
			maps, lru_index: RefCell::new(0), mapped: RefCell::new(0), _dummy: Default::default()
		}
	}

	/// Ensures that an item's contents are (immutably) mapped. This will never mutate anything in
	/// such a way that an existing reference becomes invalid. Specifically it is *NOT ALLOWED* to
	/// change a `Some(MmapMut)` into a `None`, only a `None` into a `Some`. This ensures that the
	/// unsafe function used later in `item_ref` is always safe, since it relies on those references
	/// staying valid as long as there's no mutable reference taken to this struct. (A mutable
	/// reference is needed in order to invalidate any of those references.)
	///
	/// Will return `None` if `i` is not an item we currently have stored, `Some(mapped_bytes)` with
	/// the number of bytes that has been additionally mapped (0 if it was already mapped) if it is
	/// stored.
	fn ensure_mapped(&self, i: TableItemIndex, create: Option<u64>) -> Result<RefMut<MmapMut>, ()> {
		trace!(target: "table", "Mapping table index {}", i);
		let item_cell = self.maps.get(i as usize).ok_or(())?;
		let mut item = item_cell.borrow_mut();
		let lru_index = { let mut i = self.lru_index.borrow_mut(); *i += 1; *i };
		if let Some(ref mut inner) = item.deref_mut() {
			trace!(target: "table", "Already mapped");
			inner.1 = lru_index;
		} else {
			trace!(target: "table", "Opening table index contents...");
			let name = self.contents_name(i);
			let file = OpenOptions::new()
				.read(true)
				.write(true)
				.create(create.is_some())
				.open(&name)
				.map_err(|_| ())?;
			if let Some(size) = create {
				file.set_len(size);
			}
			let data = unsafe { MmapOptions::new().map_mut(&file).map_err(|_| ())? };
			*self.mapped.borrow_mut() += data.len();
			trace!(target: "table", "Contents: {}", hex::encode(data.as_ref()));
			*item = Some((data, lru_index));
		}
		Ok(RefMut::map(item, |i| &mut i.as_mut().expect("We just guaranteed this is Some").0))
	}

	fn contents_name(&self, i: TableItemIndex) -> PathBuf {
		let mut path = self.path.clone();
		path.set_extension(format!("{}", i));
		path
	}

	/// Returns `Some(bytes)` with the bytes unmapped, if it was previously mapped. `Some(0)` if it
	/// was not previously mapped, and `None` if we are not storing an item at this index.
	fn ensure_not_mapped(&mut self, i: TableItemIndex) -> Option<usize> {
		let bytes = self.maps.get_mut(i as usize)?.get_mut().take().map_or(0, |i| i.0.len());
		*self.mapped.get_mut() -= bytes;
		Some(bytes)
	}

	fn set_header(&mut self, h: TableHeader) {
		self.header = h;
		self.header.encode_to(&mut SimpleWriter(self.header_data.as_mut(), 0));
	}

	pub fn bytes_used(&self) -> usize {
		self.data.len()
	}

	fn mutate_item_header<R>(&mut self, i: TableItemIndex, f: impl FnOnce(&mut ItemHeader<K>) -> R) -> Result<R, ()> {
		if i as TableItemCount >= self.item_count { return Err(()) }
		let data = &mut self.data[
			self.item_size * i as usize..self.item_size * i as usize + self.item_header_size
		];
		let mut h = ItemHeader::decode(&mut &data[..], self.correction_factor)
			.expect("Database corrupt?");
		let r = f(&mut h);
		h.encode_to(&mut SimpleWriter(data, 0), self.correction_factor);
		Ok(r)
	}

	fn item_header(&self, i: TableItemIndex) -> Result<ItemHeader<K>, ()> {
		if i as TableItemCount >= self.item_count { return Err(()) }
		let data = &self.data[
			self.item_size * i as usize..self.item_size * i as usize + self.item_header_size
		];
		Ok(ItemHeader::decode(&mut &data[..], self.correction_factor)
			.expect("Database corrupt?"))
	}

	#[allow(dead_code)]
	fn set_item_header<R>(&mut self, i: TableItemIndex, h: ItemHeader<K>) -> Result<(), ()> {
		if i as TableItemCount >= self.item_count { return Err(()) }
		let data = &mut self.data[
			self.item_size * i as usize..self.item_size * i as usize + self.item_header_size
		];
		h.encode_to(&mut SimpleWriter(data, 0), self.correction_factor);
		Ok(())
	}

	/// Retrieve a table item's data as an immutable pointer.
	pub fn item_ref_count(&self, i: TableItemIndex, check_hash: Option<&K>) -> Result<RefCount, ()> {
		Ok(self.item_header(i).and_then(|h| h.as_allocation(check_hash))?.0)
	}

	/// Retrieve a table item's key hash.
	#[allow(dead_code)]
	pub fn item_hash(&self, i: TableItemIndex) -> Result<K, ()> {
		self.item_header(i).and_then(|h| h.to_maybe_key().ok_or(()))
	}

	/// Retrieve a table item's data as an immutable pointer.
	pub fn item_ref(&self, i: TableItemIndex, check_hash: Option<&K>) -> Result<&[u8], ()> {
		let header = self.item_header(i).and_then(|h| h.as_allocation(check_hash))?;
		Ok(if self.value_size == 0 {
			self.ensure_mapped(i, None);
			unsafe {
				self.maps.get(i as usize)
					.ok_or(())?
					.try_borrow_unguarded()
					.expect("We never retain a mutable borrow and no functions are reentrant")
					.as_ref()
					.ok_or(())?
					.0.as_ref()
			}
		} else {
			let size = self.value_size - header.1;
			let p = self.item_size * i as usize + self.item_header_size;
			&self.data[p..p + size]
		})
	}

	pub fn set_item(&mut self, i: TableItemIndex, data: &[u8]) -> Result<(), ()> {
		let header = self.item_header(i)?;
		if self.value_size == 0 {
			self.ensure_mapped(i, Some(data.len() as u64))?.copy_from_slice(data);
		} else {
			let size = self.value_size - header.as_allocation(None)?.1;
			let p = self.item_size * i as usize + self.item_header_size;
			self.data[p..p + size].copy_from_slice(data)
		}
		Ok(())
	}

	fn check_key(hash: Option<&K>, key: &K) -> Result<(), ()> {
		if hash.map_or(true, |k| k == key) {
			Ok(())
		} else {
			Err(())
		}
	}

	/// Add another reference to a slot that is already allocated and return the resulting number of
	/// references. Err if the slot is not allocated or if the given `hash` if different to the
	/// hash of the entry.
	pub fn bump(&mut self, i: TableItemIndex, hash: Option<&K>) -> Result<RefCount, ()> {
		self.mutate_item_header(i, |item| {
			match item {
				ItemHeader::Allocated { ref mut ref_count, ref key, .. } => {
					Self::check_key(hash, key)?;
					*ref_count += 1;
					Ok(*ref_count)
				}
				ItemHeader::Free(..) => Err(()),
			}
		}).and_then(|i| i)
	}

	/// Attempt to allocate a slot.
	pub fn allocate(&mut self, key: &K, size: usize) -> Option<TableItemIndex> {
		let mut h = self.header.clone();
		let size_correction = if self.value_size > 0 { (self.value_size - size) as u32 } else { 0 };
		// OPTIMISE: Avoid extra copy of `key` by writing directly to map.
		let new_item = ItemHeader::Allocated { ref_count: 1, size_correction, key: key.clone() };
		let result = if h.used < h.touched_count {
			let result = h.next_free;
			let new_next_free = self.mutate_item_header(result, |item| {
				let new_next_free = item.as_next_free();
				*item = new_item;
				new_next_free
			}).ok()?;
			h.next_free = new_next_free;
			h.used += 1;
			self.set_header(h);
			result
		} else {
			if h.touched_count < self.item_count {
				let result = h.touched_count as TableItemIndex;
				self.mutate_item_header(result, |item| {
					assert!(matches!(item, ItemHeader::Free(_)), "Free slot expected. Database corrupt?");
					*item = new_item;
				}).ok()?;
				h.touched_count += 1;
				h.used += 1;
				self.set_header(h);
				result
			} else {
				return None
			}
		};
		if self.maps.len() <= result as usize {
			let new_len = (result as usize * 3 / 2).max(self.item_count as usize);
			self.maps.resize_with(new_len, ||RefCell::new(None));
		}
		Some(result)
	}

	/// Free up a slot or decrease the reference count if it's greater than 1. Returns Ok along with
	/// the number of refs remaining, or Err if the slot was already free.
	pub fn free(&mut self, i: TableItemIndex, check_hash: Option<&K>) -> Result<RefCount, ()> {
		let mut h = self.header.clone();
		let result = self.mutate_item_header(i, |item| {
			match item {
				ItemHeader::Allocated { ref mut ref_count, ref key, .. } => {
					Self::check_key(check_hash, key)?;
					assert!(*ref_count > 0, "Database corrupt? Zero refs.");
					if *ref_count > 1 {
						*ref_count -= 1;
						return Ok(*ref_count)
					}
				}
				ItemHeader::Free(..) => return Err(()),
			}
			// Stich the old free list head onto this item.
			*item = ItemHeader::Free(h.next_free);
			Ok(0)
		})??;
		if result == 0 {
			if self.value_size == 0 {
				// Actually remove the mapping and the file.
				self.ensure_not_mapped(i);
				std::fs::remove_file(self.contents_name(i));
			}
			// Add the item to the free list.
			h.used = h.used.checked_sub(1)
				.expect("Database corrupt? used count underflow");
			h.next_free = i;
			self.set_header(h);
		}
		Ok(result)
	}

	/// The amount of slots left in this table.
	#[allow(dead_code)]
	pub fn used(&self) -> TableItemCount {
		self.header.used
	}

	/// The amount of slots left in this table.
	#[allow(dead_code)]
	pub fn total(&self) -> TableItemCount {
		self.item_count
	}

	/// The amount of slots left in this table.
	#[allow(dead_code)]
	pub fn available(&self) -> TableItemCount {
		self.item_count - self.header.used
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use std::path::PathBuf;

	#[test]
	fn database_should_work() {
		let _ = std::fs::remove_file("/tmp/test-table");
		let x = {
			let mut t = Table::<[u8; 1]>::open(PathBuf::from("/tmp/test-table"), 0.into());
			let x = t.allocate(&[42u8], 12).unwrap();
			t.set_item(x, b"Hello world!");
			assert_eq!(t.item_ref(x, None).unwrap(), b"Hello world!");
			t.commit();
			x
		};
		let t = Table::<[u8; 1]>::open(PathBuf::from("/tmp/test-table"), 0.into());
		assert_eq!(t.item_ref(x, None).unwrap(), b"Hello world!");
	}

	#[test]
	fn thin_table_should_work() {
		let _ = std::fs::remove_file("/tmp/test-table");
		for i in 0..10 { let _ = std::fs::remove_file(format!("/tmp/test-table.{}", i)); }
		let x = {
			let mut t = Table::<[u8; 1]>::open(PathBuf::from("/tmp/test-table"), DatumSize::Oversize);
			let x = t.allocate(&[42u8], 12).unwrap();
			t.set_item(x, b"Hello world!");
			assert_eq!(t.item_ref(x, None).unwrap(), b"Hello world!");
			t.commit();
			x
		};
		let t = Table::<[u8; 1]>::open(PathBuf::from("/tmp/test-table"), DatumSize::Oversize);
		assert_eq!(t.item_ref(x, Some(&[42u8])).unwrap(), b"Hello world!");
	}
}