use std::path::PathBuf;

use crate::datum_size::DatumSize;
use crate::types::{KeyType, EntryIndex, TableIndex};
use crate::content_address::ContentAddress;
use crate::table::{Table, TableItemIndex, RefCount, TableItemCount};
use crate::Error;

pub struct Content<K: KeyType> {
	path: PathBuf,
	tables: Vec<Vec<Table<K>>>,
	_dummy: std::marker::PhantomData<K>,
}

impl<K: KeyType> Content<K> {
	/// Creates a new content table of `datum_size`.
	fn new_table(&mut self, datum_size: DatumSize) -> (TableIndex, &mut Table<K>) {
		let s = <u8>::from(datum_size);
		let table_index = self.tables[s as usize].len();
		let table_path = self.table_path(s, table_index);
		self.tables[s as usize].push(Table::open(table_path, datum_size));
		(table_index, &mut self.tables[s as usize][table_index])
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

	pub fn commit(&mut self) {
		for tables in self.tables.iter_mut() {
			for table in tables.iter_mut() {
				table.commit();
			}
		}
	}

	/// Get the raw reference to an item's content value, optionally checking its hash to ensure
	/// it's the right item.
	pub fn item_ref(&self, address: &ContentAddress, check_hash: Option<&K>) -> Result<&[u8], ()> {
		let s = u8::from(address.datum_size) as usize;
		self.tables[s as usize][address.content_table]
			.item_ref(address.entry_index as TableItemIndex, check_hash)
	}

	/// Get the reference count for an item, optionally checking its hash to ensure
	/// it's the right item.
	pub fn item_ref_count(&self, address: &ContentAddress, check_hash: Option<&K>) -> Result<RefCount, ()> {
		let s = u8::from(address.datum_size) as usize;
		self.tables[s as usize][address.content_table]
			.item_ref_count(address.entry_index as TableItemIndex, check_hash)
	}

	/// Get the reference count for an item, optionally checking its hash to ensure
	/// it's the right item.
	#[allow(dead_code)]
	pub fn item_hash(&self, address: &ContentAddress) -> Result<K, ()> {
		let s = u8::from(address.datum_size) as usize;
		self.tables[s as usize][address.content_table]
			.item_hash(address.entry_index as TableItemIndex)
	}

	/// Allocate space to store an item's contents and return its content address.
	///
	/// - `datum_size` is the size class of the item.
	/// - `key` is the hash key of the item.
	/// - `actual_size` is its real size, never more than `datum_size.size()`.
	fn allocate(&mut self, key: &K, actual_size: usize) -> ContentAddress {
		let datum_size = DatumSize::nearest(actual_size);
		let s = u8::from(datum_size) as usize;
		for (content_table, table) in self.tables[s as usize].iter_mut().enumerate() {
			if let Some(entry_index) = table.allocate(key, actual_size) {
				return ContentAddress { datum_size, content_table, entry_index: entry_index as EntryIndex };
			}
		}
		// Out of space - would create a new table
		let (content_table, table) = self.new_table(datum_size);
		let entry_index = table.allocate(key, actual_size).expect("Freshly created");
		ContentAddress { datum_size, content_table, entry_index: entry_index as EntryIndex }
	}

	/// Allocate space to store an item's contents, fill with data and return its content address.
	///
	/// - `datum_size` is the size class of the item.
	/// - `key` is the hash key of the item.
	/// - `data` is its data, whose length is never more than `datum_size.size()`.
	pub fn emplace(&mut self, key: &K, data: &[u8]) -> ContentAddress {
		let address = self.allocate(key, data.len());
		let s = u8::from(address.datum_size) as usize;
		self.tables[s as usize][address.content_table]
			.set_item(address.entry_index as TableItemIndex, data);
		address
	}

	/// Increment the references for an item given its content `address` and optionally checking
	/// that its key hash is the expected `check_hash`.
	pub fn bump(&mut self, address: &ContentAddress, check_hash: Option<&K>) -> Result<RefCount, ()> {
		let s = u8::from(address.datum_size) as usize;
		self.tables[s as usize][address.content_table]
			.bump(address.entry_index as TableItemIndex, check_hash)
	}

	/// Decrement the references for an item given its content `address` and optionally checking
	/// that its key hash is the expected `check_hash`. If they are decremented to zero then the
	/// storage used for the item will be freed.
	pub fn free(&mut self, address: &ContentAddress, check_hash: Option<&K>) -> Result<RefCount, ()> {
		let s = u8::from(address.datum_size) as usize;
		self.tables[s as usize][address.content_table]
			.free(address.entry_index as TableItemIndex, check_hash)
	}

	pub fn open(path: PathBuf) -> Result<Self, Error> {
		let tables = (0u8..64).map(|size| (0usize..)
			.map(|table_index| {
				let mut table_path = path.clone();
				table_path.push(&Self::table_name(size, table_index));
				table_path
			})
			.take_while(|table_path| table_path.is_file())
			.map(|table_path| Table::open(table_path, DatumSize::from(size)))
			.collect()
		).collect();

		Ok(Self { path, tables, _dummy: Default::default() })
	}

	pub fn info(&self) -> Vec<((DatumSize, usize), (TableItemCount, TableItemCount, usize))> {
		self.tables.iter()
			.enumerate()
			.map(|(z, tables)| (DatumSize::from(z as u8), tables))
			.flat_map(|(datum_size, tables)| tables.iter()
				.enumerate()
				.map(|(index, table)| ((datum_size, index), (table.available(), table.used(), table.bytes_used())))
				.collect::<Vec<_>>()
			)
			.collect()
	}
}
