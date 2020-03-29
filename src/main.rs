use std::path::PathBuf;

mod datum_size;
mod db;
mod index_item;
mod table;
mod types;

use db::SubDb;
use types::KeyType;

// TODO: versioning.

fn main() {
	let mut db = SubDb::<[u8; 32]>::new(PathBuf::from("/tmp/test"));
//	let key = db.put(b"Hello world!");
	let key = <[u8; 32]>::from_data(b"Hello world!");
	dbg!(&key);
	let value = db.get(&key);
	dbg!(&value);
	let value = db.get(&Default::default());
	dbg!(&value);
	db.commit();
}