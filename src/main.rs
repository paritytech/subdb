use std::path::PathBuf;

mod datum_size;
mod db;
mod index_item;
mod table;
mod types;

use db::SubDb;
use types::KeyType;

// TODO: Adaptive index size (bitwise increase).
// DONE: Better format for index n-bytes up to 4 bytes rest-of-key, 16-bit location-correction, 8-
//       bit skipped counter.
//       - for 0-7 bit table (1 - 128 entries), then 4 bytes rest-of-key.
//       - for 8-15 bit table (256 - 32768 entries), then 3 bytes rest-of-key.
//       - for 16-23 bit table (65536 - ~8M entries), then 2 bytes rest-of-key.
//       - for 24-31 bit table (~16M - ~2B entries), then 1 byte rest-of-key.
//       Then 16-bit for location correction (65536 positions, which is subtracted from the actual
//       location index to give the correct key.
//       Then 8-bit for skipped-counter (256 positions, which is the number of items following
//       this position which would have been indexed here but had to skip over this position because
//       it was already taken.
// TODO: Oversize content tables.
// TODO: Content tables should be able to grow.
// TODO: Versioning.

fn main() {
	let path = PathBuf::from("/tmp/test");
	std::fs::remove_dir_all(&path);

	let key = {
		let mut db = SubDb::<[u8; 32]>::open(path.clone(), 4, 24);
		let key = db.put(b"Hello world!");
		let value = db.get(&key);
		dbg!(value.and_then(|b| String::from_utf8(b).ok()));
		key
	};

	{
		let mut db = SubDb::<[u8; 32]>::open(path.clone(), 4, 24);

		let value = db.get(&key);
		dbg!(value.and_then(|b| String::from_utf8(b).ok()));
		dbg!(db.get_ref_count(&key));

		let value = db.get(&Default::default());
		dbg!(&value);

		println!("Info: {:?}", db.info());

		let value = db.get(&key);
		db.remove(&key);
	}

	{
		let mut db = SubDb::<[u8; 32]>::open(path, 4, 24);

		println!("Info: {:?}", db.info());

		let value = db.get(&key);
		dbg!(value.and_then(|b| String::from_utf8(b).ok()));
		dbg!(db.get_ref_count(&key));
	}
}