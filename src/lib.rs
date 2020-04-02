mod content;
mod content_address;
mod datum_size;
mod database;
mod error;
mod index;
mod index_item;
mod metadata;
mod table;
mod types;

pub use database::Options;
pub use content_address::ContentAddress;
pub use error::Error;
pub use types::KeyType;

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
// DONE: Versioning.
// DONE: Remove items.
// DONE: Adaptive index size (bitwise increase).
// DONE: Oversize content tables.
// TODO: Content tables should be able to grow.
// TODO: Stored friend links.
// TODO: Remove panickers.

#[cfg(test)]
mod tests {
	use super::*;
	use log::info;
	use std::path::PathBuf;

	fn init() {
		simplelog::CombinedLogger::init(
			vec![
				simplelog::TermLogger::new(simplelog::LevelFilter::Info, simplelog::Config::default(), simplelog::TerminalMode::Mixed).unwrap(),
			]
		).unwrap();
	}

	#[test]
	fn oversize_allocation_works() {
		init();
		let path = PathBuf::from("/tmp/test");
		std::fs::remove_dir_all(&path).unwrap();

		type Key = [u8; 8];
		let key = {
			let mut db = Options::new()
				.key_bytes(2)
				.index_bits(4)
				.path(path.clone())
				.open::<Key>()
				.unwrap();
			// Insert 1MB of zeros
			db.insert(&[0u8; 1024*1024][..], None).1
		};

		{
			let mut db = Options::from_path(path.clone()).open::<Key>().unwrap();
			// Check it's there.
			assert_eq!(db.get_ref(&key).unwrap(), &[0u8; 1024 * 1024][..]);
			// Delete it.
			db.remove(&key);
		}

		{
			let db = Options::from_path(path.clone()).open::<Key>().unwrap();
			// Check it's not there.
			assert_eq!(db.get_ref(&key), None);
		}
	}

	#[test]
	fn general_use_should_work() {
		init();
		let path = PathBuf::from("/tmp/test");
		std::fs::remove_dir_all(&path).unwrap();

		type Key = [u8; 8];
		let key = {
			let mut db = Options::new()
				.key_bytes(2)
				.index_bits(4)
				.path(path.clone())
				.open::<Key>()
				.unwrap();
			db.insert(b"Hello world!", None).1
		};

		let mut number3 = Key::default();
		{
			let mut db = Options::from_path(path.clone()).open::<Key>().unwrap();
			for i in 0..100 {
				let value = format!("The number {}", i);
				println!("👉 Inserting: {}", value);
				let key = db.insert(value.as_bytes(), None).1;
				if i == 3 {
					number3 = key;
				}
			}
		}

		{
			let mut db = Options::from_path(path.clone()).open::<Key>().unwrap();

			let value = db.get(&key);
			println!("Value: {:?}", value.and_then(|b| String::from_utf8(b).ok()));
			println!("Refs: {}", db.get_ref_count(&key));

			let value = db.get(&Default::default());
			println!("Empty value: {:?}", value);

			println!("Reindexing...");
			db.reindex(2, 8).unwrap();

			let value = db.get(&key);
			println!("Value: {:?}", value.and_then(|b| String::from_utf8(b).ok()));
			println!("Refs: {}", db.get_ref_count(&key));

			info!("Info: {:?}", db.info());

			let _value = db.get(&key);
			db.remove(&key).unwrap();
		}

		{
			let db = Options::from_path(path.clone()).open::<Key>().unwrap();

			info!("Info: {:?}", db.info());

			let value = db.get(&number3).and_then(|s| String::from_utf8(s).ok());
			println!("Number3 (key: {}) is {:?}", hex::encode(number3), value);

			let value = db.get(&key);
			println!("Value: {:?}", value.and_then(|b| String::from_utf8(b).ok()));
		}
	}
}