mod content;
mod content_address;
mod datum_size;
mod database;
mod error;
mod index;
mod index_item;
mod metadata;
mod safe_database;
mod table;
mod types;

pub use database::Options;
pub use safe_database::SafeDatabase;
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
// DONE: Content tables should be able to grow.
// TODO: Stored friend links.
// TODO: Remove panickers.
// TODO: Comprehensive tests.

#[cfg(test)]
mod tests {
	use super::*;
	use log::info;
	use std::path::PathBuf;
	use crate::types::Blake2Output;

	fn init() {
		let _ = simplelog::CombinedLogger::init(
			vec![
				simplelog::TermLogger::new(simplelog::LevelFilter::Info, simplelog::Config::default(), simplelog::TerminalMode::Mixed).unwrap(),
			]
		);
	}

	#[test]
	fn contains_key_works() {
		init();
		let path = PathBuf::from("/tmp/test-contains_key_works");
		let _ = std::fs::remove_dir_all(&path);

		type Key = Blake2Output<[u8; 8]>;
		let key = {
			let mut db = Options::new()
				.key_bytes(2)
				.index_bits(4)
				.path(path.clone())
				.open::<Key>()
				.unwrap();
			db.store(b"Hello world!").1
		};

		{
			let mut db = Options::from_path(path.clone()).open::<Key>().unwrap();
			// Check it's there.
			assert!(db.contains_key(&key));
			db.remove(&key).unwrap();
			assert!(!db.contains_key(&key));
		}
	}

	#[test]
	fn oversize_allocation_works() {
		init();
		let path = PathBuf::from("/tmp/test-oversize_allocation_works");
		let _ = std::fs::remove_dir_all(&path);

		type Key = Blake2Output<[u8; 8]>;
		let key = {
			let mut db = Options::new()
				.key_bytes(2)
				.index_bits(4)
				.path(path.clone())
				.open::<Key>()
				.unwrap();
			// Insert 1MB of zeros
			db.store(&[0u8; 1024*1024][..]).1
		};

		{
			let mut db = Options::from_path(path.clone()).open::<Key>().unwrap();
			// Check it's there.
			assert_eq!(db.get_ref(&key).unwrap().as_ref(), &[0u8; 1024 * 1024][..]);
			// Delete it.
			db.remove(&key).unwrap();
		}

		{
			let db = Options::from_path(path.clone()).open::<Key>().unwrap();
			// Check it's not there.
			assert!(!db.contains_key(&key));
		}
	}

	#[test]
	fn oversize_allocation_shrink_works() {
		init();
		let path = PathBuf::from("/tmp/test-oversize_allocation_shrink_works");
		let _ = std::fs::remove_dir_all(&path);

		type Key = Blake2Output<[u8; 8]>;
		let mut db = Options::new()
			.key_bytes(2)
			.index_bits(4)
			.oversize_shrink(8 * 1024 * 1024, 2 * 1024 * 1024)
			.all_items_backed()
			.path(path.clone())
			.open::<Key>()
			.unwrap();
		let keys = (0..8).map(|i|
			// Insert 1MB of zeros
			db.store(&[i; 1024 * 1024][..]).1
		).collect::<Vec<_>>();
		assert_eq!(db.bytes_mapped(), 8 * 1024 * 1024 + 655360);

		// Trigger shrinking.
		let key8 = db.store(&[8u8; 1024 * 1024][..]).1;
		assert_eq!(db.bytes_mapped(), 2 * 1024 * 1024 + 655360);

		// Should only be 6 & 7 left now.
		assert_eq!(db.get(&keys[7]).unwrap(), &[7u8; 1024 * 1024][..]);
		assert_eq!(db.get(&key8).unwrap(), &[8u8; 1024 * 1024][..]);
		assert_eq!(db.bytes_mapped(), 2 * 1024 * 1024 + 655360);

		// Mapping key 0 will have to go to disk.
		assert_eq!(db.get(&keys[0]).unwrap(), &[0u8; 1024 * 1024][..]);
		assert_eq!(db.bytes_mapped(), 3 * 1024 * 1024 + 655360);
	}

	#[test]
	fn general_use_should_work() {
		init();
		let path = PathBuf::from("/tmp/test-general_use_should_work");
		let _ = std::fs::remove_dir_all(&path);

		type Key = Blake2Output<[u8; 8]>;
		let key = {
			let mut db = Options::new()
				.key_bytes(2)
				.index_bits(4)
				.path(path.clone())
				.open::<Key>()
				.unwrap();
			db.store(b"Hello world!").1
		};

		let mut number3 = Key::default();
		{
			let mut db = Options::from_path(path.clone()).open::<Key>().unwrap();
			for i in 0..100 {
				let value = format!("The number {}", i);
				println!("👉 Inserting: {}", value);
				let key = db.store(value.as_bytes()).1;
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