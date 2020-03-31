use std::path::PathBuf;
use log::info;

mod content;
mod content_address;
mod datum_size;
mod database;
mod index;
mod index_item;
mod table;
mod types;

pub use database::{Database, Options};

/// Error type.
// TODO: Repot.

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
// TODO: Adaptive index size (bitwise increase).
// TODO: Content tables should be able to grow.
// TODO: Oversize content tables.
// TODO: Stored friend links.

fn main() {
	simplelog::CombinedLogger::init(
		vec![
			simplelog::TermLogger::new(simplelog::LevelFilter::Info, simplelog::Config::default(), simplelog::TerminalMode::Mixed).unwrap(),
		]
	).unwrap();

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

	{
		let mut db = Options::from_path(path.clone()).open::<Key>().unwrap();

		let value = db.get(&key);
		println!("Value: {:?}", value.and_then(|b| String::from_utf8(b).ok()));
		println!("Refs: {}", db.get_ref_count(&key));

		let value = db.get(&Default::default());
		println!("Empty value: {:?}", value);

		println!("Reindexing...");
		db.reindex(2, 8);

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

		let value = db.get(&key);
		println!("Value: {:?}", value.and_then(|b| String::from_utf8(b).ok()));
	}
}