use std::path::PathBuf;
use log::{info, trace, warn};

use crate::datum_size::DatumSize;
use crate::types::KeyType;
use crate::content::Content;
use crate::content_address::ContentAddress;
use crate::table::{RefCount, TableItemCount};
use crate::index::Index;
use crate::metadata::{Metadata, MetadataV1};
use crate::Error;

/// The options builder.
pub struct Options {
	pub(crate) path: PathBuf,
	pub(crate) key_bytes: usize,
	pub(crate) index_bits: usize,
	pub(crate) skipped_count_trigger: u8,
	pub(crate) key_correction_trigger: usize,
}

impl Options {
	/// Create a new instance.
	pub fn new() -> Self {
		Self {
			key_bytes: 4,
			index_bits: 16,
			skipped_count_trigger: 240,
			key_correction_trigger: 32,
			path: Default::default(),
		}
	}

	/// Create a new instance, providing a path.
	pub fn from_path(path: PathBuf) -> Self {
		Self::new().path(path)
	}

	/// Set the number of bytes to use for the index key (default: 4).
	pub fn key_bytes(mut self, key_bytes: usize) -> Self {
		self.key_bytes = key_bytes;
		self.index_bits = self.index_bits.min(key_bytes * 8);
		self
	}

	/// Set the number of bits to use for the index (default: 24).
	pub fn index_bits(mut self, index_bits: usize) -> Self {
		self.index_bits = index_bits;
		self.key_bytes = self.key_bytes.max(index_bits / 8);
		self
	}

	/// Set the path in which the database should be opened.
	pub fn path(mut self, path: PathBuf) -> Self {
		self.path = path;
		self
	}

	/// Open the database or create one with the configured options if it doesn't yet exist.
	pub fn open<K: KeyType>(self) -> Result<Database<K>, Error> {
		Database::open(self)
	}
}

pub struct Database<K: KeyType> {
	options: Options,
	index: Index<K, ContentAddress>,
	content: Content<K>,
	_dummy: std::marker::PhantomData<K>,
}

impl<K: KeyType> Drop for Database<K> {
	fn drop(&mut self) {
		self.commit();
	}
}

impl<K: KeyType> Database<K> {
	/// Open a database if it already exists and create a new one if not.
	pub fn open(options: Options) -> Result<Self, Error> {
		assert!(!options.path.is_file(), "Path must be a directory or not exist.");
		if !options.path.is_dir() {
			std::fs::create_dir_all(options.path.clone())?;
		}

		// Sort out metadata.
		let metadata = if let Some(metadata) = MetadataV1::try_read(&options.path)? {
			info!("Opening existing SubDB [{} bytes/{}-bit]", metadata.key_bytes, metadata.index_bits);
			metadata
		} else {
			let metadata = MetadataV1::from(&options);
			metadata.write(&options.path)?;
			info!("Creating new SubDB [{} bytes/{}-bit]", metadata.key_bytes, metadata.index_bits);
			metadata
		};

		let mut index_filename = options.path.clone();
		index_filename.push("index.subdb");
		let index = Index::open(
			index_filename,
			metadata.key_bytes,
			metadata.index_bits
		)?;

		let content = Content::open(options.path.clone())?;

		Ok(Self {
			options, index, content, _dummy: Default::default()
		})
	}

	pub fn reindex(&mut self, key_bytes: usize, index_bits: usize) -> Result<(), Error> {
		let mut temp_filename = self.options.path.clone();
		temp_filename.push("new-index.subdb");

		let mut index_filename = self.options.path.clone();
		index_filename.push("index.subdb");

		// First we create the new index.
		// We don't want to keep it around as we'll be renaming it and need it to be closed.
		Index::from_existing(temp_filename.clone(), &mut self.index, key_bytes, index_bits)?;

		// Then, we cunningly close `self.index` by replacing it with a dummy.
		self.index = Index::anonymous(1, 1)?;

		// Then, we remove the old version and rename the new version.
		std::fs::remove_file(index_filename.clone())?;
		std::fs::rename(temp_filename, index_filename.clone())?;
		// ...and reset the metadata.
		MetadataV1 { key_bytes, index_bits }.write(&self.options.path)?;
		info!("Creating new SubDB [{} bytes/{}-bit]", key_bytes, index_bits);


		// Finally, we reopen it replacing the dummy.
		self.index = Index::open(index_filename, key_bytes, index_bits)?;

		Ok(())
	}

	pub fn commit(&mut self) {
		self.index.commit();
		self.content.commit();
	}

	pub fn info(&self) -> Vec<((DatumSize, usize), (TableItemCount, TableItemCount, usize))> {
		self.content.info()
	}

	pub fn get(&self, hash: &K) -> Option<Vec<u8>> {
		self.index.with_item_try(hash, |entry| self.content.item_ref(&entry.address, Some(hash)))
			.map(|d| d.to_vec())
	}

	pub fn get_ref_count(&self, hash: &K) -> RefCount {
		self.index.with_item_try(hash, |entry| self.content.item_ref_count(&entry.address, Some(hash)))
			.unwrap_or(0)
	}

	pub fn insert(&mut self, data: &[u8], hash: Option<K>) -> (RefCount, K) {
		trace!(target: "index", "Inserting data {:?}",
			std::str::from_utf8(data).map_or_else(|_| hex::encode(data), |s| s.to_owned())
		);
		let hash = hash.unwrap_or_else(|| K::from_data(data));
		let r = loop {
			match {
				let content = &mut self.content;
				self.index.edit_in(
					&hash,
					|maybe_entry: Option<&ContentAddress>| -> Result<(Option<ContentAddress>, RefCount), ()> {
						if let Some(address) = maybe_entry {
							// Same item (almost certainly) - just need to bump the ref count on the
							// data.
							// We check that this is actually the right item, though.
							content.bump(address, Some(&hash))
								.map(|r| {
									trace!(target: "index", "Bumped.");
									(None, r)
								})
						} else {
							// Nothing there - insert the new item.
							Ok((Some(content.emplace(&hash, data)), 1))
						}
					},
				)
			} {
				Ok(r) => break r,
				Err(Error::IndexFull) => {
					let (key_bytes, index_bits) = self.index.next_size();
					self.reindex(key_bytes, index_bits).expect("Fatal error");
				}
				Err(_) => unreachable!(),
			}
		};

		let watermarks = self.index.take_watermarks();
		if watermarks.0 > self.options.skipped_count_trigger
			|| watermarks.1 >= self.options.key_correction_trigger
		{
			let (key_bytes, index_bits) = self.index.next_size();
			info!(target: "database", "Watermark triggered. Reindexing to [{} bytes/{} bits]", key_bytes, index_bits);
			if self.reindex(key_bytes, index_bits).is_err() {
				warn!("Error while reindexing. Things will probably go badly wrong now.");
			};
		}

		(r, hash)
	}

	pub fn remove(&mut self, hash: &K) -> Result<RefCount, ()> {
		let content = &mut self.content;
		self.index.edit_out(hash, |address| {
			content.free(&address, Some(hash)).map(|refs_left| {
				if refs_left == 0 {
					// Remove entry (`Some` change to `None` entry)
					(Some(None), 0)
				} else {
					// Ignore (`None` change)
					(None, refs_left)
				}
			})
		})
	}
}
