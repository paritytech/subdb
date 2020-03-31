use std::path::PathBuf;
use parity_scale_codec::{Encode, Decode};
use log::{info, trace};

use crate::datum_size::DatumSize;
use crate::types::KeyType;
use crate::content::Content;
use crate::content_address::ContentAddress;
use crate::table::{RefCount, TableItemCount};
use crate::index::Index;
use crate::Error;

/// The options builder.
pub struct Options {
	path: PathBuf,
	key_bytes: usize,
	index_bits: usize,
}

type Version = u32;

const CURRENT_VERSION: Version = 1;

#[derive(Encode, Decode)]
struct MetadataV1 {
	#[codec(encoded_as(u32))]
	key_bytes: u32,
	#[codec(encoded_as(u32))]
	index_bits: u32,
}

impl Metadata for MetadataV1 {}

trait Metadata: Encode + Decode {
	fn filename(path: &PathBuf) -> PathBuf {
		let mut filename = path.clone();
		filename.push("metadata.subdb");
		filename
	}

	fn write(&self, path: &PathBuf) -> Result<(), Error> {
		(b"SBDB", CURRENT_VERSION, &self)
			.using_encoded(|e| std::fs::write(Self::filename(path), e))?;
		Ok(())
	}

	fn try_read(path: &PathBuf) -> Result<Option<Self>, Error> {
		let filename = Self::filename(path);
		if !filename.is_file() {
			return Ok(None);
		}
		let metadata = std::fs::read(Self::filename(path))?;
		let mut input = &metadata[..];

		let magic = <[u8; 4]>::decode(&mut input).map_err(|_| Error::BadMetadata)?;
		if &magic != b"SBDB" {
			return Err(Error::BadMetadata);
		}
		let version = Version::decode(&mut input).map_err(|_| Error::BadMetadata)?;
		if version != CURRENT_VERSION {
			return Err(Error::UnsupportedVersion);
		}
		Ok(Some(Self::decode(&mut input).map_err(|_| Error::BadMetadata)?))
	}
}

impl Options {
	/// Create a new instance.
	pub fn new() -> Self {
		Self {
			key_bytes: 4,
			index_bits: 16,
			path: Default::default(),
		}
	}

	/// Create a new instance, providing a path.
	pub fn from_path(path: PathBuf) -> Self {
		Self {
			key_bytes: 4,
			index_bits: 16,
			path
		}
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
		Database::open(self.path, self.key_bytes, self.index_bits)
	}
}

pub struct Database<K: KeyType> {
	#[allow(dead_code)]
	path: PathBuf,
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
	pub fn open(path: PathBuf, mut key_bytes: usize, mut index_bits: usize) -> Result<Self, Error> {
		assert!(!path.is_file(), "Path must be a directory or not exist.");
		if !path.is_dir() {
			std::fs::create_dir_all(path.clone())?;
		}

		// Sort out metadata.
		if let Some(fields) = MetadataV1::try_read(&path)? {
			key_bytes = fields.key_bytes as usize;
			index_bits = fields.index_bits as usize;
			info!("Opening existing SubDB [{} bytes/{}-bit]", key_bytes, index_bits);
		} else {
			MetadataV1 {
				key_bytes: key_bytes as u32,
				index_bits: index_bits as u32,
			}.write(&path);
			info!("Creating new SubDB [{} bytes/{}-bit]", key_bytes, index_bits);
		}

		let mut index_filename = path.clone();
		index_filename.push("index.subdb");
		let index = Index::open(index_filename, key_bytes, index_bits)?;

		let content = Content::open(path.clone())?;

		Ok(Self {
			path, index, content, _dummy: Default::default()
		})
	}

	pub fn reindex(&mut self, key_bytes: usize, index_bits: usize) -> Result<(), Error> {
		let mut temp_filename = self.path.clone();
		temp_filename.push("new-index.subdb");

		let mut index_filename = self.path.clone();
		index_filename.push("index.subdb");

		// First we create the new index.
		// We don't want to keep it around as we'll be renaming it and need it to be closed.
		Index::from_existing(temp_filename.clone(), &mut self.index, key_bytes, index_bits)?;

		// Then, we cunningly close `self.index` by replacing it with a dummy.
		self.index = Index::anonymous(1, 1)?;

		// Then, we remove the old version and rename the new version.
		std::fs::remove_file(index_filename.clone());
		std::fs::rename(temp_filename, index_filename.clone());
		// ...and reset the metadata.
		MetadataV1 {
			key_bytes: key_bytes as u32,
			index_bits: index_bits as u32,
		}.write(&self.path);
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
		let content = &mut self.content;
		let r = self.index.edit_in(
			&hash,
			|maybe_entry: Option<&ContentAddress>| -> Result<(Option<ContentAddress>, RefCount), ()> {
				if let Some(address) = maybe_entry {
					// Same item (almost certainly) - just need to bump the ref count on the
					// data.
					// We check that this is actually the right item, though.
					content.bump(address, Some(&hash))
						.map(|r| { trace!(target: "index", "Bumped."); (None, r) })
				} else {
					// Nothing there - insert the new item.
					Ok((Some(content.emplace(&hash, data)), 1))
				}
			},
		);
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
