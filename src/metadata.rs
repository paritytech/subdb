use parity_scale_codec::{self as codec, Encode, Decode};
use std::path::PathBuf;
use crate::{Error, database::Options};

type Version = u32;

const CURRENT_VERSION: Version = 1;

pub struct MetadataV1 {
	pub(crate) key_bytes: usize,
	pub(crate) index_bits: usize,
}

impl Metadata for MetadataV1 {}

impl Decode for MetadataV1 {
	fn decode<I: codec::Input>(input: &mut I) -> Result<Self, codec::Error> {
		Ok(Self {
			key_bytes: u32::decode(input)? as usize,
			index_bits: u32::decode(input)? as usize,
		})
	}
}

impl Encode for MetadataV1 {
	fn encode_to<O: codec::Output>(&self, dest: &mut O) {
		(self.key_bytes as u32).encode_to(dest);
		(self.index_bits as u32).encode_to(dest);
	}
}

pub trait Metadata: Encode + Decode {
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

impl<'a> From<&'a Options> for MetadataV1 {
	fn from(o: &'a Options) -> Self {
		Self {
			key_bytes: o.key_bytes,
			index_bits: o.index_bits,
		}
	}
}