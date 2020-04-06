use parking_lot::RwLock;
use blake2_rfc::blake2b::blake2b;
use sp_database::{self, ColumnId};
use parity_scale_codec::Encode;
use crate::database::Database;
use crate::types::KeyType;

/// A database hidden behind an RwLock, so that it implements Send + Sync.
///
/// Construct by creating a `Database` and then using `.into()`.
pub struct SafeDatabase<H: KeyType>(RwLock<Database<H>>);
impl<H: KeyType> From<Database<H>> for SafeDatabase<H> {
	fn from(db: Database<H>) -> Self {
		Self(RwLock::new(db))
	}
}

impl<H: KeyType> sp_database::Database<H> for SafeDatabase<H> {
	fn get(&self, col: ColumnId, key: &[u8]) -> Option<Vec<u8>> {
		let mut hash = H::default();
		(col, key).using_encoded(|d|
			hash.as_mut().copy_from_slice(blake2b(32, &[], d).as_bytes())
		);
		self.0.read().get(&hash)
	}

	fn with_get<R>(&self, col: ColumnId, key: &[u8], f: impl FnOnce(&[u8]) -> R) -> Option<R> {
		let mut hash = H::default();
		(col, key).using_encoded(|d|
			hash.as_mut().copy_from_slice(blake2b(32, &[], d).as_bytes())
		);
		self.0.read().get_ref(&hash).map(|d| f(d.as_ref()))
	}

	fn set(&self, col: ColumnId, key: &[u8], value: &[u8]) {
		let mut hash = H::default();
		(col, key).using_encoded(|d|
			hash.as_mut().copy_from_slice(blake2b(32, &[], d).as_bytes())
		);
		self.0.write().insert(&value, &hash);
	}

	fn remove(&self, col: ColumnId, key: &[u8]) {
		let mut hash = H::default();
		(col, key).using_encoded(|d|
			hash.as_mut().copy_from_slice(blake2b(32, &[], d).as_bytes())
		);
		let _ = self.0.write().remove(&hash);
	}

	fn lookup(&self, hash: &H) -> Option<Vec<u8>> {
		self.0.read().get(hash)
	}

	fn with_lookup<R>(&self, hash: &H, f: impl FnOnce(&[u8]) -> R) -> Option<R> {
		self.0.read().get_ref(hash).map(|d| f(d.as_ref()))
	}

	fn store(&self, hash: &H, preimage: &[u8]) {
		self.0.write().insert(preimage, hash);
	}

	fn release(&self, hash: &H) {
		let _ = self.0.write().remove(hash);
	}
}
