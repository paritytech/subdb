use std::path::PathBuf;
use std::hash::Hasher;
use parity_scale_codec::{Encode, Decode};
use criterion::{Criterion, criterion_group, criterion_main, BatchSize};
use subdb::{Options, KeyType};
use twox_hash::XxHash64;

#[derive(Eq, PartialEq, Ord, PartialOrd, Clone, Encode, Decode, Debug)]
struct Xx256([u8; 32]);

impl KeyType for Xx256 {
	const SIZE: usize = 32;
	fn from_data(data: &[u8]) -> Self {
		let mut key = [0u8; 32];
		let mut h = XxHash64::with_seed(0);
		h.write(data);
		let r = h.finish();
		key[..8].copy_from_slice(&r.to_le_bytes());
		key[8..16].copy_from_slice(&r.to_le_bytes());
		key[16..24].copy_from_slice(&r.to_le_bytes());
		key[24..].copy_from_slice(&r.to_le_bytes());
		Self(key)
	}
}
impl AsRef<[u8]> for Xx256 {
	fn as_ref(&self) -> &[u8] { &self.0 }
}
impl AsMut<[u8]> for Xx256 {
	fn as_mut(&mut self) -> &mut [u8] { &mut self.0 }
}

fn criterion_benchmark(c: &mut Criterion) {
	let keys = (0..1_000).map(|i| {
		let data = format!("This is a fairly long string with a unique value at the end of {}", i);
		let key = <[u8; 32]>::from_data(data.as_bytes());
		(data.into_bytes(), key)
	}).collect::<Vec<_>>();

	let new_db = || {
		let path = tempfile::TempDir::new().unwrap();
		let db = Options::new()
			.key_bytes(4)
			.index_bits(24)
			.path(path.as_ref().to_path_buf())
			.open::<[u8; 32]>()
			.unwrap();
		(path, db)
	};
	
	c.bench_function("insert 1000 (mem)", |b| {
		b.iter_batched(|| new_db(), |(_, mut db)| {
			for (ref data, ref k) in keys.iter() {
				db.insert(data.as_ref(), Some(k.clone()));
			}
		}, BatchSize::LargeInput)
	});

	c.bench_function("remove 1000 (mem)", |b| {
		b.iter_batched(|| {
			let (path, mut db) = new_db();
			for (ref data, ref k) in keys.iter() {
				db.insert(data.as_ref(), Some(k.clone()));
			}
			(path, db)
		}, |(_, mut db)| {
			for (_, ref k) in keys.iter() {
				db.remove(k).unwrap();
			}
		}, BatchSize::LargeInput)
	});
}

criterion_group!{
	name = benches;
	config = Criterion::default().sample_size(10);
	targets = criterion_benchmark
}
criterion_main!(benches);