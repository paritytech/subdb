use std::mem::size_of;

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub enum DatumSize {
	Oversize,
	Size(u8),
}
impl DatumSize {
	/// The size of a datum, or `None` if the datum is oversized.
	pub fn size(&self) -> Option<usize> {
		match *self {
			DatumSize::Oversize => None,
			DatumSize::Size(size) => {
				assert!(size < 127);
				let exp = size as usize / 8;
				let tweak = size as usize % 8;
				let base = 32usize << exp;
				Some(base + base / 8 * tweak)
			}
		}
	}

	/// The nearest datum size for `s`.
	pub fn nearest(s: usize) -> Self {
		if s <= 32 {
			return DatumSize::Size(0)
		}
		if s > 1835008 {
			return DatumSize::Oversize
		}
		let exp = size_of::<usize>() as usize * 8 - s.leading_zeros() as usize - 6;
		let base = 32usize << exp;
		let incr = base / 8;
		let incrs = (s - base + incr - 1) / incr;
		DatumSize::Size((exp * 8 + incrs) as u8)
	}

	/// How many entries should be in a contents table whose items are this size?
	pub fn contents_entries(&self) -> usize {
		// max total size per contents table = 2MB
		// max number of items in contents table = 65536
		if let Some(size) = self.size() {
			(2048 * 1024 / size).max(65536).min(1)
		} else {
			return 1
		}
	}

	/// How big should the data part of the contents file be?
	///
	/// `None` if the contents are oversize - in this case, it's just one item.
	pub fn contents_size(&self) -> Option<usize> {
		self.size().map(|s| s * self.contents_entries())
	}
}

impl From<u8> for DatumSize {
	fn from(x: u8) -> Self {
		if x < 127 {
			DatumSize::Size(x)
		} else {
			DatumSize::Oversize
		}
	}
}

impl From<DatumSize> for u8 {
	fn from(x: DatumSize) -> u8 {
		match x {
			DatumSize::Oversize => 127,
			DatumSize::Size(x) => x,
		}
	}
}

#[test]
fn datum_size_works() {
	assert_eq!(DatumSize::from(0).size().unwrap(), 32);
	assert_eq!(DatumSize::from(1).size().unwrap(), 36);
	assert_eq!(DatumSize::from(2).size().unwrap(), 40);
	assert_eq!(DatumSize::from(7).size().unwrap(), 60);
	assert_eq!(DatumSize::from(8).size().unwrap(), 64);
	assert_eq!(DatumSize::from(9).size().unwrap(), 72);
	assert_eq!(DatumSize::from(15).size().unwrap(), 120);
	assert_eq!(DatumSize::from(16).size().unwrap(), 128);
	assert_eq!(DatumSize::from(17).size().unwrap(), 144);
	assert_eq!(DatumSize::from(24).size().unwrap(), 256);
	assert_eq!(DatumSize::from(32).size().unwrap(), 512);
	assert_eq!(DatumSize::from(40).size().unwrap(), 1_024);
	assert_eq!(DatumSize::from(48).size().unwrap(), 2_048);
	assert_eq!(DatumSize::from(56).size().unwrap(), 4_096);
	assert_eq!(DatumSize::from(64).size().unwrap(), 8_192);
	assert_eq!(DatumSize::from(72).size().unwrap(), 16_384);
	assert_eq!(DatumSize::from(80).size().unwrap(), 32_768);
	assert_eq!(DatumSize::from(88).size().unwrap(), 65_536);
	assert_eq!(DatumSize::from(96).size().unwrap(), 131_072);
	assert_eq!(DatumSize::from(104).size().unwrap(), 262_144);
	assert_eq!(DatumSize::from(126).size().unwrap(), 1_835_008);
	assert_eq!(DatumSize::from(127).size(), None);

	assert_eq!(DatumSize::nearest(0).size().unwrap(), 32);
	assert_eq!(DatumSize::nearest(29).size().unwrap(), 32);
	assert_eq!(DatumSize::nearest(30).size().unwrap(), 32);
	assert_eq!(DatumSize::nearest(31).size().unwrap(), 32);
	assert_eq!(DatumSize::nearest(32).size().unwrap(), 32);
	assert_eq!(DatumSize::nearest(33).size().unwrap(), 36);
	assert_eq!(DatumSize::nearest(34).size().unwrap(), 36);
	assert_eq!(DatumSize::nearest(35).size().unwrap(), 36);
	assert_eq!(DatumSize::nearest(36).size().unwrap(), 36);
	assert_eq!(DatumSize::nearest(37).size().unwrap(), 40);
	assert_eq!(DatumSize::nearest(38).size().unwrap(), 40);
	assert_eq!(DatumSize::nearest(39).size().unwrap(), 40);
	assert_eq!(DatumSize::nearest(40).size().unwrap(), 40);
	assert_eq!(DatumSize::nearest(62).size().unwrap(), 64);
	assert_eq!(DatumSize::nearest(63).size().unwrap(), 64);
	assert_eq!(DatumSize::nearest(64).size().unwrap(), 64);
	assert_eq!(DatumSize::nearest(65).size().unwrap(), 72);
	assert_eq!(DatumSize::nearest(66).size().unwrap(), 72);
	assert_eq!(DatumSize::nearest(67).size().unwrap(), 72);
	assert_eq!(DatumSize::nearest(68).size().unwrap(), 72);
	assert_eq!(DatumSize::nearest(69).size().unwrap(), 72);
	assert_eq!(DatumSize::nearest(70).size().unwrap(), 72);
	assert_eq!(DatumSize::nearest(71).size().unwrap(), 72);
	assert_eq!(DatumSize::nearest(72).size().unwrap(), 72);
	assert_eq!(DatumSize::nearest(73).size().unwrap(), 80);
	assert_eq!(DatumSize::nearest(1_835_007).size().unwrap(), 1_835_008);
	assert_eq!(DatumSize::nearest(1_835_008).size().unwrap(), 1_835_008);
	assert_eq!(DatumSize::nearest(1_835_009).size(), None);
}
