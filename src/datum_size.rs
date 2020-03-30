use std::mem::size_of;

const MAX_SIZE: u8 = 63;

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
			DatumSize::Size(size_class) => {
				assert!(size_class < MAX_SIZE);
				if size_class < 32 {
					let exp = size_class as usize / 8;
					let tweak = size_class as usize % 8;
					let base = 32usize << exp;
					Some(base + base / 8 * tweak)
				} else {
					let exp = size_class as usize / 4 - 4;
					let tweak = size_class as usize % 4;
					let base = 32usize << exp;
					Some(base + base / 4 * tweak)
				}
			}
		}
	}

	/// The nearest datum size for `s`.
	pub fn nearest(s: usize) -> Self {
		if s <= 32 {
			return DatumSize::Size(0)
		}
		let exp = size_of::<usize>() as usize * 8 - s.leading_zeros() as usize - 6;
		let base = 32usize << exp;
		let rem = s - base;

		let result = if exp < 4 {
			// incr of 1/8
			let incr = base / 8;
			let incrs = (rem + incr - 1) / incr;
			exp * 8 + incrs
		} else {
			// incr of 1/4
			let incr = base / 4;
			let incrs = (rem + incr - 1) / incr;
			32 + ((exp - 4) * 4) + incrs
		};

		if result < MAX_SIZE as usize {
			DatumSize::Size(result as u8)
		} else {
			DatumSize::Oversize
		}
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

	/// Total number of different sizes that are served by this. Only sensible for Sized.
	pub fn size_range(&self) -> usize {
		match *self {
			DatumSize::Oversize => usize::max_value(),
			DatumSize::Size(size_class) => {
				assert!(size_class < MAX_SIZE);
				if size_class == 0 {
					33
				} else {
					if size_class <= 32 {
						let exp = size_class as usize / 8;
						let tweak = size_class as usize % 8;
						let base = 32usize << exp;
						if tweak == 0 {
							base / 8 / 2
						} else {
							base / 8
						}
					} else {
						let exp = size_class as usize / 4 - 4;
						let tweak = size_class as usize % 4;
						let base = 32usize << exp;
						if tweak == 0 {
							base / 4 / 2
						} else {
							base / 4
						}
					}
				}
			}
		}
	}
}

impl From<u8> for DatumSize {
	fn from(x: u8) -> Self {
		if x < MAX_SIZE {
			DatumSize::Size(x)
		} else {
			DatumSize::Oversize
		}
	}
}

impl From<DatumSize> for u8 {
	fn from(x: DatumSize) -> u8 {
		match x {
			DatumSize::Oversize => MAX_SIZE,
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
	assert_eq!(DatumSize::from(31).size().unwrap(), 480);
	assert_eq!(DatumSize::from(32).size().unwrap(), 512);
	assert_eq!(DatumSize::from(33).size().unwrap(), 640);
	assert_eq!(DatumSize::from(34).size().unwrap(), 768);
	assert_eq!(DatumSize::from(35).size().unwrap(), 896);
	assert_eq!(DatumSize::from(36).size().unwrap(), 1_024);
	assert_eq!(DatumSize::from(37).size().unwrap(), 1_280);
	assert_eq!(DatumSize::from(38).size().unwrap(), 1_536);
	assert_eq!(DatumSize::from(39).size().unwrap(), 1_792);
	assert_eq!(DatumSize::from(40).size().unwrap(), 2_048);
	assert_eq!(DatumSize::from(44).size().unwrap(), 4_096);
	assert_eq!(DatumSize::from(48).size().unwrap(), 8_192);
	assert_eq!(DatumSize::from(52).size().unwrap(), 16_384);
	assert_eq!(DatumSize::from(56).size().unwrap(), 32_768);
	assert_eq!(DatumSize::from(60).size().unwrap(), 65_536);
	assert_eq!(DatumSize::from(62).size().unwrap(), 98_304);
	assert_eq!(DatumSize::from(63).size(), None);

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

	assert_eq!(DatumSize::nearest(480).size().unwrap(), 480);
	assert_eq!(DatumSize::nearest(481).size().unwrap(), 512);
	assert_eq!(DatumSize::nearest(512).size().unwrap(), 512);
	assert_eq!(DatumSize::nearest(513).size().unwrap(), 640);
	assert_eq!(DatumSize::nearest(640).size().unwrap(), 640);
	assert_eq!(DatumSize::nearest(641).size().unwrap(), 768);

	assert_eq!(DatumSize::nearest(98_303).size().unwrap(), 98_304);
	assert_eq!(DatumSize::nearest(98_304).size().unwrap(), 98_304);
	assert_eq!(DatumSize::nearest(98_305).size(), None);
}
