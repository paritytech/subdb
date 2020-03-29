pub struct FreeMap<'a>(&'a mut [u64]);
impl<'a> FreeMap<'a> {
	pub fn new(data: &'a mut [u64]) -> Self {
		Self(data)
	}

	pub fn set(&mut self, i: usize) {
		if !self.get(i) {
			self.0[i / 64] |= (1u64 << (i % 64) as u64);
			self.1 += 1;
		}
	}

	pub fn clear(&mut self, i: usize) {
		if self.get(i) {
			self.0[i / 64] &= !(1u64 << (i % 64) as u64);
			self.1 -= 1;
		}
	}

	pub fn get(&self, i: usize) -> bool {
		(self.0[i / 64] & (1u64 << (i % 64) as u64)) != 0
	}

	pub fn next_free(&self, count: usize) -> Option<usize> {
		self.0.iter()
			.enumerate()
			.find(|(_, v)| v != u64::max_value())
			.map(|(i, v)| (!v).leading_zeros() as usize + i * 64)
			.filter(|&x| x < count)
	}

	pub fn total_set(&self) -> usize {
		self.0.iter().map(|&x| x.count_ones() as usize).sum()
	}
}
