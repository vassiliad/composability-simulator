pub struct Resource {
    pub capacity: f32,
    pub current: f32,
    num_allocate: u32,
    // pub lendable: f32,
}

impl Resource {
    pub fn new(capacity: f32 /*, lendable: f32*/) -> Result<Self, String> {
        if capacity < 0. {
            return Err(format!("capacity {} cannot be negative", capacity));
        }
        // if lendable > capacity {
        //     return Err(format!(
        //         "lendable {} cannot be greater than capacity {}",
        //         lendable, capacity
        //     ));
        // }
        Ok(Self {
            capacity: capacity,
            // lendable: lendable,
            current: capacity,
            num_allocate: 0,
        })
    }
    pub fn allocate(&mut self, value: f32) {
        self.current -= value;
        self.num_allocate += 1;

        assert!(self.current >= 0.)
    }

    pub fn free(&mut self, value: f32) {
        self.num_allocate -= 1;
        if self.num_allocate == 0 {
            self.current = self.capacity;
            return
        }
        self.current += value;

        assert!(self.current <= self.capacity)
    }
    
}
