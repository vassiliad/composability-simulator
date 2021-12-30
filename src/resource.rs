pub struct Resource {
    pub capacity: f32,
    pub current: f32,
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
        })
    }
}
