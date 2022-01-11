/*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
*/

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
            capacity,
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
            return;
        }
        self.current += value;

        assert!(self.current <= self.capacity)
    }
}
