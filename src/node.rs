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

use std::fmt::{Display, Formatter};

use crate::resource;

pub type NodeId = usize;

#[derive(Debug)]
pub struct Node {
    pub cores: resource::Resource,
    pub memory: resource::Resource,
    pub name: String,
    pub uid: NodeId,
    pub share_from: Vec<NodeId>,
}

impl Display for Node {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result<(), std::fmt::Error> {
        write!(
            f,
            "{}::{} cores: {}/{}, memory: {}/{}",
            self.uid,
            self.name,
            self.cores.current,
            self.cores.capacity,
            self.memory.current,
            self.memory.capacity,
        )
    }
}

impl Node {
    pub fn new(
        uid: usize,
        name: &str,
        cores: f32,
        memory: f32,
        // memory_lendable: f32,
    ) -> Result<Self, String> {
        let name = String::from(name);
        let cores = resource::Resource::new(cores /*, 0.*/);

        let cores = match cores {
            Ok(cores) => cores,
            Err(s) => {
                return Err(format!(
                    "cores definition of {} invalid because {}",
                    name, s
                ))
            }
        };
        let memory = resource::Resource::new(memory /*, memory_lendable*/);

        let memory = match memory {
            Ok(memory) => memory,
            Err(s) => {
                return Err(format!(
                    "memory definition of {} invalid because {}",
                    name, s
                ))
            }
        };

        Ok(Self {
            cores,
            memory,
            name,
            share_from: vec![],
            uid,
        })
    }

    #[allow(dead_code)]
    pub fn can_host_job(&self, cores: f32, memory: f32) -> bool {
        self.cores.current >= cores && self.memory.current >= memory
    }

    #[allow(dead_code)]
    pub fn allocate_job(&mut self, cores: f32, memory: f32) {
        self.allocate_cores(cores);
        self.allocate_memory(memory);
    }

    pub fn allocate_cores(&mut self, cores: f32) {
        self.cores.allocate(cores)
    }

    pub fn allocate_memory(&mut self, memory: f32) {
        self.memory.allocate(memory)
    }

    pub fn free_memory(&mut self, memory: f32) {
        self.memory.free(memory)
    }

    pub fn free_cores(&mut self, cores: f32) {
        self.cores.free(cores)
    }
}
