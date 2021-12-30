use crate::resource;

use std::fmt::{Display, Formatter};

pub type NodeId = usize;

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

    pub fn can_host_job(&self, cores: f32, memory: f32) -> bool {
        self.cores.current >= cores && self.memory.current >= memory
    }

    pub fn allocate_job(&mut self, cores: f32, memory: f32) {
        self.allocate_cores(cores);
        self.allocate_memory(memory);
    }

    pub fn allocate_cores(&mut self, cores: f32) {
        self.cores.current -= cores;
        assert!(self.cores.current >= 0.);
    }

    pub fn allocate_memory(&mut self, memory: f32) {
        self.memory.current -= memory;
        assert!(self.memory.current >= 0.);
    }
}
