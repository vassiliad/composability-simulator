use crate::resource;

use std::fmt::{Debug, Formatter};

pub type NodeId = usize;

pub struct Node {
    pub cores: resource::Resource,
    pub memory: resource::Resource,
    pub name: String,
    pub uid: NodeId,
    pub share_from: Vec<NodeId>,
}

impl Debug for Node {
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
        memory_lendable: f32,
    ) -> Result<Self, String> {
        let name = String::from(name);
        let rcores = resource::Resource::new(cores, 0.);

        let rcores = match rcores {
            Ok(rcores) => rcores,
            Err(s) => {
                return Err(format!(
                    "cores definition of {} invalid because {}",
                    name, s
                ))
            }
        };
        let rmemory = resource::Resource::new(memory, memory_lendable);

        let rmemory = match rmemory {
            Ok(rmemory) => rmemory,
            Err(s) => {
                return Err(format!(
                    "memory definition of {} invalid because {}",
                    name, s
                ))
            }
        };

        Ok(Self {
            cores: rcores,
            memory: rmemory,
            name: name,
            share_from: vec![],
            uid: uid,
        })
    }
}
