use crate::node::Node;
use std::collections::HashMap;
use std::sync::Mutex;
pub type UIDFactory = HashMap<String, usize>;

pub struct NodeRegistry {
    registry: Mutex<UIDFactory>,
    nodes: Vec<Node>,
}

impl NodeRegistry {
    pub fn new() -> Self {
        return Self {
            registry: Mutex::new(HashMap::new()),
            nodes: vec![],
        };
    }

    fn register_node(&mut self, name: &str) -> Result<usize, String> {
        let mut factory = self.registry.lock().unwrap();

        match factory.get(name) {
            Some(uid) => Err(format!("node {} already exists with UID {}", name, uid)),
            None => {
                let uid = factory.len();
                factory.insert(name.to_owned(), uid);
                Ok(uid)
            }
        }
    }

    pub fn new_node(
        &mut self,
        name: &str,
        cores: f32,
        memory: f32,
        memory_lendable: f32,
    ) -> Result<&Node, String> {
        let uid = self.register_node(&name)?;
        let node = Node::new(uid, name, cores, memory, memory_lendable)?;
        self.nodes.push(node);

        Ok(&self.nodes.last().unwrap())
    }
}
