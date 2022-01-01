use crate::node::Node;
use std::collections::HashMap;
pub type UIDFactory = HashMap<String, usize>;

pub struct NodeRegistry {
    pub registry: UIDFactory,
    pub nodes: Vec<Node>,
    pub sorted_cores: Vec<usize>,
    pub sorted_memory: Vec<usize>,
}

impl NodeRegistry {
    pub fn new() -> Self {
        return Self {
            registry: HashMap::new(),
            nodes: vec![],
            sorted_cores: vec![],
            sorted_memory: vec![],
        };
    }

    pub fn nodes_mut(&mut self) -> &mut Vec<Node> {
        &mut self.nodes
    }

    pub fn nodes_immut(&self) -> &Vec<Node> {
        &self.nodes
    }

    fn register_node(&mut self, name: &str) -> Result<usize, String> {
        match self.registry.get(name) {
            Some(uid) => Err(format!("node {} already exists with UID {}", name, uid)),
            None => {
                let uid = self.registry.len();
                self.registry.insert(name.to_owned(), uid);
                Ok(uid)
            }
        }
    }

    pub fn index_bisect_right<FCompareNodes>(
        &self,
        indices: &Vec<usize>,
        node_before: FCompareNodes,
        lo: Option<usize>,
        hi: Option<usize>,
    ) -> usize
    where
        FCompareNodes: Fn(&Node) -> bool,
    {
        let lo = lo.or(Some(0)).unwrap();
        let hi = hi.or(Some(indices.len())).unwrap();
        if lo != hi {
            let predicate = |idx: &usize| -> bool {
                let node = &self.nodes[*idx];
                node_before(node)
            };
            indices[lo..hi].partition_point(predicate) + lo
        } else {
            lo
        }
    }

    fn insort_cores(&mut self, node: &Node) {
        let cores_at_least = |n: &Node| -> bool { n.cores.current < node.cores.current };
        let idx = self.index_bisect_right(&self.sorted_cores, cores_at_least, None, None);
        self.sorted_cores.insert(idx, node.uid);
    }

    fn insort_memory(&mut self, node: &Node) {
        let memory_at_least = |n: &Node| -> bool { n.memory.current < node.memory.current };
        let idx = self.index_bisect_right(&self.sorted_memory, memory_at_least, None, None);
        self.sorted_memory.insert(idx, node.uid);
    }

    pub fn nodes_sorted_cores(&self, at_least: f32) -> impl Iterator<Item = &Node> {
        let cores_at_least = |n: &Node| -> bool { n.cores.current < at_least };
        let idx = self.index_bisect_right(&self.sorted_cores, cores_at_least, None, None);

        self.sorted_cores
            .iter()
            .skip(idx)
            .map(|idx| &self.nodes[*idx])
    }

    fn indices_without(indices: &Vec<usize>, except: &Vec<usize>) -> Vec<usize> {
        let mut new: Vec<usize> = Vec::with_capacity(except.len());

        for idx in indices {
            if except.contains(idx) == false {
                new.push(*idx);
            }
        }

        new
    }

    pub fn resort_nodes_cores(&mut self, nodes: &Vec<usize>) {
        // VV: The idea here is to create a new `indices` array that contains
        // the `sorted_cores` elements but those in the @nodes array
        // then perform insertion sort in there
        let mut new_cores: Vec<usize> = NodeRegistry::indices_without(&self.sorted_cores, nodes);
        for idx in nodes {
            let node = &self.nodes[*idx];
            let cores_at_least = |n: &Node| -> bool { n.cores.current < node.cores.current };
            let idx = self.index_bisect_right(&new_cores, cores_at_least, None, None);
            new_cores.insert(idx, node.uid);
        }
        self.sorted_cores.clear();
        self.sorted_cores.append(&mut new_cores);
    }

    pub fn resort_nodes_memory(&mut self, nodes: &Vec<usize>) {
        // VV: The idea here is to create a new `indices` array that contains
        // the `sorted_memory` elements but those in the @nodes array
        // then perform insertion sort in there
        let mut new_memory: Vec<usize> = NodeRegistry::indices_without(&self.sorted_memory, nodes);
        for idx in nodes {
            let node = &self.nodes[*idx];
            let memory_at_least = |n: &Node| -> bool { n.memory.current < node.memory.current };
            let idx = self.index_bisect_right(&new_memory, memory_at_least, None, None);
            new_memory.insert(idx, node.uid);
        }
        self.sorted_memory.clear();
        self.sorted_memory.append(&mut new_memory);
    }

    pub fn nodes_sorted_memory(&self, at_least: f32) -> impl Iterator<Item = &Node> {
        let memory_at_least = |n: &Node| -> bool { n.memory.current < at_least };
        let idx = self.index_bisect_right(&self.sorted_memory, memory_at_least, None, None);

        self.sorted_memory
            .iter()
            .skip(idx)
            .map(|idx| &self.nodes[*idx])
    }

    pub fn idx_sorted_cores(&self, node: &Node) -> usize {
        let cores_target = node.cores.current;
        let uid = node.uid;

        let pred_cores = |node: &Node| -> bool { node.cores.current < cores_target };

        let idx_cores = self.index_bisect_right(&self.sorted_cores, pred_cores, None, None);

        let sorted_cores = &self.sorted_cores;
        let idx_cores = sorted_cores[idx_cores..sorted_cores.len()]
            .iter()
            .position(|idx: &usize| self.nodes[*idx].uid == uid)
            .unwrap()
            + idx_cores;

        idx_cores
    }

    pub fn idx_sorted_memory(&self, node: &Node) -> usize {
        let memory_target = node.memory.current;
        let uid = node.uid;

        let pred_memory = |node: &Node| -> bool { node.memory.current < memory_target };

        let idx_memory = self.index_bisect_right(&self.sorted_memory, pred_memory, None, None);

        let sorted_memory = &self.sorted_memory;
        let idx_memory = sorted_memory[idx_memory..sorted_memory.len()]
            .iter()
            .position(|idx: &usize| self.nodes[*idx].uid == uid)
            .unwrap()
            + idx_memory;

        idx_memory
    }

    pub fn new_node(
        &mut self,
        name: &str,
        cores: f32,
        memory: f32,
        // memory_lendable: f32,
    ) -> Result<&Node, String> {
        let uid = self.register_node(&name)?;

        let node = Node::new(uid, name, cores, memory /*, memory_lendable*/)?;

        self.insort_memory(&node);
        self.insort_cores(&node);
        self.nodes.push(node);

        Ok(&self.nodes[uid])
    }
}
