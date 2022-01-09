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

use std::collections::HashMap;
use std::fs::File;
use std::io::BufRead;
use std::io::BufReader;
use std::path::{self, Path};

use crate::node::Node;

pub type UIDFactory = HashMap<String, usize>;

pub struct NodeRegistry {
    pub registry: UIDFactory,
    pub nodes: Vec<Node>,
    pub sorted_cores: Vec<usize>,
    pub sorted_memory: Vec<usize>,
    pub connections: HashMap<usize, Vec<usize>>,
}

impl NodeRegistry {
    pub fn new() -> Self {
        return Self {
            registry: HashMap::new(),
            nodes: vec![],
            sorted_cores: vec![],
            sorted_memory: vec![],
            connections: HashMap::new(),
        };
    }

    pub fn load_nodes(&mut self, path: &Path) -> Result<(), String> {
        let file = File::open(path);

        if let Err(x) = file {
            return Err(format!(
                "Unable to open node_definitions file {} because of {:?}",
                path.display(),
                x
            ));
        }

        let br = BufReader::new(file.unwrap());

        for (i, x) in br.lines().enumerate() {
            if let Err(err) = x {
                return Err(format!("Unable to read line {} because of {}", i, err));
            }

            let line = x.unwrap();
            let line = line.trim();

            if line.len() > 0 && line.starts_with("#") == false {
                self.new_node_from_str(line)?;
            }
        }

        Ok(())
    }

    pub fn load_connections(&mut self, path: &Path) -> Result<(), String> {
        let file = File::open(path);

        if let Err(x) = file {
            return Err(format!(
                "Unable to open node_connections file {} because of {:?}",
                path.display(),
                x
            ));
        }

        let br = BufReader::new(file.unwrap());

        for (i, x) in br.lines().enumerate() {
            if let Err(err) = x {
                return Err(format!("Unable to read line {} because of {}", i, err));
            }

            let line = x.unwrap();
            let line = line.trim();

            if line.len() > 0 && line.starts_with("#") == false {
                self.new_connection_from_str(line)?;
            }
        }

        Ok(())
    }

    pub fn from_paths(path_nodes: &Path, path_connections: &Path) -> Result<Self, String> {
        let mut reg = Self::new();

        reg.load_nodes(path_nodes)?;
        reg.load_connections(path_connections)?;

        Ok(reg)
    }

    pub fn nodes_mut(&mut self) -> &mut Vec<Node> {
        return &mut self.nodes;
    }

    pub fn nodes_immut(&self) -> &Vec<Node> {
        return &self.nodes;
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

    pub fn nodes_sorted_cores(&self, at_least: f32) -> impl Iterator<Item=&Node> {
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

    pub fn nodes_sorted_memory(&self, at_least: f32) -> impl Iterator<Item=&Node> {
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

    pub fn idx_nodes_with_more_memory(&self, memory: f32) -> usize {
        let pred = |idx: &usize| -> bool {
            let node = &self.nodes[*idx];
            node.memory.current < memory
        };
        self.sorted_memory.partition_point(pred)
    }

    pub fn idx_nodes_with_more_cores(&self, cores: f32) -> usize {
        let pred = |idx: &usize| -> bool {
            let node = &self.nodes[*idx];
            node.cores.current < cores
        };
        self.sorted_cores.partition_point(pred)
    }

    pub fn idx_sorted_memory(&self, node: &Node) -> usize {
        let memory_target = node.memory.current;
        let uid = node.uid;

        let pred_memory = |node: &Node| -> bool { node.memory.current < memory_target };
        let sorted_memory = &self.sorted_memory;
        let idx_memory = self.index_bisect_right(sorted_memory, pred_memory, None, None);

        let idx_memory = sorted_memory[idx_memory..sorted_memory.len()]
            .iter()
            .position(|idx: &usize| self.nodes[*idx].uid == uid)
            .unwrap()
            + idx_memory;

        idx_memory
    }

    pub fn new_connection_from_str(&mut self, line: &str) -> Result<(), String> {
        // VV: format is <node_borrower:str>;[<lender1:str>;...<lender_n:str>]
        // lenders might also be "*" (<node_borrowser:str>;*)
        // in which case the borrower can borrow from *any* node
        let tokens: Vec<_> = line.split(";").map(|x| x.trim()).collect();
        let borrower;

        if let Some(&c) = self.registry.get(tokens[0]) {
            borrower = c;
        } else {
            return Err(format!("Unknown borrower name {}", tokens[0]));
        }

        let mut lenders: Vec<usize> = vec![];

        let mut process_lender = |i: usize, v: &str| -> Result<(), String> {
            if v.len() == 0 {
                return Ok(());
            }
            if let Some(&c) = self.registry.get(v) {
                if lenders.contains(&c) {
                    return Err(format!("The {}th lender \"{}\" is repeated", i, v));
                }
                lenders.push(c);
            } else {
                return Err(format!("The {}th lender \"{}\" is unknown", i, v));
            }
            Ok(())
        };

        if tokens.len() == 2 {
            if tokens[1] == "*" {
                for v in self.registry.values() {
                    if *v != borrower {
                        lenders.push(*v);
                    }
                }
            } else {
                process_lender(1, tokens[1])?;
            }
        } else if tokens.len() > 2 {
            for (i, v) in tokens.iter().skip(1).enumerate() {
                process_lender(i, v)?;
            }
        }

        self.new_connection(borrower, lenders)
    }

    pub fn new_connection(
        &mut self,
        uid_borrower: usize,
        lenders: Vec<usize>,
    ) -> Result<(), String> {
        if uid_borrower >= self.nodes.len() {
            return Err(format!("Borrower {} is an unknown UID", uid_borrower));
        }

        for uid_lender in &lenders {
            if uid_lender >= &self.nodes.len() {
                return Err(format!("Lender {} is an unknown UID", uid_lender));
            }
            if &uid_borrower == uid_lender {
                return Err(format!("Borrower {} cannot borrow from itself", uid_lender));
            }
        }

        self.connections.insert(uid_borrower, lenders);

        Ok(())
    }

    pub fn new_node_from_str(&mut self, line: &str) -> Result<&Node, String> {
        // VV: format is <name>;<cores>;<memory>
        let tokens: Vec<_> = line.split(";").map(|s| s.trim()).collect();

        if tokens.len() != 3 {
            return Err(format!(
                "Expected that \"{}\" contained <name:str>;<cores:f32>;<memory:f32>, \
                instead got {:?}",
                line, tokens
            ));
        }

        let name = tokens[0];
        let cores;
        let memory;

        if let Ok(c) = tokens[1].parse() {
            cores = c;
        } else {
            return Err(format!("Unable to parse {} into cores:f32", tokens[1]));
        }

        if let Ok(m) = tokens[2].parse() {
            memory = m;
        } else {
            return Err(format!("Unable to parse {} into memory:f32", tokens[2]));
        }

        self.new_node(name, cores, memory)
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

    pub fn avl_memory_to_node_uid(&self, uid: usize, own_memory: f32) -> f32 {
        let can_borrow: f32 = match self.connections.get(&uid) {
            Some(lenders) => lenders
                .iter().map(|idx| self.nodes[*idx].memory.current)
                .sum(),
            None => 0.
        };
        can_borrow + own_memory
    }

    pub fn get_max_cores_memory(&self) -> (f32, f32) {
        let mut uid_max_cores = *self.sorted_cores.last().unwrap();
        let max_cores = self.nodes[uid_max_cores].cores.current;
        let mut max_memory: f32 = 0.0;
        let idx_min_cores = self.idx_nodes_with_more_cores(0.0);

        for uid in &self.sorted_cores[
            idx_min_cores..self.sorted_cores.len()] {
            let uid = *uid;
            let memory = self.nodes[uid].memory.current;
            let other_memory = self.avl_memory_to_node_uid(uid, memory);
            max_memory = max_memory.max(other_memory);
        }

        (max_cores, max_memory)
    }
}
