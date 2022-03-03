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
use std::path::Path;

use anyhow::bail;
use anyhow::Context;
use anyhow::Result;

use crate::node::Node;

pub type UIDFactory = HashMap<String, usize>;

// VV: UID, cores, memory
#[derive(Debug)]
pub struct ParetoPoint(pub usize, pub f32, pub f32);

pub struct NodeRegistry {
    pub registry: UIDFactory,
    pub nodes: Vec<Node>,
    pub memory_total: Vec<f32>,
    pub sorted_cores: Vec<usize>,
    pub sorted_memory: Vec<usize>,
    pub connections: HashMap<usize, Vec<usize>>,
    pub connections_reverse: HashMap<usize, Vec<usize>>,
    pub is_dirty: bool,
}

impl NodeRegistry {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self {
            registry: HashMap::new(),
            nodes: vec![],
            memory_total: vec![],
            sorted_cores: vec![],
            sorted_memory: vec![],
            connections: HashMap::new(),
            connections_reverse: HashMap::new(),
            is_dirty: false,
        }
    }

    pub fn load_nodes(&mut self, path: &Path) -> Result<()> {
        let file = File::open(path);

        if let Err(x) = file {
            bail!("Unable to open node_definitions file {} because of {:?}", path.display(), x)
        }

        let br = BufReader::new(file.unwrap());

        for (i, x) in br.lines().enumerate() {
            if let Err(err) = x {
                bail!("Unable to read line {} because of {}", i, err)
            }

            let line = x.unwrap();
            let line = line.trim();

            if !line.is_empty() && !line.starts_with('#') {
                self.new_node_from_str(line)?;
            }
        }

        Ok(())
    }

    pub fn load_connections(&mut self, path: &Path) -> Result<()> {
        let file = File::open(path);

        if let Err(x) = file {
            bail!("Unable to open node_connections file {} because of {:?}", path.display(), x)
        }

        let br = BufReader::new(file.unwrap());

        for (i, x) in br.lines().enumerate() {
            if let Err(err) = x {
                bail!("Unable to read line {} because of {}", i, err)
            }

            let line = x.unwrap();
            let line = line.trim();

            if !line.is_empty() && !line.starts_with('#') {
                self.new_connection_from_str(line)?;
            }
        }

        Ok(())
    }

    pub fn from_paths(path_nodes: &Path, path_connections: &Path) -> Result<Self> {
        let mut reg = Self::new();

        reg.load_nodes(path_nodes)?;
        reg.load_connections(path_connections)?;

        Ok(reg)
    }

    #[allow(dead_code)]
    pub fn nodes_mut(&mut self) -> &mut Vec<Node> { &mut self.nodes }

    #[allow(dead_code)]
    pub fn nodes_immut(&self) -> &Vec<Node> { &self.nodes }

    fn register_node(&mut self, name: &str) -> Result<usize> {
        match self.registry.get(name) {
            Some(uid) => bail!("node {} already exists with UID {}", name, uid),
            None => {
                let uid = self.registry.len();
                self.registry.insert(name.to_owned(), uid);
                Ok(uid)
            }
        }
    }

    pub fn index_bisect_right<FCompareNodes>(
        &self,
        indices: &[usize],
        node_before: FCompareNodes,
        lo: Option<usize>,
        hi: Option<usize>,
    ) -> usize
        where
            FCompareNodes: Fn(&Node) -> bool,
    {
        let lo = lo.unwrap_or(0);
        let hi = hi.unwrap_or(indices.len());
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

    #[allow(dead_code)]
    pub fn nodes_sorted_cores(&self, at_least: f32) -> impl Iterator<Item=&Node> {
        let cores_at_least = |n: &Node| -> bool { n.cores.current < at_least };
        let idx = self.index_bisect_right(&self.sorted_cores, cores_at_least, None, None);

        self.sorted_cores
            .iter()
            .skip(idx)
            .map(|idx| &self.nodes[*idx])
    }

    pub fn resort_nodes_cores(&mut self) {
        self.sorted_cores.sort_by(|idx1: &usize, idx2: &usize| -> std::cmp::Ordering {
            self.nodes[*idx1].cores.current.partial_cmp(
                &self.nodes[*idx2].cores.current)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
    }

    pub fn resort_nodes_memory(&mut self) {
        self.sorted_memory.sort_by(|idx1: &usize, idx2: &usize| -> std::cmp::Ordering {
            self.nodes[*idx1].memory.current.partial_cmp(
                &self.nodes[*idx2].memory.current)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
    }

    #[allow(dead_code)]
    pub fn nodes_sorted_memory(&self, at_least: f32) -> impl Iterator<Item=&Node> {
        let memory_at_least = |n: &Node| -> bool { n.memory.current < at_least };
        let idx = self.index_bisect_right(&self.sorted_memory, memory_at_least, None, None);

        self.sorted_memory
            .iter()
            .skip(idx)
            .map(|idx| &self.nodes[*idx])
    }

    #[allow(dead_code)]
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

    #[allow(dead_code)]
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

    pub fn new_connection_from_str(&mut self, line: &str) -> Result<()> {
        // VV: format is <node_borrower:str>;[<lender1:str>;...<lender_n:str>]
        // lenders might also be "*" (<node_borrowser:str>;*)
        // in which case the borrower can borrow from *any* node
        let tokens: Vec<_> = line.split(';').map(|x| x.trim()).collect();
        let borrower;

        if let Some(&c) = self.registry.get(tokens[0]) {
            borrower = c;
        } else {
            bail!("Unknown borrower name {}", tokens[0])
        }

        let mut lenders: Vec<usize> = vec![];

        let mut process_lender = |i: usize, v: &str| -> Result<()> {
            if v.is_empty() {
                return Ok(());
            }
            if let Some(&c) = self.registry.get(v) {
                if lenders.contains(&c) {
                    bail!("The {}th lender \"{}\" is repeated", i, v)
                }
                lenders.push(c);
            } else {
                bail!("The {}th lender \"{}\" is unknown", i, v)
            }
            Ok(())
        };

        #[allow(clippy::comparison_chain)]
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

    /// Discovers pareto frontier of nodes. The objective is for nodes to have the highest capacity
    /// of resources. We can use the pareto frontier to check whether a Job can be scheduled at all
    /// by checking just a fraction of the total nodes
    pub fn pareto(&self, composable: bool) -> Vec<ParetoPoint> {
        let nodes: Vec<_> = self.nodes.iter()
            .enumerate()
            .filter_map(|(idx, node)| {
                if node.cores.current >= 0. {
                    let memory = if composable {
                        self.avl_memory_to_node_uid(node.uid)
                    } else {
                        node.memory.current
                    };

                    if memory > 0.0 {
                        Some((idx, node.cores.current, memory))
                    } else {
                        None
                    }
                } else {
                    None
                }
            }).collect();

        nodes.iter()
            .filter_map(|&(idx, cores, memory)| {
                if nodes.iter()
                    .all(|&(other_idx, other_cores, other_memory)| {
                        if other_cores < cores || other_memory < memory {
                            other_cores != cores || other_memory != memory || other_idx >= idx
                        } else if other_cores == cores && other_memory == memory {
                            other_idx >= idx
                        } else {
                            false
                        }
                    }) {
                    Some(ParetoPoint(idx, cores, memory))
                } else {
                    None
                }
            }).collect()

        // efficient.iter().enumerate().filter_map(|(uid, is_eff) | {
        //     if *is_eff {
        //         Some(uid)
        //     } else {
        //         None
        //     }
        // }).collect()
    }

    pub fn new_connection(
        &mut self,
        uid_borrower: usize,
        lenders: Vec<usize>,
    ) -> Result<()> {
        if uid_borrower >= self.nodes.len() {
            bail!("Borrower {} is an unknown UID", uid_borrower)
        }

        for uid_lender in &lenders {
            if uid_lender >= &self.nodes.len() {
                bail!("Lender {} is an unknown UID", uid_lender)
            }
            if &uid_borrower == uid_lender {
                bail!("Borrower {} cannot borrow from itself", uid_lender)
            }
        }

        let rev = &mut self.connections_reverse;
        for uid_lender in &lenders {
            let borrowers = &mut rev.get_mut(uid_lender)
                .context("Unknown lender {uid_lender?} in connections_reverse")
                .unwrap();
            borrowers.push(uid_borrower)
        }
        self.connections.insert(uid_borrower, lenders);

        Ok(())
    }

    pub fn new_node_from_str(&mut self, line: &str) -> Result<&Node> {
        // VV: format is <name>;<cores>;<memory>
        let tokens: Vec<_> = line.split(';').map(|s| s.trim()).collect();

        if tokens.len() != 3 {
            bail!("Expected that \"{}\" contained <name:str>;<cores:f32>;<memory:f32>, \
                instead got {:?}", line, tokens)
        }

        let name = tokens[0];
        let cores;
        let memory;

        if let Ok(c) = tokens[1].parse() {
            cores = c;
        } else {
            bail!("Unable to parse {} into cores:f32", tokens[1]);
        }

        if let Ok(m) = tokens[2].parse() {
            memory = m;
        } else {
            bail!("Unable to parse {} into memory:f32", tokens[2])
        }

        self.new_node(name, cores, memory)
    }

    pub fn new_node(
        &mut self,
        name: &str,
        cores: f32,
        memory: f32,
        // memory_lendable: f32,
    ) -> Result<&Node> {
        let uid = self.register_node(name)?;

        let node = Node::new(uid, name, cores, memory /*, memory_lendable*/)?;

        self.insort_memory(&node);
        self.insort_cores(&node);
        self.memory_total.push(node.memory.capacity);
        self.nodes.push(node);
        self.connections.insert(uid, vec![]);
        self.connections_reverse.insert(uid, vec![]);
        Ok(&self.nodes[uid])
    }

    pub fn avl_memory_to_node_uid(&self, uid: usize) -> f32 {
        let can_borrow: f32 = match self.connections.get(&uid) {
            Some(lenders) => lenders
                .iter().map(|idx| self.nodes[*idx].memory.current)
                .sum(),
            None => 0.
        };
        can_borrow + self.nodes[uid].memory.current
    }
}
