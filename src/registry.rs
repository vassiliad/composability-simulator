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

use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::BufRead;
use std::io::BufReader;
use std::path::Path;

use anyhow::bail;
use anyhow::Context;
use anyhow::Result;

use crate::node::Node;

// use rayon::prelude::*;

pub type UIDFactory = HashMap<String, usize>;

// VV: UID, cores, memory
#[derive(Debug)]
pub struct ParetoPoint(pub usize, pub f32, pub f32);

pub struct NodeRegistry {
    pub registry: UIDFactory,
    pub nodes: Vec<Node>,
    pub memory_composable: Vec<f32>,
    pub sorted_vanilla: Vec<usize>,
    pub sorted_composable: Vec<usize>,
    pub connections: HashMap<usize, Vec<usize>>,
    pub connections_reverse: HashMap<usize, Vec<usize>>,
}

impl NodeRegistry {
    #[allow(clippy::new_without_default)]
    pub fn new() -> Self {
        Self {
            registry: HashMap::new(),
            nodes: vec![],
            memory_composable: vec![],
            sorted_vanilla: vec![],
            sorted_composable: vec![],
            connections: HashMap::new(),
            connections_reverse: HashMap::new(),
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

    pub fn idx_node_vanilla(&self, cores: f32, memory: f32) -> usize {
        let pred = |uid: &usize| -> bool {
            let node = &self.nodes[*uid];

            if node.cores.current < cores || node.memory.current < memory {
                true
            } else if cores == node.cores.current {
                node.memory.current < memory
            } else {
                false
            }
        };

        self.sorted_vanilla.partition_point(pred)
    }

    pub fn sort_nodes_vanilla(&mut self, except: &HashSet<usize>) {
        let nodes = &*self.nodes;
        let mut indices: Vec<_> = self.sorted_vanilla
            .drain(0..self.sorted_vanilla.len())
            .filter_map(|uid| {
                if !except.contains(&uid) {
                    Some(uid)
                } else {
                    None
                }
            })
            .collect();

        self.sorted_vanilla.append(&mut indices);


        for &uid in except {
            let node = &nodes[uid];
            let index = self.idx_node_vanilla(
                node.cores.current, node.memory.current);
            self.sorted_vanilla.insert(index, uid);
        }
        assert_eq!(self.nodes.len(), self.sorted_vanilla.len());
    }

    pub fn idx_node_composable(&self, cores: f32, memory: f32) -> usize {
        let pred = |uid: &usize| -> bool {
            let node = &self.nodes[*uid];

            if node.cores.current < cores || self.memory_composable[*uid] < memory {
                true
            } else if cores == node.cores.current {
                self.memory_composable[*uid] < memory
            } else {
                false
            }
        };

        self.sorted_composable.partition_point(pred)
    }

    pub fn sort_nodes_composable(&mut self, except: &HashSet<usize>) {
        let nodes = &*self.nodes;
        let mut indices: Vec<_> = self.sorted_composable
            .drain(0..self.sorted_composable.len())
            .filter_map(|uid| {
                if !except.contains(&uid) {
                    Some(uid)
                } else {
                    None
                }
            })
            .collect();
        self.sorted_composable.append(&mut indices);

        for &uid in except {
            self.memory_composable[uid] = self.avl_memory_to_node_uid(uid);
        }

        for &uid in except {
            let node = &nodes[uid];
            let index = self.idx_node_composable(
                node.cores.current, self.memory_composable[uid]);
            self.sorted_composable.insert(index, uid);
        }
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
                if node.cores.current > 0. {
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
        self.memory_composable[uid_borrower] = self.avl_memory_to_node_uid(uid_borrower);

        let except = HashSet::from([uid_borrower]);
        self.sort_nodes_composable(&except);

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

        self.memory_composable.push(node.memory.capacity);
        self.nodes.push(node);
        self.connections.insert(uid, vec![]);
        self.connections_reverse.insert(uid, vec![]);

        let except = HashSet::from([uid]);
        self.sort_nodes_vanilla(&except);
        self.sort_nodes_composable(&except);

        Ok(&self.nodes[uid])
    }

    pub fn avl_memory_to_node_uid(&self, uid: usize) -> f32 {
        let can_borrow: f32 = match self.connections.get(&uid) {
            Some(lenders) => lenders
                .iter().map(|idx| self.nodes[*idx].memory.current)
                .sum(),
            None => 0.
        };
        can_borrow + (&self.nodes[uid]).memory.current
    }
}
