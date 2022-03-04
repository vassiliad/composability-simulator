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
use std::collections::BTreeSet;
use std::collections::VecDeque;

use crate::job::Job;
use crate::job_factory::JobFactory;
use crate::registry::NodeRegistry;

pub struct Scheduler
{
    pub registry: NodeRegistry,
    pub job_factory: Box<dyn JobFactory>,
    pub now: f32,

    pub jobs_queuing: VecDeque<Job>,
    pub jobs_running: VecDeque<Job>,
    pub jobs_done: Vec<usize>,
}

impl Scheduler
{
    pub fn new(registry: NodeRegistry, job_factory: Box<dyn JobFactory>) -> Self {
        Self {
            registry,
            job_factory,
            now: 0.0,
            jobs_done: Vec::new(),
            jobs_queuing: VecDeque::new(),
            jobs_running: VecDeque::new(),
        }
    }

    pub fn has_unschedulable(&self) -> bool {
        self.jobs_running.is_empty()
            && !self.jobs_queuing.is_empty()
            && !self.job_factory.more_jobs()
    }

    fn job_free(&mut self, job: Job) {
        // println!(
        //      "Freeing {}(on {}):{}x{} from cores {} and memory {:?}",
        //      job.uid, job.time_done.unwrap(),
        //      job.cores,
        //      job.memory,
        //      job.node_cores.unwrap(),
        //      job.node_memory
        // );

        let uid_cores = job.node_cores.unwrap();
        self.registry.nodes[uid_cores].free_cores(job.cores);

        for (uid_memory, memory) in &job.node_memory {
            let node = &mut self.registry.nodes[*uid_memory];
            node.free_memory(*memory);
        }
        // VV: It's not safe to use the sorted indices any more
        self.registry.is_dirty = true;
        self.jobs_done.push(job.uid);
        self.job_factory.job_mark_done(&job);
    }

    fn try_allocate_on_many_nodes(
        registry: &NodeRegistry,
        uid_cores: usize,
        job: &Job,
    ) -> Option<(usize, Vec<(usize, f32)>)> {
        let node_cores = &registry.nodes[uid_cores];

        if node_cores.cores.current < job.cores {
            return None;
        }

        let lenders = registry.connections.get(&uid_cores).unwrap();
        let mut rem_mem = job.memory;
        let mut mem_alloc: Vec<(usize, f32)> = Vec::new();

        if node_cores.memory.current > 0.0 {
            let alloc = rem_mem.min(node_cores.memory.current);
            mem_alloc.push((uid_cores, alloc));

            // registry.nodes[uid_cores].allocate_memory(alloc);
            rem_mem -= alloc;
        }

        for uid_mem in lenders {
            if uid_mem != &uid_cores {
                let node_mem = &registry.nodes[*uid_mem];
                let alloc = rem_mem.min(node_mem.memory.current);
                mem_alloc.push((*uid_mem, alloc));

                rem_mem -= alloc;

                if rem_mem == 0.0 {
                    break;
                }
            }
        }

        if rem_mem == 0.0 {
            Some((uid_cores, mem_alloc))
        } else {
            None
        }
    }

    fn job_commit_allocate(
        registry: &mut NodeRegistry,
        job: &mut Job,
        choice: (usize, Vec<(usize, f32)>))
    {
        let (uid_cores, mut all_memory) = choice;

        registry.nodes[uid_cores].allocate_cores(job.cores);

        let node_cores = &registry.nodes[uid_cores];
        job.node_cores = Some(node_cores.uid);

        for (uid_mem, allocated) in &all_memory {
            let node_mem = &mut registry.nodes[*uid_mem];
            node_mem.allocate_memory(*allocated);
        }
        job.node_memory.append(&mut all_memory);
        // println!("Scheduling {}:{}x{} on Cores:{:?}, Memory:{:?}",
        //     job.uid, job.cores, job.memory, job.node_cores, job.node_memory);

        // VV: It's not safe to use the sorted indices any more
        registry.is_dirty = true;
    }

    fn job_allocate_on_nodes_subset(
        registry: &mut NodeRegistry,
        job: &mut Job,
        uid_nodes: &[usize],
    ) -> bool {
        let mut ret = None;
        // println!("Attempting cores: {}, memory: {}, compose: {}",
        //          job.cores, job.memory, job.can_borrow);

        let idx_start = uid_nodes.partition_point(|uid| {
            let node = &registry.nodes[*uid];
            node.cores.current < job.cores
        });

        let uid_nodes = &uid_nodes[idx_start..];

        for &uid_cores in uid_nodes {
            let n = &registry.nodes[uid_cores];
            if job.memory <= n.memory.current && job.cores <= n.cores.current {
                // println!("* vanilla uid: {}, cores: {}, memory: {}",
                //          n.uid, n.cores.current, n.memory.current);
                ret = Some((uid_cores, vec![(uid_cores, job.memory)]));
                break;
            } else {
                // println!("  vanilla uid: {}, cores: {}, memory: {}",
                //          n.uid, n.cores.current, n.memory.current);
            }
        }

        if ret.is_none() && job.can_borrow {
            for &uid_cores in uid_nodes {
                ret = Self::try_allocate_on_many_nodes(registry, uid_cores, job);

                if ret.is_some() {
                    // let n = &registry.nodes[uid_cores];
                    // println!("* composable uid: {}, cores: {}, memory: {}",
                    //      n.uid, n.cores.current, registry.avl_memory_to_node_uid(n.uid));
                    break;
                } else {
                    // let n = &registry.nodes[uid_cores];
                    // println!("  composable uid: {}, cores: {}, memory: {}",
                    //      n.uid, n.cores.current, registry.avl_memory_to_node_uid(n.uid));
                }
            }
        }

        match ret {
            Some(choice) => {
                Scheduler::job_commit_allocate(registry, job, choice);
                true
            }
            None => false
        }
    }

    fn job_try_allocate(
        registry: &NodeRegistry,
        job: &Job,
        idx_memory: usize,
        cores_start: usize,
        cores_end: usize,
    ) -> Option<(usize, Vec<(usize, f32)>)> {
        if idx_memory < registry.sorted_memory.len() {
            // VV: There's a chance one of the nodes with enough cores (all_cores) has enough
            // memory too (all_memory)
            let all_cores = &registry.sorted_cores[cores_start..cores_end];

            for &uid_cores in all_cores {
                let node = &registry.nodes[uid_cores];
                if node.memory.current >= job.memory {
                    return Some((uid_cores, vec![(uid_cores, job.memory)]));
                }
            }
        }

        None
    }

    fn job_allocate(registry: &mut NodeRegistry, job: &mut Job) -> bool {
        if registry.is_dirty {
            registry.resort_nodes_cores();
            registry.resort_nodes_memory();
            // VV: It's now safe to use the sorted indices
            registry.is_dirty = false;
        }

        let cores_start = registry.idx_nodes_with_more_cores(job.cores);

        // println!("Searching for {} cores for {} in -> idx {cores_start}", job.cores, job.uid);
        // for &x in &registry.sorted_cores {
        //     print!("{}, ", registry.nodes[x].cores.current);
        // }
        // println!("");

        if cores_start == registry.sorted_cores.len() {
            return false;
        }

        let idx_memory = registry.idx_nodes_with_more_memory(job.memory);

        // VV: First ty to fit job on any single node
        let mut ret = Self::job_try_allocate(registry, job, idx_memory,
                                             cores_start, registry.sorted_cores.len());

        // VV: If that's not possible, and the job can borrow resources, then try scheduling it
        // using multiple nodes
        if ret.is_none() && job.can_borrow {
            for idx_cores in cores_start..registry.sorted_cores.len() {
                let uid_cores = registry.sorted_cores[idx_cores];
                ret = Self::try_allocate_on_many_nodes(registry, uid_cores, job);
                if ret.is_some() {
                    break;
                }
            }
        }

        match ret {
            Some(choice) => {
                Scheduler::job_commit_allocate(registry, job, choice);
                true
            }
            None => false
        }
    }

    pub fn tick(&mut self) -> bool {
        let mut next_tick: Option<f32> = None;
        let mut run_now: Vec<usize> = vec![];
        // println!("Now is {}", self.now);

        loop {
            let mut new_queueing = 0;
            let new_running;
            let mut new_done = 0;

            // VV: Use a BTreeSet to enforce a deterministic order when iterating the elements
            let mut all_uids = BTreeSet::new();

            /*println!("Jobs running");
            for x in &self.jobs_running {
                println!(",{},{},{}", x.uid, x.time_done.unwrap(), x.node_cores.unwrap());
            }*/

            /*println!("Sorted cores: {:?}", self.registry.sorted_cores);
            for x in &self.registry.sorted_cores {
                println!("{}", self.registry.nodes[*x].cores.current);
            }*/

            while !self.jobs_running.is_empty() {
                let job = &self.jobs_running[0];
                if job.time_done.unwrap() <= self.now {
                    let job = self.jobs_running.pop_front().unwrap();
                    /*println!(
                        "  Job {} that started on {} with duration {} finished",
                        job.uid,
                        job.time_started.unwrap(),
                        job.duration
                   );*/

                    all_uids.insert(job.node_cores.unwrap());
                    for &(uid_mem, _) in &job.node_memory {
                        all_uids.insert(uid_mem);

                        for &x in self.registry.connections_reverse
                            .get(&uid_mem).unwrap() {
                            all_uids.insert(x);
                        }
                    }

                    self.job_free(job);
                    new_done += 1;
                } else {
                    // println!("  NextRunning {}", job.time_done.unwrap());
                    next_tick = match next_tick {
                        Some(x) => Some(x.min(job.time_done.unwrap())),
                        None => job.time_done,
                    };

                    break;
                }
            }

            if !all_uids.is_empty() && !self.jobs_queuing.is_empty() {
                let recompute_uid_nodes = |registry: &NodeRegistry| -> Vec<usize> {
                    let mut uid_nodes: Vec<usize> = vec![];

                    for &uid in &all_uids {
                        let n = &registry.nodes[uid];

                        let pred = |idx: &usize| -> bool {
                            let node = &registry.nodes[*idx];
                            match node.cores.current.partial_cmp(&n.cores.current).unwrap() {
                                std::cmp::Ordering::Equal => {
                                    return node.uid < n.uid
                                },
                                std::cmp::Ordering::Less => true,
                                std::cmp::Ordering::Greater => false,
                            }
                        };

                        let index = uid_nodes.partition_point(pred);
                        uid_nodes.insert(index, n.uid)
                    }
                    uid_nodes
                };
                let mut uid_nodes = recompute_uid_nodes(&self.registry);

                for (i, job) in self.jobs_queuing.iter_mut().enumerate() {
                    if Self::job_allocate_on_nodes_subset(&mut self.registry, job, &uid_nodes) {
                        // println!("Allocated on subset nodes: {:?}", uid_nodes);
                        run_now.push(i);
                        uid_nodes = recompute_uid_nodes(&self.registry);
                    }
                }
            }

            let orig_queueing = self.jobs_queuing.len();

            while let Some(job) = self.job_factory.job_peek() {
                if job.time_created <= self.now {
                    let job = self.job_factory.job_get();
                    // println!("New queueing {}:{:#}", job.uid, job.time_created);
                    self.jobs_queuing.push_back(job);
                    new_queueing += 1;
                } else {
                    // println!("  NextQueueing {}", job.time_created);
                    next_tick = match next_tick {
                        Some(x) => Some(x.min(job.time_created)),
                        None => Some(job.time_created),
                    };
                    break;
                }
            }

            let skip = orig_queueing;
            // if orig_queueing {
            //     skip = 0;
            // } else {
            //     // VV: No jobs finished during this iteration of the current tick, no need to re-process
            //     // the first few orig_queueing jobs, just the ones that this iteration discovered
            //     skip = orig_queueing;
            // }

            // println!("Cores: {}, Memory: {}", max_cores, max_memory);
            // let nodes = self.registry.sorted_composable.clone();
            for (i, job) in self.jobs_queuing.iter_mut().skip(skip).enumerate() {
                if Self::job_allocate(&mut self.registry, job) {
                    run_now.push(i + skip);
                }

                // if Self::job_allocate_on_nodes_subset(&mut self.registry, job, &nodes) {
                //     run_now.push(i + skip);
                // }
            }

            new_running = run_now.len();

            if new_running > 0 {
                let mut q: VecDeque<Job> =
                    VecDeque::with_capacity(self.jobs_queuing.len() - new_running);
                let mut i = 0;
                while !self.jobs_queuing.is_empty() {
                    let mut job = self.jobs_queuing.pop_front().unwrap();
                    let done = self.now + job.duration;
                    if run_now.contains(&i) {
                        job.time_started = Some(self.now);
                        job.time_done = Some(done);

                        let predicate = |j: &Job| -> bool {
                            match j.time_done.unwrap().partial_cmp(&done).unwrap() {
                                std::cmp::Ordering::Equal => j.uid < job.uid,
                                std::cmp::Ordering::Less => true,
                                std::cmp::Ordering::Greater => false,
                            }
                        };
                        let idx = self.jobs_running.partition_point(predicate);
                        self.jobs_running.insert(idx, job);
                    } else {
                        q.push_back(job);
                    }

                    i += 1;
                }

                self.jobs_queuing.append(&mut q);
                run_now.clear();
            }

            if new_queueing + new_running + new_done == 0 {
                break;
            }
        }

        self.now = next_tick.unwrap_or(self.now);

        (self.jobs_queuing.len() + self.jobs_running.len() > 0) || self.job_factory.more_jobs()
    }
}
