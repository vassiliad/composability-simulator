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
use std::collections::HashSet;
use std::collections::VecDeque;
use std::time::SystemTime;

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
    pub jobs_done: HashSet<usize>,
}

impl Scheduler
{
    pub fn new(registry: NodeRegistry, job_factory: Box<dyn JobFactory>) -> Self {
        Self {
            registry,
            job_factory,
            now: 0.0,
            jobs_done: HashSet::new(),
            jobs_queuing: VecDeque::new(),
            jobs_running: VecDeque::new(),
        }
    }

    fn job_free(&mut self, job: Job) {
        // println!(
        //     "Freeing {}x{} from cores {} and memory {:?}",
        //     job.cores,
        //     job.memory,
        //     job.node_cores.unwrap(),
        //     job.node_memory
        // );

        self.jobs_done.insert(job.uid);
        self.job_factory.job_mark_done(&job);

        let uid_cores = job.node_cores.unwrap();
        self.registry.nodes[uid_cores].free_cores(job.cores);

        let mut recomp_mem: HashSet<usize> = HashSet::new();

        for (uid_mem, memory) in &job.node_memory {
            let node = &mut self.registry.nodes[*uid_mem];
            node.free_memory(*memory);

            recomp_mem.insert(*uid_mem);
            for x in self.registry.connections_reverse.get(uid_mem).unwrap() {
                recomp_mem.insert(*x);
            }
        }

        for uid_mem in recomp_mem {
            self.registry.memory_total[uid_mem] = self.registry.avl_memory_to_node_uid(uid_mem);
        }

        // VV: It's not safe to use the sorted indices any more
        self.registry.is_dirty = true;
    }

    fn try_allocate_on_many_cores(
        registry: &NodeRegistry,
        idx_cores: usize,
        idx_memory: usize,
        job: &Job,
    ) -> Option<(usize, Vec<(usize, f32)>)> {
        let uid_cores = registry.sorted_cores[idx_cores];

        if *registry.memory_total.get(uid_cores).unwrap() < job.memory {
            return None;
        }

        let lenders = registry.connections.get(&uid_cores).unwrap();
        let all_memory: Vec<usize> = registry.sorted_memory
            [idx_memory..registry.sorted_memory.len()]
            .iter()
            .filter_map(|x| if lenders.contains(x) { Some(*x) } else { None })
            .collect();

        let mut rem_mem = job.memory;
        let mut mem_alloc: Vec<(usize, f32)> = Vec::new();
        let node_cores = &registry.nodes[uid_cores];

        if node_cores.memory.current > 0.0 {
            let alloc = rem_mem.min(node_cores.memory.current);
            if alloc > 0. {
                mem_alloc.push((uid_cores, alloc));
            }
            // registry.nodes[uid_cores].allocate_memory(alloc);
            rem_mem -= alloc;
        }

        for uid_mem in &all_memory {
            if uid_mem != &uid_cores {
                let node_mem = &registry.nodes[*uid_mem];
                let alloc = rem_mem.min(node_mem.memory.current);

                if alloc > 0. {
                    mem_alloc.push((*uid_mem, alloc));
                }

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

    fn job_try_allocate(
        registry: &NodeRegistry,
        job: &Job,
        cores_start: usize,
        cores_end: usize,
    ) -> Option<(usize, Vec<(usize, f32)>)> {
        let memory = registry.idx_nodes_with_more_memory(job.memory);

        if memory < registry.sorted_memory.len() {
            let all_memory = &registry.sorted_memory[memory..registry.sorted_memory.len()];

            let all_cores = &registry.sorted_cores[cores_start..cores_end];
            // VV: There's a chance one of the nodes with enough cores (all_cores) has enough
            // memory too (all_memory)
            for &uid_cores in all_cores {
                if all_memory.iter().any(|&uid| uid == uid_cores) {
                    // Self::job_allocate_on_single_node(registry, idx_cores, job);
                    return Some((uid_cores, vec![(uid_cores, job.memory)]));
                }
            }
        }

        if !job.can_borrow {
            return None;
        }

        let idx_memory = registry.idx_nodes_with_more_memory(0.0);

        for idx_cores in cores_start..cores_end {
            let try_alloc = Self::try_allocate_on_many_cores(registry, idx_cores, idx_memory, job);
            if try_alloc.is_some() {
                return try_alloc;
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

        let cores = registry.idx_nodes_with_more_cores(job.cores);
        if cores == registry.sorted_cores.len() {
            return false;
        }

        let mut ret = None;

        for idx_cores in cores..registry.sorted_cores.len() {
            ret = Self::job_try_allocate(registry, job,
                                         idx_cores, idx_cores + 1);
            if ret.is_some() {
                break;
            }
        }

        match ret {
            Some((uid_cores, mut all_memory)) => {
                registry.nodes[uid_cores].allocate_cores(job.cores);

                let node_cores = &registry.nodes[uid_cores];
                job.node_cores = Some(node_cores.uid);

                let mut recomp_mem: HashSet<usize> = HashSet::new();

                for (uid_mem, allocated) in &all_memory {
                    let node_mem = &mut registry.nodes[*uid_mem];
                    node_mem.allocate_memory(*allocated);
                    recomp_mem.insert(*uid_mem);
                    for x in registry.connections_reverse.get(uid_mem).unwrap() {
                        recomp_mem.insert(*x);
                    }
                }
                job.node_memory.append(&mut all_memory);
                // println!("Scheduling {}x{} on Cores:{:?}, Memory:{:?}",
                // job.cores, job.memory, job.node_cores, job.node_memory);

                // VV: It's not safe to use the sorted indices any more
                registry.is_dirty = true;

                for uid_mem in recomp_mem {
                    registry.memory_total[uid_mem] = registry.avl_memory_to_node_uid(uid_mem)
                }

                true
            }
            None => false
        }
    }

    pub fn tick(&mut self) -> bool {
        let mut next_tick: Option<f32> = None;
        let mut run_now: Vec<usize> = vec![];
        // println!("Now is {}", self.now);

        let max_secs = 5.0;
        let tick_started = SystemTime::now();

        loop {
            let mut new_queueing = 0;
            let new_running;
            let mut new_done = 0;

            while !self.jobs_running.is_empty() {
                let job = &self.jobs_running[0];
                if job.time_done.unwrap() <= self.now {
                    let job = self.jobs_running.pop_front().unwrap();
                    // println!(
                    //     "  Job {} that started on {} with duration {} finished",
                    //     job.uid,
                    //     job.time_started.unwrap(),
                    //     job.duration
                    // );

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

            let orig_queueing = self.jobs_queuing.len();
            // VV: Only add up to 100 jobs per loop so that simulator
            // has more chances to print out periodic reports
            let max_new_queuing = 100;
            loop {
                match self.job_factory.job_peek() {
                    Some(job) => {
                        if job.time_created <= self.now {
                            let job = self.job_factory.job_get();
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
                    None => break,
                }

                if new_queueing == max_new_queuing {
                    break;
                }
            }

            let skip;
            if new_done > 0 {
                skip = 0;
            } else {
                // VV: No jobs finished during this iteration of the current tick, no need to re-process
                // the first few orig_queueing jobs, just the ones that this iteration discovered
                skip = orig_queueing;
            }

            let (t_max_cores, t_max_mem) = self.registry.get_max_cores_memory();
            let mut max_cores = t_max_cores;
            let mut max_memory = t_max_mem;

            // println!("Cores: {}, Memory: {}", max_cores, max_memory);

            for (i, job) in self.jobs_queuing.iter_mut().skip(skip).enumerate() {
                if job.cores > max_cores || job.memory > max_memory {
                    continue;
                }

                if Self::job_allocate(&mut self.registry, job) {
                    run_now.push(i + skip);
                    let (t_max_cores, t_max_mem) = self.registry.get_max_cores_memory();
                    max_cores = t_max_cores;
                    max_memory = t_max_mem;
                }
            }

            new_running = run_now.len();

            if new_running > 0 {
                let mut q: VecDeque<Job> =
                    VecDeque::with_capacity(self.jobs_queuing.len() - new_running);
                let mut i = 0;
                while !self.jobs_queuing.is_empty() {
                    let mut job = self.jobs_queuing.pop_front().unwrap();
                    let done = self.now + job.duration;
                    job.time_started = Some(self.now);
                    job.time_done = Some(done);
                    if run_now.contains(&i) {
                        let predicate = |job: &Job| -> bool { job.time_done.unwrap() < done };
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

            // VV: if this tick is taking longer than 5 secs to process then bail out
            // whoever is running the simulator might wish to print something to the terminal
            let going_for = SystemTime::now().duration_since(tick_started).unwrap();
            if going_for.as_secs_f32() > max_secs {
                return true;
            }
        }

        self.now = next_tick.unwrap_or(self.now);

        (self.jobs_queuing.len() + self.jobs_running.len() > 0) || self.job_factory.more_jobs()
    }
}
