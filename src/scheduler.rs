use crate::job::Job;
use crate::job_factory::JobFactory;
use crate::registry::NodeRegistry;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::vec;

pub struct Scheduler<T>
where
    T: JobFactory,
{
    pub registry: NodeRegistry,
    pub job_factory: T,
    pub now: f32,

    pub jobs_queuing: VecDeque<Job>,
    pub jobs_running: VecDeque<Job>,
    pub jobs_done: HashSet<usize>,
}

impl<T> Scheduler<T>
where
    T: JobFactory,
{
    pub fn new(registry: NodeRegistry, job_factory: T) -> Self {
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

        let all_uids_cores = vec![uid_cores];
        // VV: after finding the index of the node in the sorted_cores array
        // it's now safe to free cores
        self.registry.nodes[uid_cores].free_cores(job.cores);

        let all_uids_memory: Vec<usize> = job
            .node_memory
            .iter()
            .map(|&(uid_memory, memory)| {
                let node = &mut self.registry.nodes[uid_memory];
                // println!(
                //     "Freeing {} memory from {} with max memory {}",
                //     memory, node.name, node.memory.capacity
                // );
                node.free_memory(memory);

                uid_memory
            })
            .collect();

        self.registry.resort_nodes_cores(&all_uids_cores);
        self.registry.resort_nodes_memory(&all_uids_memory);
    }

    fn job_allocate_on_single_node(
        registry: &mut NodeRegistry,
        idx_cores: usize,
        job: &mut Job,
    ) {
        let uid_node = registry.sorted_cores[idx_cores];

        // VV: finally, actually allocate the job on the node
        let node = &mut registry.nodes[uid_node];

        // println!("Can fit {}x{} to {}", job.cores, job.memory, node);

        node.allocate_job(job.cores, job.memory);

        job.node_cores = Some(node.uid);
        job.node_memory.push((node.uid, job.memory));
        // VV: and then re-sort the sorted_cores and sorted_memory indices of
        // the registry. This ensures that the next allocation will find
        // the nodes in proper order.
        registry.resort_nodes_cores(&vec![uid_node]);
        registry.resort_nodes_memory(&vec![uid_node]);
    }

    fn job_allocate_on_many_nodes(
        registry: &mut NodeRegistry,
        idx_cores: usize,
        idx_memory: usize,
        job: &mut Job,
    ) -> bool {
        let uid_cores = registry.sorted_cores[idx_cores];
        let lenders = registry.connections.get(&uid_cores).unwrap();
        let all_memory: Vec<usize> = registry.sorted_memory
            [idx_memory..registry.sorted_memory.len()]
            .iter()
            .filter_map(|x| if lenders.contains(x) { Some(*x) } else { None })
            .collect();
        // let all_memory = &registry.sorted_memory[idx_memory..registry.sorted_memory.len()];
        // VV: Assumes that *all* node scan steal memory from *any* other node
        let total_memory: f32 = all_memory
            .iter()
            .map(|idx: &usize| registry.nodes[*idx].memory.current)
            .sum::<f32>()
            + registry.nodes[uid_cores].memory.current;

        if total_memory < job.memory {
            // println!("Not enough memory {} for {} on {} from {:?} -- {:?}",
            //     total_memory, job.memory, uid_cores, lenders, registry.connections);
            return false;
        }

        let mut uids_memory = vec![];

        let mut rem_mem = job.memory;

        registry.nodes[uid_cores].allocate_cores(job.cores);

        let node_cores = &registry.nodes[uid_cores];
        job.node_cores = Some(node_cores.uid);

        if node_cores.memory.current > 0.0 {
            let alloc = rem_mem.min(node_cores.memory.current);
            job.node_memory.push((uid_cores, alloc));
            uids_memory.push(uid_cores);

            registry.nodes[uid_cores].allocate_memory(alloc);
            rem_mem -= alloc;
        }

        for uid_mem in &all_memory {
            if uid_mem != &uid_cores {
                let node_mem = &registry.nodes[*uid_mem];
                let alloc = rem_mem.min(node_mem.memory.current);
                job.node_memory.push((*uid_mem, alloc));

                uids_memory.push(*uid_mem);
                registry.nodes[*uid_mem].allocate_memory(alloc);
                rem_mem -= alloc;

                if rem_mem == 0.0 {
                    break;
                }
            }
        }

        // println!("Scheduling {}x{} on Cores:{:?}, Memory:{:?}",
        // job.cores, job.memory, job.node_cores, job.node_memory);

        registry.resort_nodes_cores(&vec![uid_cores]);
        registry.resort_nodes_memory(&uids_memory);

        assert_eq!(rem_mem, 0.0);

        true
    }

    fn job_allocate(registry: &mut NodeRegistry, job: &mut Job) -> bool {
        let cores = registry.idx_nodes_with_more_cores(job.cores);
        if cores == registry.sorted_cores.len() {
            return false;
        }
        let memory = registry.idx_nodes_with_more_memory(job.memory);

        if memory < registry.sorted_memory.len() {
            let all_memory = &registry.sorted_memory[memory..registry.sorted_memory.len()];

            let all_cores = &registry.sorted_cores[cores..registry.sorted_cores.len()];
            // VV: There's a chance one of the nodes with enough cores (all_cores) has enough
            // memory too (all_memory)
            let mut idx_cores = cores;

            for uid_cores in all_cores {
                if let Some(_) = all_memory.iter().position(|uid: &usize| uid == uid_cores)
                {
                    Self::job_allocate_on_single_node(registry, idx_cores, job);
                    return true;
                }

                idx_cores += 1;
            }
        }

        if job.can_borrow == false {
            return false;
        }

        let idx_memory = registry.idx_nodes_with_more_memory(0.0);

        for idx_cores in cores..registry.sorted_cores.len() {
            if Self::job_allocate_on_many_nodes(registry, idx_cores, idx_memory, job) == true {
                return true;
            }
        }

        return false;
    }

    pub fn tick(&mut self) -> bool {
        let mut next_tick: Option<f32> = None;
        let mut run_now: Vec<usize> = Vec::with_capacity(self.jobs_queuing.len().max(10).min(10));
        // println!("Now is {}", self.now);

        loop {
            let mut new_queueing = 0;
            let new_running;
            let mut new_done = 0;

            while self.jobs_running.len() > 0 {
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
                        None => job.time_done.clone(),
                    };

                    break;
                }
            }

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
                                None => Some(job.time_created.clone()),
                            };
                            break;
                        }
                    }
                    None => break,
                }
            }

            for (i, job) in self.jobs_queuing.iter_mut().enumerate() {
                if Self::job_allocate(&mut self.registry, job) {
                    run_now.push(i);
                }
            }

            new_running = run_now.len();

            if new_running > 0 {
                let mut q: VecDeque<Job> =
                    VecDeque::with_capacity(self.jobs_queuing.len() - new_running);
                let mut i = 0;
                while self.jobs_queuing.len() > 0 {
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
        }

        self.now = next_tick.unwrap_or(self.now);

        if (self.jobs_queuing.len() + self.jobs_running.len() > 0) || self.job_factory.more_jobs() {
            true
        } else {
            false
        }
    }
}
