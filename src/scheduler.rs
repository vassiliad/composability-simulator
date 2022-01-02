use crate::job::Job;
use crate::job_factory::JobFactory;
use crate::node::Node;
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

    jobs_queuing: VecDeque<Job>,
    jobs_running: VecDeque<Job>,
    jobs_done: HashSet<usize>,
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
        self.jobs_done.insert(job.uid);
        self.job_factory.job_mark_done(&job);

        let idx_node = job.node_cores.unwrap();
        let node = &self.registry.nodes[idx_node];

        let all_idx_cores = vec![self.registry.idx_sorted_cores(node)];
        // VV: after finding the index of the node in the sorted_cores array
        // it's now safe to free cores
        self.registry.nodes[idx_node].free_cores(job.cores);

        let all_idx_memory: Vec<usize> = job
            .node_memory
            .iter()
            .map(|&(idx_node, memory)| {
                let node = &self.registry.nodes[idx_node];
                let ret = self.registry.idx_sorted_memory(node);

                // VV: we know idx_memory, so now free memory
                let node = &mut self.registry.nodes[idx_node];
                node.free_memory(memory);

                ret
            })
            .collect();

        self.registry.resort_nodes_cores(&all_idx_cores);
        self.registry.resort_nodes_memory(&all_idx_memory);

        // println!(
        //     "Freeing {}x{} from cores {} and memory {:?}",
        //     job.cores,
        //     job.memory,
        //     job.node_cores.unwrap(),
        //     job.node_memory
        // );
    }

    fn job_allocate(registry: &mut NodeRegistry, job: &mut Job) -> bool {
        let pred_fit = |node: &Node| -> bool {
            node.cores.current < job.cores && node.memory.current < job.memory
        };

        let idx_cores = registry.index_bisect_right(&registry.sorted_cores, pred_fit, None, None);

        if idx_cores < registry.sorted_cores.len() {
            // VV: Found a node which can fit the entire Job, good job (hehe)
            let idx_node;

            idx_node = registry.sorted_cores[idx_cores];
            let node = &registry.nodes[idx_node];
            // println!("Can fit {}x{} to {}", job.cores, job.memory, node);

            // VV: Now find the idx of node in the sorted_memory array of indices
            let idx_memory = registry.idx_sorted_memory(node);

            // VV: finally, actually allocate the job on the node
            let node = &mut registry.nodes[idx_node];
            node.allocate_job(job.cores, job.memory);

            job.node_cores = Some(node.uid);
            job.node_memory.push((node.uid, job.memory));
            // VV: and then re-sort the sorted_cores and sorted_memory indices of
            // the registry. This ensures that the next allocation will find
            // the nodes in proper order.
            registry.resort_nodes_cores(&vec![idx_cores]);
            registry.resort_nodes_memory(&vec![idx_memory]);

            return true;
        }

        false
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

                    assert_eq!(job.time_done.unwrap(), self.now);

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
