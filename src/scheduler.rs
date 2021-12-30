use crate::job::Job;
use crate::job_factory::JobFactory;
use crate::registry::NodeRegistry;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::hash::Hash;

pub struct Scheduler<T>
where
    T: JobFactory,
{
    registry: NodeRegistry,
    job_factory: T,
    now: f32,
    jobs_done: HashSet<String>,
    jobs_running: VecDeque<Job>,
    jobs_queuing: VecDeque<Job>,
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
}
