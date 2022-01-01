use crate::job::Job;
use std::collections::VecDeque;

pub trait JobFactory {
    fn job_peek(&self) -> Option<&Job>;
    fn job_get(&mut self) -> Job;
    fn job_mark_done(&mut self, _job: &Job) {}
    fn more_jobs(&self) -> bool;
}

pub struct JobCollection {
    pub jobs_done: Vec<usize>,
    pub jobs: VecDeque<Job>,
}

impl JobFactory for JobCollection {
    fn job_peek(&self) -> Option<&Job> {
        self.jobs.get(0)
    }

    fn job_get(&mut self) -> Job {
        self.jobs
            .pop_front()
            .expect("JobCollection is already empty")
    }

    fn job_mark_done(&mut self, job: &Job) {
        self.jobs_done.push(job.uid)
    }

    fn more_jobs(&self) -> bool {
        self.jobs.len() > 0
    }
}

impl JobCollection {
    pub fn new(jobs: Vec<Job>) -> Self {
        let jobs = VecDeque::from(jobs);
        Self {
            jobs,
            jobs_done: vec![],
        }
    }
}
