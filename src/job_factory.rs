use crate::job::Job;
use std::collections::VecDeque;

pub trait JobFactory {
    fn job_peek(&self) -> Option<&Job>;
    fn job_get(&mut self, when: f32) -> Job;
    fn job_mark_done(&mut self, job: &Job) {}
}

struct JobCollection<T>
where
    T: Fn(&Job),
{
    jobs: VecDeque<Job>,
    cb_done: Option<T>,
}

impl<T> JobFactory for JobCollection<T>
where
    T: Fn(&Job),
{
    fn job_peek(&self) -> Option<&Job> {
        self.jobs.get(0)
    }

    fn job_get(&mut self, when: f32) -> Job {
        self.jobs
            .pop_front()
            .expect("JobCollection is already empty")
    }

    fn job_mark_done(&mut self, job: &Job) {
        match &self.cb_done {
            Some(cb_done) => cb_done(job),
            None => (),
        }
    }
}

impl<T> JobCollection<T>
where
    T: Fn(&Job),
{
    fn new(jobs: Vec<Job>, cb_done: Option<T>) -> Self {
        let jobs = VecDeque::from(jobs);
        Self { jobs, cb_done }
    }
}
