use crate::job::reset_job_metadata;
use crate::job::Job;
use std::collections::VecDeque;
use std::fs::File;
use std::io::BufRead;
use std::io::BufReader;
use std::io::Cursor;
use std::path::Path;

pub trait JobFactory {
    fn job_peek(&self) -> Option<&Job>;
    fn job_get(&mut self) -> Job;
    fn job_mark_done(&mut self, _job: &Job) {}
    fn more_jobs(&self) -> bool;
}

pub struct JobStreaming {
    pub reader: Box<dyn BufRead>,
    pub jobs_done: Vec<usize>,
    next_job: Option<Job>,
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

impl JobStreaming {
    pub fn from_path(path: &Path) -> Result<Self, String> {
        let file = File::open(path);
        if let Err(x) = file {
            return Err(format!(
                "Unable to open file \"{}\" because: {:?}",
                path.display(),
                x
            ));
        }

        let reader = Box::new(BufReader::new(file.unwrap())) as Box<dyn BufRead>;

        Ok(Self::from_reader(reader))
    }

    pub fn from_string(content: String) -> Result<Self, String> {
        let reader = Box::new(Cursor::new(content));
        Ok(Self::from_reader(reader))
    }

    pub fn from_reader(reader: Box<dyn BufRead>) -> Self {
        let mut me = Self {
            reader,
            jobs_done: vec![],
            next_job: None,
        };
        reset_job_metadata();

        me.may_read_line();

        me
    }

    fn may_read_line(&mut self) {
        let mut line: String = String::new();
        loop {
            let read = self.reader.read_line(&mut line);
            match read {
                Ok(0) => (break),
                Ok(_) => {
                    // VV: Skip empty lines, and lines starting with a "#"
                    line = line.trim().to_owned();

                    if line.starts_with("#") || line.len() == 0 {
                        line.clear();
                        continue;
                    }

                    self.next_job = Some(line.parse().unwrap());
                    break;
                }
                Err(x) => panic!("Could not read next line due to {}", x),
            }
        }
    }
}

impl JobFactory for JobStreaming {
    fn job_peek(&self) -> Option<&Job> {
        match &self.next_job {
            Some(x) => Some(&x),
            None => None,
        }
    }

    /// Consumes job and also reads the next availale job definition from the stream
    fn job_get(&mut self) -> Job {
        let cur_job = std::mem::replace(&mut self.next_job, None).unwrap();
        self.may_read_line();

        cur_job
    }

    fn job_mark_done(&mut self, job: &Job) {
        self.jobs_done.push(job.uid)
    }

    fn more_jobs(&self) -> bool {
        self.next_job.is_some()
    }
}
