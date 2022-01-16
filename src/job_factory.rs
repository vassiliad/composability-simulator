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

use std::collections::VecDeque;
use std::fs::File;
use std::io::BufRead;
use std::io::BufReader;
use std::io::BufWriter;
use std::io::Cursor;
use std::io::Write;
use std::path::Path;

use anyhow::bail;
use anyhow::Result;

use crate::job::Job;
use crate::job::reset_job_metadata;

pub trait JobFactory {
    fn job_peek(&self) -> Option<&Job>;
    fn job_get(&mut self) -> Job;
    fn job_mark_done(&mut self, _job: &Job) {}
    fn more_jobs(&self) -> bool;
    fn jobs_done(&self) -> &Vec<usize>;
}

pub struct JobStreaming {
    pub reader: Box<dyn BufRead>,
    jobs_done: Vec<usize>,
    next_job: Option<Job>,
}

pub struct JobStreamingWithOutput {
    pub inner: JobStreaming,
    pub writer: Box<dyn Write>,
}

pub struct JobCollection {
    jobs_done: Vec<usize>,
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
        !self.jobs.is_empty()
    }

    fn jobs_done(&self) -> &Vec<usize> {
        &self.jobs_done
    }
}

impl JobCollection {
    #[allow(dead_code)]
    pub fn new(jobs: Vec<Job>) -> Self {
        let jobs = VecDeque::from(jobs);
        Self {
            jobs,
            jobs_done: vec![],
        }
    }
}

impl JobStreaming {
    pub fn from_path(path: &Path) -> Result<Self> {
        let file = File::open(path);
        if let Err(x) = file {
            bail!("Unable to open file \"{}\" because: {:?}", path.display(), x)
        }

        let reader = Box::new(BufReader::new(file.unwrap())) as Box<dyn BufRead>;

        Ok(Self::from_reader(reader))
    }

    #[allow(dead_code)]
    pub fn from_string(content: String) -> Result<Self> {
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
                Ok(0) => break,
                Ok(_) => {
                    // VV: Skip empty lines, and lines starting with a "#"
                    line = line.trim().to_owned();

                    if line.starts_with('#') || line.is_empty() {
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

impl JobStreamingWithOutput {
    fn make_writer(path: &Path) -> Result<Box<dyn Write>> {
        let file = File::create(path);
        if let Err(x) = file {
            bail!("Unable to create file \"{}\" because: {:?}", path.display(), x)
        }

        let mut writer = Box::new(BufWriter::new(file.unwrap())) as Box<dyn Write>;
        if let Err(x) = writeln!(writer, "#uid:usize;cores:f32;memory:f32;duration:f32;\
            can_borrow:y/n;time_created:f32;time_started:f32;time_done:f32;uid_node_cores:usize;\
            [uid_node_memory:usize;memory_alloc:f32]+") {
            bail!("Unable to write header to path {} because of {:?}", path.display(), x)
        }
        Ok(writer)
    }

    pub fn from_path_to_path(path: &Path, output_path: &Path) -> Result<Self> {
        let file = File::open(path);
        if let Err(x) = file {
            bail!("Unable to open file \"{}\" because: {:?}", path.display(), x)
        }

        let reader = Box::new(BufReader::new(file.unwrap())) as Box<dyn BufRead>;

        Self::from_reader_to_path(reader, output_path)
    }

    #[allow(dead_code)]
    pub fn from_string_to_path(content: String, output_path: &Path) -> Result<Self> {
        let reader = Box::new(Cursor::new(content));
        Self::from_reader_to_path(reader, output_path)
    }

    pub fn from_reader_to_path(reader: Box<dyn BufRead>, output_path: &Path) -> Result<Self> {
        let inner = JobStreaming::from_reader(reader);
        let writer = JobStreamingWithOutput::make_writer(output_path)?;
        Ok(Self { inner, writer })
    }
}

impl JobFactory for JobStreaming {
    fn job_peek(&self) -> Option<&Job> {
        match &self.next_job {
            Some(x) => Some(x),
            None => None,
        }
    }

    /// Consumes job and also reads the next available job definition from the stream
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

    fn jobs_done(&self) -> &Vec<usize> {
        &self.jobs_done
    }
}


impl JobFactory for JobStreamingWithOutput {
    fn job_peek(&self) -> Option<&Job> {
        self.inner.job_peek()
    }

    fn job_get(&mut self) -> Job {
        self.inner.job_get()
    }

    fn job_mark_done(&mut self, job: &Job) {
        self.inner.jobs_done.push(job.uid);
        writeln!(self.writer, "{}", job).unwrap();
        self.writer.flush().unwrap();
    }

    fn more_jobs(&self) -> bool {
        self.inner.more_jobs()
    }

    fn jobs_done(&self) -> &Vec<usize> {
        &self.inner.jobs_done
    }
}