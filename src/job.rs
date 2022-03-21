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

use std::fmt::Display;
use std::fmt::Formatter;
use std::str::FromStr;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use anyhow::Context;

static NEXT_JOB_UID: AtomicUsize = AtomicUsize::new(0);
static mut LAST_TIME_CREATED: f32 = 0.0;

pub fn reset_job_metadata() {
    unsafe {
        LAST_TIME_CREATED = 0.0;
    }
    NEXT_JOB_UID.store(0, Ordering::SeqCst);
}

#[derive(Debug, Clone)]
pub struct Job {
    pub uid: usize,
    pub cores: f32,
    pub memory: f32,
    pub can_borrow: bool,
    pub duration: f32,
    pub time_created: f32,
    pub time_started: Option<f32>,
    pub time_done: Option<f32>,
    // VV: uid(s) of nodes
    pub node_cores: Option<usize>,
    pub node_memory: Vec<(usize, f32)>,
}

impl Job {
    #[allow(dead_code)]
    pub fn new(
        cores: f32,
        memory: f32,
        duration: f32,
        can_borrow: bool,
        time_created: f32,
    ) -> Self {
        let uid = NEXT_JOB_UID.fetch_or(0, Ordering::SeqCst);

        Self::new_with_uid(uid, cores, memory, duration, can_borrow, time_created)
    }

    pub fn new_with_uid(
        uid: usize,
        cores: f32,
        memory: f32,
        duration: f32,
        can_borrow: bool,
        time_created: f32,
    ) -> Self {
        let cur_uid = NEXT_JOB_UID.fetch_add(1, Ordering::SeqCst);

        if cur_uid != uid {
            println!(
                "Should not create Job with UID {}, \
            first create the Job with UID {}",
                uid, cur_uid
            );
        }

        // VV: There's always a first! - is it all downhill from now on?
        // Basically, I need to create a *different* Object which keeps track of metadata
        // for creating jobs such as next uid, and last_time_created.
        // Currently, you will get these prints periodically when running `cargo test`
        unsafe {
            if LAST_TIME_CREATED > time_created {
                println!(
                    "Should not create Job with UID {} with time_create {} because a job with \
                a future time_create ({}) exists.",
                    uid, time_created, LAST_TIME_CREATED
                );
            }

            LAST_TIME_CREATED = time_created;
        }

        Self {
            uid,
            cores,
            memory,
            can_borrow,
            duration,
            time_created,
            time_started: None,
            time_done: None,
            node_cores: None,
            node_memory: vec![],
        }
    }
}

impl Display for Job {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        // VV: format is:
        // uid:usize;cores:f32;memory:f32;duration:f32;can_borrow:y/n,
        // time_created:f32(sec);time_started:f32(sec);time_done:f32(sec),
        // node_cores_uid:usize;[node_memory_uid:usize;memory:f32]+
        if self.time_done.is_some() {
            write!(f, "{};{};{};{};{};{};{};{};{}",
                   self.uid, self.cores, self.memory, self.duration,
                   if self.can_borrow { 'y' } else { 'n' }, self.time_created,
                   self.time_started.unwrap(), self.time_done.unwrap(), self.node_cores.unwrap())?;

            for (node, mem) in &self.node_memory {
                write!(f, ";{};{}", node, mem)?;
            }
            std::fmt::Result::Ok(())
        } else if self.time_started.is_some() {
            write!(f, "{};{};{};{};{};{};{};null;{}",
                   self.uid, self.cores, self.memory, self.duration,
                   if self.can_borrow { 'y' } else { 'n' }, self.time_created,
                   self.time_started.unwrap(), self.node_cores.unwrap())?;
            for (node, mem) in &self.node_memory {
                write!(f, ";{};{}", node, mem)?;
            }
            std::fmt::Result::Ok(())
        } else {
            write!(f, "{};{};{};{};{};{};null;null;null",
                   self.uid, self.cores, self.memory, self.duration,
                   if self.can_borrow { 'y' } else { 'n' }, self.time_created)
        }
    }
}

impl FromStr for Job {
    type Err = String;
    /// Format is:
    ///
    /// uid:usize; cores:f32; memory:f32; duration:f32; can_borrow:y/n;
    /// time_created:f32(sec); time_started:f32(sec); time_done:f32(sec);
    /// node_cores_uid:usize; 
    /// [node_memory_uid:usize; memory:f32]+
    ///
    /// "uid" can also be '?' to auto-pick number.
    /// First job must have uid 0, subsequent jobs must increase uid by 1 and Jobs
    /// cannot skip UID values.
    ///
    /// Just use '?' for the parser to pick the appropriate UID.
    fn from_str(line: &str) -> Result<Self, <Self as FromStr>::Err> {
        let tokens: Vec<_> = line.split(';').map(|s| s.trim()).collect();
        let num = tokens.len();

        if num != 6 && num != 9 && !(num > 9 && (num - 9) % 2 == 0) {
            return Err(format!(
                "Expected 6 or 9 or 9+2*N tokens partitioned by ';' in string but found {}: \"{:?}\"",
                num, tokens));
        }

        let uid: usize = if tokens[0] == "?" {
            NEXT_JOB_UID.fetch_or(0, Ordering::SeqCst)
        } else if let Ok(c) = tokens[0].parse() {
            c
        } else {
            return Err(format!("Invalid uid \"{}\"", tokens[0]));
        };

        let cores: f32 = if let Ok(c) = tokens[1].parse() {
            c
        } else {
            return Err(format!("Invalid cores \"{}\"", tokens[1]));
        };

        let memory: f32 = if let Ok(c) = tokens[2].parse() {
            c
        } else {
            return Err(format!("Invalid memory \"{}\"", tokens[2]));
        };

        let duration: f32 = if let Ok(c) = tokens[3].parse() {
            c
        } else {
            return Err(format!("Invalid duration \"{}\"", tokens[3]));
        };

        let borrow: bool = if tokens[4] == "y" {
            true
        } else if tokens[4] == "n" {
            false
        } else {
            return Err(format!("borrow may only be y or n but found \"{}\"", tokens[4]));
        };

        let time_created: f32 = if let Ok(c) = tokens[5].parse() {
            c
        } else {
            return Err(format!("Invalid time_created \"{}\"", tokens[5]));
        };

        let mut job = Self::new_with_uid(
            uid,
            cores,
            memory,
            duration,
            borrow,
            time_created,
        );

        if num > 6 {
            if let Ok(c) = tokens[6].parse() {
                job.node_cores = Some(c);
            } else {
                return Err(format!("Invalid uid_cores \"{}\"", tokens[6]));
            }

            for i in (7..num).step_by(2) {
                let mem_node: usize = tokens[i].parse()
                    .with_context(|| format!("Invalid node_memory_uid \"{}\"", tokens[i]))
                    .unwrap();
                let mem_memory: f32 = tokens[i + 1].parse()
                    .with_context(|| format!("Invalid node_memory_memory \"{}\"", tokens[i + 1]))
                    .unwrap();
                job.node_memory.push((mem_node, mem_memory));
            }
        }

        Ok(job)
    }
}
