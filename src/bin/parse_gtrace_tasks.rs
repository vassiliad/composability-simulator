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

// The google dataset 2011-v2 can be found here: https://github.com/google/cluster-data/blob/master/ClusterData2011_2.md
// it is licensed under CC-BY license: https://creativecommons.org/licenses/by/4.0/

use std::collections::HashMap;
use std::env::args;
use std::fs::File;
use std::fs::read_dir;
use std::io::BufRead;
use std::io::BufReader;
use std::io::BufWriter;
use std::io::Write;
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;
use std::sync::Mutex;

use anyhow::{bail, Context, Result};

#[derive(Debug)]
struct TaskDef {
    time_created: f32,
    time_started: f32,
    time_done: f32,
    cores: f32,
    memory: f32,
}

type Draft = HashMap<(usize, usize), TaskDef>;
type Book = Vec<TaskDef>;

enum TaskEvent {
    // A task or job became eligible for scheduling.
    Submit,

    // A job or task was scheduled on a machine. (It may not start running
    // immediately due to code-shipping time, etc.) For jobs, this occurs the first time any
    // task of the job is scheduled on a machine.
    Schedule,

    // A task or job was descheduled because of a higher priority task or job,
    // because the scheduler overcommitted and the actual demand exceeded the machine
    // capacity, because the machine on which it was running became unusable (e.g. taken
    // offline for repairs), or because a disk holding the task’s data was lost.
    Evict,

    // A task or job was descheduled (or, in rare cases, ceased to be eligible for
    // scheduling while it was pending) due to a task failure.
    Fail,

    // A task or job completed normally.
    Finish,

    // A task or job was cancelled by the user or a driver program or because
    // another job or task on which this job was dependent died.
    Kill,

    // A task or job was presumably terminated, but a record indicating its
    // termination was missing from our source data.
    Lost,

    // A task or job’s scheduling class, resource requirements, or
    // constraints were updated while it was waiting to be scheduled.
    UpdatePending,

    // A task or job’s scheduling class, resource requirements, or
    // constraints were updated while it was scheduled.
    UpdateRunning,
}

impl FromStr for TaskEvent {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "0" => Ok(TaskEvent::Submit),
            "1" => Ok(TaskEvent::Schedule),
            "2" => Ok(TaskEvent::Evict),
            "3" => Ok(TaskEvent::Fail),
            "4" => Ok(TaskEvent::Finish),
            "5" => Ok(TaskEvent::Kill),
            "6" => Ok(TaskEvent::Lost),
            "7" => Ok(TaskEvent::UpdatePending),
            "8" => Ok(TaskEvent::UpdateRunning),
            n => bail!("Unable to convert \"{}\" into a TaskEvent", n)
        }
    }
}

/// Discovers files in the top level of @path_to_root and returns a sorted Vector
/// containing the file paths.
fn find_files_in_directory<P: AsRef<Path>>(path_to_root: P) -> Result<Vec<String>> {
    let path_to_root = path_to_root.as_ref();

    let mut entries = read_dir(path_to_root)
        .context(format!("Unable to list entries under {}", path_to_root.display()))?;
    let mut paths: Vec<_> = entries.filter_map(|entry| {
        let entry = entry.ok()?;
        let metadata = entry.metadata().ok()?;
        let path = entry.path();
        let path = path.to_str().unwrap();

        if metadata.is_file() && path.ends_with(".csv") {
            Some(path.to_string())
        } else {
            None
        }
    }).collect();

    paths.sort();

    Ok(paths)
}

fn extract_tasks_from_traces<P: AsRef<Path>>(path_to_root: P, path_output: P) -> Result<()> {
    let mut book = Book::new();
    let mut draft = Draft::new();
    let paths = find_files_in_directory(path_to_root)?;

    println!("Discovered {} files", paths.len());
    let mut total_tasks = 0;
    let mut total_tasks_dropped = 0;

    let stop_processing = Arc::new(Mutex::new(AtomicBool::new(false)));
    let handler_stop = stop_processing.clone();
    ctrlc::set_handler(move || {
        eprintln!("Caught SIG INT, program will stop after it finishes processing \
            the current file.");
        handler_stop.lock().unwrap().store(true, Ordering::SeqCst);
    }).expect("Unable to set handler for SIG INT");

    let path_output = path_output.as_ref().clone();
    let file = File::create(path_output)
        .context(format!("Unable to open {} for output", path_output.display()))?;
    let mut writer = std::io::BufWriter::new(file);

    writeln!(&mut writer, "#time_created:f32;time_started:f32;time_done:f32;cores:f32;memory:f32")?;
    let mut drop_tasks_created_before: f32 = f32::MAX;

    for p in paths {
        println!("Processing {}", p);
        let (tasks_new, tasks_dropped) = digest_task_events_into_book(
            &mut book, &mut draft, p)?;

        let mut earliest_unsheduled = f32::MAX;
        let mut earliest_running = f32::MAX;
        drop_tasks_created_before = f32::MAX;
        for t in draft.values() {
            drop_tasks_created_before = drop_tasks_created_before.min(t.time_created);
            earliest_unsheduled = earliest_unsheduled.min(t.time_created);

            if t.time_started > -1. {
                earliest_running = earliest_running.min(t.time_started);
            }
        }
        total_tasks += tasks_new;
        total_tasks_dropped += tasks_dropped;

        println!("  Discovered {} new tasks, dropped {} tasks, draft tasks {}",
                 tasks_new, tasks_dropped, draft.len());
        println!("  Total tasks ({}) {}, total dropped tasks {}",
                 book.len(), total_tasks, total_tasks_dropped);

        let drop_before = book.partition_point(
            |task| task.time_created <= drop_tasks_created_before);

        if drop_before > 0 {
            println!("  It is safe to store the first {} tasks, earliest draft task is {}",
                     drop_before, drop_tasks_created_before);

            for idx in 0..drop_before {
                let t = &book[idx];
                writeln!(&mut writer, "{};{};{};{};{}",
                         t.time_created, t.time_started, t.time_done, t.cores, t.memory)?;
            }

            book.drain(0..drop_before);
            writer.flush();
        } else {
            println!("  Cannot store any tasks because earliest draft-task is {}",
                     drop_tasks_created_before);
        }

        println!("  ! Earliest pending {}, earliest running {}",
                 earliest_unsheduled, earliest_running);
        if book.len() > 0 {
            println!("Earliest task {:#?}", book[0]);
        }

        if stop_processing.lock().unwrap().load(Ordering::SeqCst) {
            println!("Stop processing new task events due to SIG INT");
            break;
        }
    }

    for t in &book {
        writeln!(&mut writer, "{};{};{};{};{}",
                 t.time_created, t.time_started, t.time_done, t.cores, t.memory)?;
    }

    Ok(())
}

fn digest_task_events_into_book<P: AsRef<Path>>(
    book: &mut Book,
    draft: &mut Draft,
    path: P,
) -> Result<(usize, usize)> {
    let path = path.as_ref();
    let mut num_full_tasks = 0;
    let mut num_dropped_tasks = 0;

    let file = File::open(&path)
        .context(format!("Unable to open input file {}", path.display()))?;

    let mut latest = 0.0;
    let br = BufReader::new(file);
    for (i, line) in br.lines().enumerate() {
        let line = line
            .context(format!("Unable to read line {} of {}", i, path.display()))?;

        // VV: columns:
        //     0. timestamp
        //     1. missing info
        //     2. job ID
        //     3. task index - within the job
        //     4. machine ID
        //     5. event type
        //     6. user name
        //     7. scheduling class
        //     8. priority
        //     9. resource request for CPU cores
        //     10. resource request for RAM
        //     11. resource request for local disk space
        //     12. different-machine constraint

        let toks: Vec<&str> = line.split(',').collect();

        let task_event: TaskEvent = toks[5].parse()
            .with_context(|| format!("Unable to parse TaskEvent from line {} = {} of {}",
                                     i, line, path.display()))?;

        let job_id: usize = toks[2].parse()
            .with_context(|| format!("Unable to parse JobId from line {} = {} of {}",
                                     i, line, path.display()))?;
        let task_index: usize = toks[3].parse()
            .with_context(|| format!("Unable to parse task_index from line {} = {} of {}",
                                     i, line, path.display()))?;
        let key = (job_id, task_index);

        let timestamp = || -> Result<f32> {
            let timestamp = toks[0]
                .parse::<f64>()
                .with_context(|| format!("Unable to parse timestamp from line {} = {} of {}",
                                         i, line, path.display()))?;
            let timestamp = (timestamp / 1e6) as f32;

            Ok(timestamp)
        };

        let cores = || -> Result<f32> {
            let cores = toks[9]
                .parse::<f32>()
                .with_context(|| format!("Unable to parse cores from line {} = {} of {}",
                                         i, line, path.display()))?;
            Ok(cores)
        };

        let memory = || -> Result<f32> {
            let memory = toks[10]
                .parse::<f32>()
                .with_context(|| format!("Unable to parse memory from line {} = {} of {}",
                                         i, line, path.display()))?;
            Ok(memory)
        };

        match task_event {
            TaskEvent::Submit => {
                // VV: Tasks may be re-submit it, for the sake of simplicity
                // get rid of them
                let timestamp = timestamp()?;
                latest = timestamp;
                if draft.contains_key(&key) {
                    num_dropped_tasks += 1;
                    draft.remove(&key);
                }

                if timestamp == 0. {
                    num_dropped_tasks += 1;
                    continue
                }

                let cores = cores();
                if cores.is_err() {
                    continue;
                }

                let memory = memory();
                if memory.is_err() {
                    continue;
                }

                let task = TaskDef {
                    time_created: timestamp,
                    time_started: -1.,
                    time_done: -1.,
                    cores: cores?,
                    memory: memory?,
                };

                if task.cores == 0.0 || task.memory == 0.0 {
                    num_dropped_tasks += 1;
                    continue
                }

                draft.insert(key, task);
            }

            TaskEvent::Schedule => {
                if draft.contains_key(&key) == false {
                    // VV: Must have been the wind ...
                    continue
                }
                let mut task = draft.get_mut(&key).unwrap();

                task.time_started = timestamp()?;
                latest = task.time_started;
            }

            TaskEvent::Kill | TaskEvent::Evict | TaskEvent::Finish | TaskEvent::Fail => {
                if draft.contains_key(&key) == false {
                    // VV: Must have been the wind ...
                    continue;
                }

                let mut task = draft.remove(&key).unwrap();

                if task.time_started < 0. {
                    // VV: We've never registered this Job before
                    continue;
                }

                task.time_done = timestamp()?;
                latest = task.time_done;

                let idx = book.partition_point(|t| {
                    t.time_created < task.time_created
                });


                book.insert(idx, task);
                num_full_tasks += 1;
            }

            TaskEvent::UpdateRunning | TaskEvent::UpdatePending | TaskEvent::Lost => {
                // VV: bb incomplete task definition, or task definition who decided to change
                // you will be missed but never forgotten.
                draft.remove(&key);

                latest = timestamp()?;
            }
        }
    }

    // VV: if there're any weird tasks PENDING for over 8 hours throw those out
    // VV: If there're any weird tasks RUNNING for longer than 24 hours then mark them as done

    let mut removed: Vec<(usize, usize)> = vec![];

    for (key, task) in draft.iter_mut() {
        if task.time_started == -1. {
            if latest - task.time_created > (8 * 3600) as f32 {
                removed.push(*key);
            }
        } else if task.time_done == -1.0 {
            if latest - task.time_started > (24 * 3600) as f32 {
                task.time_done = latest;
                removed.push(*key);
            }
        } else {
            panic!("Task {:#?} should have been moved to book already", task);
        }
    }

    for key in &removed {
        if let Some(task) = draft.remove(key) {
            if task.time_done != -1.0 {
                let idx = book.partition_point(|t| {
                    t.time_created < task.time_created
                });

                num_full_tasks += 1;
                book.insert(idx, task);
            }
        }
    }

    println!("   Simulated time is currently: {}", latest);
    num_dropped_tasks += removed.len();
    Ok((num_full_tasks, num_dropped_tasks))
}


fn main() -> Result<()> {
    let args: Vec<String> = args().collect();
    let path_to_root = Path::new(&args[1]);
    let path_output = Path::new(&args[2]);
    extract_tasks_from_traces(path_to_root, path_output)?;
    Ok(())
}