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

use std::path::PathBuf;
use std::time::SystemTime;

use anyhow::bail;
use anyhow::Result;
use clap::Arg;
use clap::Command;
use rand::{Rng, thread_rng};
use rand::distributions::Alphanumeric;

use registry::ParetoPoint;

mod job;
mod job_factory;

mod node;
mod registry;
mod resource;

mod scheduler;

#[derive(Debug)]
enum JobFactoryType {
    Jobs,
    Workflow,
}

#[derive(Debug)]
struct Arguments {
    node_definitions: PathBuf,
    node_connections: Option<PathBuf>,
    job_factory_type: JobFactoryType,
    job_factory_file: PathBuf,
    output_trace_file: PathBuf,
}

fn parse_arguments() -> Result<Arguments> {
    let cmd = Command::new("Composability Simulator")
        .version("0.1.0")
        .author("Vassilis Vassiliadis")
        .about("Generates a trace of Job executing on a potentially disaggregated memory system")
        .arg(Arg::new("nodeDefinitionsFile")
            .short('d')
            .long("nodeDefinitionsFile")
            .takes_value(true)
            .required(true)
            .help("Path to file containing the definition of nodes"))
        .arg(Arg::new("nodeConnectionsFile")
            .short('c')
            .long("nodeConnectionsFile")
            .takes_value(true)
            .required(false)
            .help("Path to file containing the connections between nodes"))
        .arg(Arg::new("jobFactoryType")
            .short('t')
            .long("jobFactoryType")
            .possible_value("jobs")
            .possible_value("workflow")
            .takes_value(true)
            .default_value("jobs")
            .required(false)
            .help("Sets the schema of the jobFactoryFile, options are \"jobs\" and \"workflow\" \
                defaults to \"jobs\""))
        .arg(Arg::new("jobFactoryFile")
            .short('j')
            .long("jobFactoryFile")
            .takes_value(true)
            .required(true)
            .help("Path to file containing the definition of the jobFactory (see jobFactoryType)"))
        .arg(Arg::new("outputTraceFile")
            .short('o')
            .long("outputTraceFile")
            .takes_value(true)
            .required(false)
            .default_value("")
            .help("Path to store the output trace file under, set to \"\" to use \
                ${random}.trace file"));

    let args = cmd.get_matches();
    let node_definitions = args.value_of("nodeDefinitionsFile").unwrap();
    let node_definitions = PathBuf::from(node_definitions);
    let node_connections = match args.value_of("nodeConnectionsFile") {
        Some(node_connections) => Some(PathBuf::from(node_connections)),
        None => None,
    };

    let job_factory_type = args.value_of("jobFactoryType").unwrap();
    let job_factory_type = match job_factory_type {
        "jobs" => JobFactoryType::Jobs,
        "workflow" => JobFactoryType::Workflow,
        _ => bail!("Unknown jogFactoryType value {job_factory_type}")
    };

    let job_factory_file = args.value_of("jobFactoryFile").unwrap();
    let job_factory_file = PathBuf::from(job_factory_file);

    let output_trace_file = args.value_of("outputTraceFile").unwrap();
    let output_trace_file = match output_trace_file {
        "" => {
            let mut rng = thread_rng();
            let s: String = (0..7).map(|_| rng.sample(Alphanumeric) as char).collect();
            format!("{s}.trace").to_string()
        }
        x => x.to_string()
    };
    let output_trace_file = PathBuf::from(output_trace_file);


    Ok(
        Arguments {
            node_definitions,
            node_connections,
            job_factory_type,
            job_factory_file,
            output_trace_file,
        }
    )
}

fn main() -> Result<()> {
    let arguments = parse_arguments()?;

    println!("Instantiating node registry");
    let mut registry = registry::NodeRegistry::new();
    registry.load_nodes(&arguments.node_definitions)?;
    if let Some(node_connections) = arguments.node_connections {
        registry.load_connections(&node_connections)?;
    }

    println!("Instantiating job factory");
    let jfactory: Box<dyn job_factory::JobFactory> = match arguments.job_factory_type {
        JobFactoryType::Jobs => {
            let jf = job_factory::JobStreamingWithOutput::
            from_path_to_path(&arguments.job_factory_file, &arguments.output_trace_file)?;
            Box::new(jf)
        },
        JobFactoryType::Workflow => {
            let jf = job_factory::JobWorkflowFactory::
            from_path_to_path(&arguments.job_factory_file, &arguments.output_trace_file)?;
            Box::new(jf)
        }
    };

    println!("Instantiating scheduler");
    let mut sched = scheduler::Scheduler::new(registry, jfactory);

    let first_job_at = match sched.job_factory.job_peek() {
        Some(j) => if j.time_created == f32::MAX { 0.0 } else { j.time_created },
        None => 0.0,
    };

    println!("Starting simulation - first job arrives at {first_job_at}");

    let report_every_secs = 5.0;
    let mut last_report_time = SystemTime::now();
    let start = last_report_time;

    let mut throughput_last = 0;
    let mut throughput_delta = 0;

    while sched.tick() {
        let now = SystemTime::now();
        let delta = now.duration_since(last_report_time).unwrap();

        if delta.as_secs_f32() > report_every_secs {
            let throughput = sched.jobs_running.len()
                + sched.jobs_done.len()
                + sched.jobs_queuing.len();
            throughput_delta += throughput - throughput_last;
            throughput_last = throughput;
            last_report_time = now;

            let since_beg = now.duration_since(start).unwrap();
            println!("{:#?}) At tick {}, finished: {} - running: {} - queueing: {}",
                     since_beg, sched.now, sched.jobs_done.len(), sched.jobs_running.len(),
                     sched.jobs_queuing.len());

            let idle = sched.registry.nodes.iter().filter(|&n| {
                n.memory.capacity == n.memory.current &&
                    n.cores.capacity == n.cores.current
            }).count();

            println!("  Idle nodes {idle}/{}", sched.registry.nodes.len());

            println!("  Nodes in pareto front:");
            let pareto = sched.registry.pareto(true);
            for &ParetoPoint(uid, cores, memory) in &pareto {
                println!("    {}: cores {}, memory {}",
                         sched.registry.nodes[uid].name, cores, memory);
            }

            println!("  Simulator throughput events: {}", throughput_delta);
            println!("  Simulator throughput events/sec: {}",
                     throughput_delta as f32 / (delta.as_secs_f32()));
            throughput_delta = 0;
        }
        if sched.has_unschedulable() {
            break;
        }
    }
    let delta = SystemTime::now().duration_since(start).unwrap();

    println!("{}) Scheduled {} jobs in simulated seconds {}",
             delta.as_secs_f32(), sched.jobs_done.len(), sched.now - first_job_at);

    if sched.has_unschedulable() {
        eprintln!("There were {} unschedulable jobs", sched.jobs_queuing.len());

        for j in &sched.jobs_queuing {
            println!("{}", j);
        }

        bail!("Unable to schedule {} jobs", sched.jobs_queuing.len())
    } else {
        Ok(())
    }
}
