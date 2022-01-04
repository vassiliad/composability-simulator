use std::env::args;
use std::path::Path;

mod job;
mod job_factory;

mod node;
mod registry;
mod resource;

mod scheduler;

fn main() -> Result<(), String> {
    let arguments: Vec<_> = args().collect();

    if arguments.len() != 1 + 3 {
        return Err(format!(
            "Expected arguments:
            <path to node definition> \
            <path to node connection definition> \
            <path to job definition>"
        ));
    }

    let path_nodes = Path::new(&arguments[1]);
    let path_connections = Path::new(&arguments[2]);
    let path_jobs = Path::new(&arguments[3]);

    let registry = registry::NodeRegistry::from_paths(path_nodes, path_connections)?;

    let jfactory = job_factory::JobStreaming::from_path(path_jobs)?;

    let mut sched = scheduler::Scheduler::new(registry, jfactory);

    let mut last_report: usize = 0;
    let report_every = 1000;
    while sched.tick() {

        if sched.jobs_done.len() >= report_every + last_report {
            last_report = sched.jobs_done.len();
            println!("Jobs finished {} on {}", last_report, sched.now);
        }
    }

    println!(
        "Scheduled {} jobs in simulated seconds {}",
        sched.jobs_done.len(),
        sched.now
    );

    Ok(())
}
