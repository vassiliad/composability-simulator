use dismem::job::Job;
use dismem::job_factory::JobCollection;
use dismem::registry::NodeRegistry;
use dismem::scheduler::Scheduler;

fn registry_init_homogeneous(
    nodes: usize,
    cores: f32,
    memory: f32,
) -> Result<NodeRegistry, String> {
    let mut reg = NodeRegistry::new();

    for i in 0..nodes {
        reg.new_node(&format!("{}", i), cores, memory)?;
    }

    Ok(reg)
}

fn jobfactory_init_homogeneous(
    jobs_created: &Vec<f32>,
    cores: f32,
    memory: f32,
    duration: f32,
    can_borrow: bool,
) -> JobCollection {
    let jobs: Vec<_> = jobs_created
        .iter()
        .map(|created| Job::new(cores, memory, duration, can_borrow, *created))
        .collect();

    JobCollection::new(jobs)
}

#[cfg(test)]
#[test]
fn test_scheduler_vanilla() -> Result<(), String> {
    let reg = registry_init_homogeneous(2, 1., 1.)?;
    let job_created: Vec<_> = vec![0.0, 1., 2., 3.];

    let job_factory = jobfactory_init_homogeneous(&job_created, 1.0, 1.0, 5.0, false);

    let mut sched = Scheduler::new(reg, job_factory);

    while sched.tick() {}

    assert!(sched.job_factory.jobs_done.len() == 4);

    Ok(())
}
