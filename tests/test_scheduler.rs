use anyhow::Result;

use dismem::job::Job;
use dismem::job::reset_job_metadata;
use dismem::job_factory::JobCollection;
use dismem::registry::NodeRegistry;
use dismem::scheduler::Scheduler;

#[cfg(test)]
mod test_scheduler {
    use dismem::registry::ParetoPoint;

    use super::*;

    fn registry_init_homogeneous(
        nodes: usize,
        cores: f32,
        memory: f32,
    ) -> Result<NodeRegistry> {
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
        reset_job_metadata();

        let jobs: Vec<_> = jobs_created
            .iter()
            .enumerate()
            .map(|(uid, created)| {
                Job::new_with_uid(uid, cores, memory, duration, can_borrow, *created)
            })
            .collect();

        JobCollection::new(jobs)
    }

    #[test]
    fn scheduler_vanilla_small() -> Result<()> {
        let reg = registry_init_homogeneous(2, 1., 1.)?;
        let job_created: Vec<_> = vec![0.0, 1., 2., 3.];

        let job_factory = jobfactory_init_homogeneous(&job_created, 1.0, 1.0, 5.0, false);

        let mut sched = Scheduler::new(reg, Box::new(job_factory));

        while sched.tick() {}

        assert_eq!(sched.job_factory.jobs_done().len(), 4);
        assert_eq!(sched.now, 11.0);
        Ok(())
    }

    #[test]
    fn schedule_vanilla_large() -> Result<()> {
        let reg = registry_init_homogeneous(100, 1., 1.)?;
        let num_jobs = 100;

        let job_created: Vec<_> = vec![0.0; num_jobs];

        let job_factory = jobfactory_init_homogeneous(
            &job_created, 1.0, 1.0, 5.0, false);

        let mut sched = Scheduler::new(reg, Box::new(job_factory));

        while sched.tick() {}

        assert_eq!(sched.job_factory.jobs_done().len(), num_jobs);

        assert_eq!(sched.now, 5.0);

        Ok(())
    }

    #[test]
    fn unschedulable_vanilla() -> Result<()> {
        let reg = registry_init_homogeneous(100, 1., 1.)?;
        let num_jobs = 100;

        reset_job_metadata();

        let mut jobs: Vec<_> = (0..num_jobs)
            .map(|uid| Job::new_with_uid(uid, 1.0, 1.0, 5.0,
                                         false, 0.0))
            .collect();

        jobs.push(Job::new_with_uid(101, 100.0, 100.0, 5.0,
                                    false, 0.0));

        let job_factory = JobCollection::new(jobs);

        let mut sched = Scheduler::new(reg, Box::new(job_factory));

        while sched.tick() && !sched.has_unschedulable() {}

        assert_eq!(sched.job_factory.jobs_done().len(), num_jobs);

        assert_eq!(sched.now, 5.0);
        assert_eq!(sched.jobs_queuing.len(), 1);

        Ok(())
    }

    #[test]
    fn scheduler_dismem_small() -> Result<()> {
        let mut reg = NodeRegistry::new();
        reg.new_node("CPU", 4.0, 0.0)?;
        reg.new_node("RAM", 0.0, 2.0)?;
        reg.new_node("RAM but unusable", 0.0, 2.0)?;

        reg.new_connection_from_str("CPU;RAM")?;
        reg.new_connection_from_str("RAM;")?;

        let job_created: Vec<_> = vec![0.0, 1., 2., 3.];

        let job_factory = jobfactory_init_homogeneous(&job_created, 1.0, 1.0, 5.0, true);

        let mut sched = Scheduler::new(reg, Box::new(job_factory));

        while sched.tick() {}

        assert_eq!(sched.job_factory.jobs_done().len(), 4);
        assert_eq!(sched.now, 11.0);
        Ok(())
    }

    #[test]
    fn scheduler_dismem_small_with_2_lenders() -> Result<()> {
        let mut reg = NodeRegistry::new();
        reg.new_node("CPU", 3.0, 0.0)?;
        reg.new_node("RAM", 0.0, 2.0)?;
        reg.new_node("RAM more", 0.0, 2.0)?;

        reg.new_connection_from_str("CPU;*")?;
        reg.new_connection_from_str("RAM;")?;

        let job_created: Vec<_> = vec![0.0, 1., 2., 3.];

        let job_factory = jobfactory_init_homogeneous(&job_created, 1.0, 1.0, 5.0, true);

        let mut sched = Scheduler::new(reg, Box::new(job_factory));

        while sched.tick() {}

        assert_eq!(sched.job_factory.jobs_done().len(), 4);
        assert_eq!(sched.now, 10.0);
        Ok(())
    }

    #[test]
    fn registry_pareto() -> Result<()> {
        let mut reg = NodeRegistry::new();
        reg.new_node("CPU1", 3.0, 0.1)?;
        reg.new_node("RAM", 0.0, 2.0)?;
        reg.new_node("RAM more", 0.0, 2.0)?;
        reg.new_node("CPU2", 4.0, 0.1)?;

        let pareto = reg.pareto(true);

        for ParetoPoint(uid, _cores, _memory) in &pareto {
            println!("Node: {:#?}", reg.nodes[*uid])
        }

        let uids: Vec<_> = pareto.iter().map(|p| { p.0 }).collect();
        assert_eq!(uids, [3]);

        Ok(())
    }

    #[test]
    fn registry_pareto_equal() -> Result<()> {
        let mut reg = NodeRegistry::new();
        reg.new_node("CPU_FEW_CORES", 4.0, 0.1)?;
        reg.new_node("RAM", 0.0, 2.0)?;
        reg.new_node("RAM more", 0.0, 2.0)?;
        reg.new_node("CPU_FEW_CORES_AGAIN", 4.0, 0.1)?;

        reg.new_connection_from_str("CPU_FEW_CORES;*")?;
        reg.new_connection_from_str("CPU_FEW_CORES_AGAIN;*")?;
        reg.new_connection_from_str("RAM;")?;

        let pareto = reg.pareto(true);

        for ParetoPoint(uid, _cores, _memory) in &pareto {
            println!("Node: {:#?}", reg.nodes[*uid])
        }
        let uids: Vec<_> = pareto.iter().map(|p| { p.0 }).collect();
        assert_eq!(uids, [0]);

        Ok(())
    }
}
