use anyhow::Result;

use compsim::job::Job;
use compsim::job::reset_job_metadata;
use compsim::job_factory::JobCollection;
use compsim::job_factory::JobFactory;
use compsim::job_factory::JobWorkflowFactory;
use compsim::registry::NodeRegistry;
use compsim::scheduler::Scheduler;

#[cfg(test)]
mod test_scheduler {
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
        let mut steps = 0;

        while steps < 1000 && sched.tick() { steps += 1 }

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
        let mut steps = 0;

        while steps < 1000 && sched.tick() { steps += 1 }

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
        let mut steps = 0;

        while steps < 1000 && sched.tick() && !sched.has_unschedulable() { steps += 1 }

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

        let mut steps = 0;
        while sched.tick() && steps < 10000 { steps += 1; }

        assert_eq!(sched.now, 11.0);
        assert_eq!(sched.job_factory.jobs_done().len(), 4);

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
        let mut steps = 0;

        while steps < 1000 && sched.tick() { steps += 1 }

        assert_eq!(sched.job_factory.jobs_done().len(), 4);
        assert_eq!(sched.now, 10.0);
        Ok(())
    }

    #[test]
    fn workflow_factory_vanilla_small() -> Result<()> {
        let mut reg = NodeRegistry::new();
        reg.new_node("CPU", 4.0, 2.0)?;
        reg.new_node("RAM", 4.0, 8.0)?;

        let content = "\
            0;2.0;1.0;5.0;y;0.0\n\
            1;1.0;1.0;1.0;y;0.0\n
            :dependencies\n\
            :replicate 2\n\
            1;0";

        let factory = JobWorkflowFactory::from_string(content.to_string())?;

        let mut sched = Scheduler::new(reg, Box::new(factory));
        let mut steps = 0;

        while steps < 1000 {
            println!("Process tick {}", sched.now);
            steps += 1;
            if !sched.tick() { break }
        }

        assert_eq!(sched.job_factory.jobs_done().len(), 4);
        assert_eq!(sched.now, 6.0);
        Ok(())
    }

    #[test]
    fn workflow_factory_vanilla_small_with_delay() -> Result<()> {
        let mut reg = NodeRegistry::new();
        reg.new_node("CPU", 4.0, 2.0)?;
        reg.new_node("RAM", 4.0, 8.0)?;

        let content = "\
            0;2.0;1.0;5.0;y;0.0\n\
            1;1.0;1.0;1.0;y;4.0\n
            :dependencies\n\
            :replicate 2\n\
            1;0";

        let factory = JobWorkflowFactory::from_string(content.to_string())?;

        let mut sched = Scheduler::new(reg, Box::new(factory));
        let mut steps = 0;

        while steps < 1000 {
            println!("Process tick {}", sched.now);
            steps += 1;
            if !sched.tick() { break }
        }

        assert_eq!(sched.job_factory.jobs_done().len(), 4);
        assert_eq!(sched.now, 6.0+4.0);
        Ok(())
    }
}
