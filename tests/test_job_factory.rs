use anyhow::Result;

use compsim::job_factory::JobFactory;
use compsim::job_factory::JobStreaming;
use compsim::job_factory::JobWorkflowFactory;

#[cfg(test)]
mod test_job_factory {
    use super::*;

    #[test]
    fn job_factory_streaming() -> Result<()> {
        let content = "0;0.0;0.0;0.0;y;0.0\n\
        # this is a comment above an empty line\n\
        \n\
        1;1.0;1.0;1.0;y;1.0\n
        # 2;1.0;1.0;1.0;y;1.0\n
        # the line above is a comment";

        println!("{}", content);

        let mut factory = JobStreaming::from_string(content.to_string())?;

        for idx in 0.. {
            println!("Is there a job {} {}", idx, factory.more_jobs());

            if factory.more_jobs() == false {
                break;
            }

            let job = factory.job_get();

            assert_eq!(job.memory, 1.0 * idx as f32);
            assert_eq!(job.cores, 1.0 * idx as f32);
            assert_eq!(job.time_created, 1.0 * idx as f32);
            assert_eq!(job.duration, 1.0 * idx as f32);

            factory.job_mark_done(&job);
        }

        assert_eq!(factory.jobs_done().len(), 2);

        Ok(())
    }

    #[test]
    fn job_factory_workflow() -> Result<()> {
        let content = "0;0.0;0.0;0.0;y;0.0\n\
        # this is a comment above an empty line\n\
        \n\
        1;1.0;1.0;1.0;y;1.0\n
        # 2;1.0;1.0;1.0;y;1.0\n
        # the line above is a comment\n\
        :dependencies\n\
        :replicate 1\n\
        1;0";

        println!("{}", content);

        let factory = JobWorkflowFactory::from_string(content.to_string())?;

        assert_eq!(factory.jobs_dependencies.get(&0), None);
        assert_eq!(factory.jobs_dependencies.get(&1), Some(&vec![0]));

        println!("Next up: {:?}", factory.job_peek());

        assert_eq!(factory.job_peek().is_none(), false);

        // assert!(false);

        Ok(())
    }
}
