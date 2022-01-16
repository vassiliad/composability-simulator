// use dismem::job::reset_job_metadata;
use anyhow::Result;

use dismem::job_factory::JobStreaming;

#[cfg(test)]
mod test_job_factory {
    use dismem::job_factory::JobFactory;

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
}
