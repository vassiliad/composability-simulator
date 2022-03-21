use anyhow::Result;

use compsim::job::Job;

#[cfg(test)]
mod test_job {
    use super::*;

    #[test]
    fn job_parsing() -> Result<()> {
        let text = "0;0.0;0.0;0.0;y;0.0\n\
            # this is a comment above an empty line\n\
            \n\
            0;0.0;0.0;0.0;y;0.0;1;2;0.125\n\
            # 2;1.0;1.0;1.0;y;1.0\n\
            # the line above is a comment\n\
            # the line below is a full job\n\
            0;0.0;0.0;0.0;y;0.0;1;2;0.5;3;0.250";

        println!("{}", text);

        let jobs: Vec<Job> = text.lines().filter_map(|x| {
            let x = x.trim();
            if x.starts_with('#') || x.is_empty() {
                None
            } else {
                Some(x.parse().unwrap())
            }
        }).collect();

        assert_eq!(jobs.len(), 3);

        let j = &jobs[0];

        assert!(j.node_cores.is_none());
        assert!(j.node_memory.is_empty());

        let j = &jobs[1];

        assert_eq!(j.node_cores, Some(1));
        assert_eq!(j.node_memory, [(2, 0.125)]);

        let j = &jobs[2];

        assert_eq!(j.node_cores, Some(1));
        assert_eq!(j.node_memory, [(2, 0.5), (3, 0.250)]);


        Ok(())
    }
}
