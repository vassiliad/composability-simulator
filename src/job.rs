use std::sync::atomic::{AtomicUsize, Ordering};

static NEXT_JOB_UID: AtomicUsize = AtomicUsize::new(0);

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
    pub fn new(
        cores: f32,
        memory: f32,
        duration: f32,
        can_borrow: bool,
        time_created: f32,
    ) -> Self {
        let uid = NEXT_JOB_UID.fetch_add(1, Ordering::SeqCst);

        let job = Self {
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
        };

        job
    }
}
