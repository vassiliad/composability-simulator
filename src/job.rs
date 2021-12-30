use crate::node::Node;

pub struct Job {
    pub cores: f32,
    pub memory: f32,
    pub can_borrow: bool,
    pub time_created: f32,
    pub time_started: Option<f32>,
    pub time_done: Option<f32>,
    pub node_cores: Option<Node>,
    pub node_memory: Option<Vec<Node>>,
}

impl Job {
    pub fn new(cores: f32, memory: f32, can_borrow: bool, time_created: f32) -> Self {
        Self {
            cores,
            memory,
            can_borrow,
            time_created,
            time_started: None,
            time_done: None,
            node_cores: None,
            node_memory: None,
        }
    }
}
