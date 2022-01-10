use dismem::node;
use dismem::registry;

#[cfg(test)]
mod test_node {
    use super::*;

    #[test]
    fn init_node() -> Result<(), String> {
        let mut reg = registry::NodeRegistry::new();
        let _ = reg.new_node("test", 1.0, 1. /*, 1.*/)?;

        Ok(())
    }

    #[test]
    fn fail_init_node_name() -> Result<(), String> {
        let mut reg = registry::NodeRegistry::new();
        let _n0 = reg.new_node("test", 1.0, 1. /*, 1.*/)?;
        let n1 = reg.new_node("test", 1.0, 1. /*, 1.*/);

        match n1 {
            Err(_) => Ok(()),
            Ok(_) => Err("Should not have created a second Node with the name test".to_owned()),
        }
    }

    #[test]
    fn insort_nodes() -> Result<(), String> {
        let mut reg = registry::NodeRegistry::new();
        let _ = reg.new_node("more_memory", 1., 2. /*, 1.*/)?;
        let _ = reg.new_node("more_cores", 2., 1. /*, 1.*/);

        let cores: Vec<&node::Node> = reg.nodes_sorted_cores(-1.).collect();
        let memory: Vec<&node::Node> = reg.nodes_sorted_memory(-1.).collect();

        assert_eq!(cores[0].name, "more_memory");
        assert_eq!(cores[1].name, "more_cores");

        assert_eq!(memory[0].name, "more_cores");
        assert_eq!(memory[1].name, "more_memory");

        Ok(())
    }

    #[test]
    fn filter_nodes() -> Result<(), String> {
        let mut reg = registry::NodeRegistry::new();
        let _ = reg.new_node("more_memory", 1., 2. /*, 1.*/)?;
        let _ = reg.new_node("more_cores", 2., 1. /*, 1.*/);

        let cores: Vec<&node::Node> = reg.nodes_sorted_cores(1.01).collect();
        let memory: Vec<&node::Node> = reg.nodes_sorted_memory(1.01).collect();

        assert_eq!(cores[0].name, "more_cores");
        assert_eq!(memory[0].name, "more_memory");

        assert_eq!(cores.len(), 1);
        assert_eq!(memory.len(), 1);

        Ok(())
    }

    #[test]
    fn resort_nodes() -> Result<(), String> {
        let mut reg = registry::NodeRegistry::new();
        let _ = reg.new_node("more_memory", 1., 2. /*, 1.*/)?;
        let _ = reg.new_node("more_cores", 2., 1. /*, 1.*/);
        let _ = reg.new_node("uber", 1000., 1000. /*, 1.*/);

        let cores: Vec<&node::Node> = reg.nodes_sorted_cores(1.01).collect();
        let memory: Vec<&node::Node> = reg.nodes_sorted_memory(1.01).collect();

        assert_eq!(cores[0].name, "more_cores");
        assert_eq!(memory[0].name, "more_memory");

        assert_eq!(cores.len(), 2);
        assert_eq!(memory.len(), 2);

        // VV: Real test starts here, we essentially make the `more_memory` node
        // have more cores, and the `more_cores` one have more memory

        let nodes = reg.nodes_mut();

        nodes[0].cores.capacity = 10.;
        nodes[0].cores.current = 10.;

        nodes[1].memory.capacity = 10.;
        nodes[1].memory.current = 10.;

        let nodes = reg.nodes_immut();
        reg.resort_nodes_cores();
        reg.resort_nodes_memory();

        let cores: Vec<&node::Node> = reg.nodes_sorted_cores(-1.).collect();
        let memory: Vec<&node::Node> = reg.nodes_sorted_memory(0.).collect();

        for node in &cores {
            print!("After sort {}\n", node);
        }

        assert_eq!(cores[0].name, "more_cores");
        assert_eq!(cores[1].name, "more_memory");

        assert_eq!(memory[0].name, "more_memory");
        assert_eq!(memory[1].name, "more_cores");

        Ok(())
    }
}
