#[cfg(test)]
mod test_node {
    use dismem::registry;

    #[test]
    fn test_init_node() -> Result<(), String> {
        let mut reg = registry::NodeRegistry::new();
        let _ = reg.new_node("test", 1.0, 1., 1.)?;

        Ok(())
    }

    #[test]
    fn test_fail_init_node_name() -> Result<(), String> {
        let mut reg = registry::NodeRegistry::new();
        let _n0 = reg.new_node("test", 1.0, 1., 1.)?;
        let n1 = reg.new_node("test", 1.0, 1., 1.);

        match n1 {
            Err(_) => Ok(()),
            Ok(_) => Err("Should not have created a second Node with the name test".to_owned()),
        }
    }
}
