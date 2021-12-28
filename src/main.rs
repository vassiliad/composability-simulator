mod node;
mod registry;
mod resource;

fn main() -> Result<(), String> {
    let mut reg = registry::NodeRegistry::new();

    let n = reg.new_node("test", 1.0, 1., 1.)?;
    println!("{:?}", n);
    Ok(())
}
