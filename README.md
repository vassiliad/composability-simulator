# Simulator for Scheduling jobs on Cluster with hardware-composability features

This is a work-in-progress application for simulating the scheduling of Jobs on a cluster that features hardware-composability such as memory borrowing.

## Compiling

To compile, run:

```bash
cargo build --release
```

## Testing

```bash
cargo test
```

## Examples

### Simulate memory borrowing

To execute, run:

```bash
cargo run  --release --bin=dismem examples/dismem_racks/nodes.csv examples/dismem_racks/connections.csv examples/dismem_racks/tiny.jobs
```