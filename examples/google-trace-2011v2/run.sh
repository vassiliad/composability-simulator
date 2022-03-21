#!/bin/env bash
cargo build --release

rack_size=24
memory_fraction_local=$1
memory_fraction_remote=$2
experiment_type=$3

gtrace_events_file=${GTRACE_EVENTS_FILE:-/opt/google/gtrace-2011-v2/machine_events/part-00000-of-00001.csv}

mkdir -p clusters_vanilla clusters_borrow

lbl="${rack_size}_${memory_fraction_local}-${memory_fraction_remote}"

echo "Generating experiment configuration files"
cargo run --release --bin parse_gtrace_machines --  \
  -i ${gtrace_events_file} \
  -o clusters_${experiment_type}/${rack_size}_${memory_fraction_local}-${memory_fraction_remote} \
  -r ${memory_fraction_remote} \
  -l ${memory_fraction_local}

echo "Starting experiment"
cargo run --release --bin=compsim -- \
  -d clusters_${experiment_type}/${lbl}.nodes \
  -c clusters_${experiment_type}/${lbl}.connections \
  -j 12_hours.${experiment_type} \
  -o ${lbl}.${experiment_type} 2>&1 | tee ${experiment_type}-${lbl}.log 
