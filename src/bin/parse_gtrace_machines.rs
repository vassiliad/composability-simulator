/*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
*/

// The google dataset 2011-v2 can be found here: https://github.com/google/cluster-data/blob/master/ClusterData2011_2.md
// it is licensed under CC-BY license: https://creativecommons.org/licenses/by/4.0/

use std::collections::HashSet;
use std::fs::create_dir_all;
use std::fs::File;
use std::io::BufRead;
use std::io::BufReader;
use std::io::BufWriter;
use std::io::Write;
use std::path::Path;
use std::path::PathBuf;
use std::str::FromStr;

use anyhow::bail;
use anyhow::Context;
use anyhow::Result;
use clap::App;
use clap::Arg;

#[derive(Debug)]
struct Node {
    uid: usize,
    cores: f32,
    memory: f32,
}

impl FromStr for Node {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self> {
        /* VV: The file format of machine events CSV files is
        0. time,INTEGER,YES
        1. machine ID,INTEGER,YES
        2. event type,INTEGER,YES
        3. platform ID,STRING_HASH,NO
        4. CPUs,FLOAT,NO      <--+ Use just these 2
        5. Memory,FLOAT,NO    <--|
        */
        let toks = s.split(',').collect::<Vec<_>>();
        if toks.len() != 6 {
            bail!("Expected a string with 6 comma separated tokens, \
                        but got {:?} instead.", toks);
        }

        let uid: usize = toks[1].parse()
            .context(format!("Unable to parse uid \"{}\" into a usize", toks[1]))?;
        let cores: f32 = toks[4].parse()
            .context(format!("Unable to parse cores \"{}\" into a f32", toks[4]))?;
        let memory: f32 = toks[5].parse()
            .context(format!("Unable to parse memory \"{}\" into a f32", toks[5]))?;

        Ok(Node { cores, memory, uid })
    }
}


#[derive(Debug)]
struct Arguments {
    input_file: PathBuf,
    output_file_prefix: PathBuf,
    fraction_memory_local: f32,
    fraction_memory_remote: f32,
    pool_population: usize,
}

fn parse_arguments() -> Result<Arguments> {
    let app = App::new("Parse google trace machine events")
        .version("0.1.0")
        .author("Vassilis Vassiliadis")
        .about("Generates a nodes and connections file for dismem simulator")
        .arg(Arg::new("input_file")
            .short('i')
            .long("input_file")
            .takes_value(true)
            .required(true)
            .help("Path to the google trace .csv file to process"))
        .arg(Arg::new("outputFilePrefix")
            .short('o')
            .long("outputFilePrefix")
            .takes_value(true)
            .required(true)
            .help("Prefix to the <prefix>.nodes and <prefix>.connections paths that \
                the  script will generate"))
        .arg(Arg::new("fractionMemoryLocal")
            .short('l')
            .long("fractionMemoryLocal")
            .help("Fraction of memory that the original node defines which \
                the resulting node should use as its local memory. Setting this \
                to 1.0 and fractionRemoteMemory to 0.0 will effectively generate \
                a node which is exactly as the one defined in the original trace.")
            .default_value("1.0"))
        .arg(Arg::new("fractionMemoryRemote")
            .short('r')
            .long("fractionRemoteMemory")
            .help("Fraction of memory that the original node defines which \
                the resulting node can only use by borrowing it from the shared pool in the \
                rack. Setting this to 1.0 and fractionLocalMemory to 0.0 will produce \
                a node with 0 local memory that must always borrow memory from the pool.")
            .default_value("0.0"))
        .arg(Arg::new("poolPopulation")
            .short('p')
            .long("poolPopulation")
            .help("Maximum number of nodes that can borrow from a pool. The nodes are first \
            sorted based on their core capacity. Then @poolPopulation of them are grouped \
            together. This simulates nodes in the same rack being able to borrow memory from \
            the same pool.")
            .default_value("24"));

    let args = app.get_matches();
    let input_file = args.value_of("input_file").unwrap();
    let output_file_prefix = args.value_of("outputFilePrefix").unwrap();
    let input_file = PathBuf::from(input_file);
    let output_file_prefix = PathBuf::from(output_file_prefix);

    let fraction_memory_local = args.value_of("fractionMemoryLocal").unwrap();
    let fraction_memory_local: f32 = fraction_memory_local.parse()
        .context(format!("fractionMemoryLocal \"{}\" is not a valid f32",
                         fraction_memory_local))?;
    let fraction_memory_remote = args.value_of("fractionMemoryRemote").unwrap();
    let fraction_memory_remote: f32 = fraction_memory_remote.parse()
        .context(format!("fractionMemoryRemote \"{}\" is not a valid f32",
                         fraction_memory_remote))?;
    let pool_population = args.value_of("poolPopulation").unwrap();
    let pool_population: usize = pool_population.parse()
        .context(format!("poolPopulation \"{}\" is not a valid usize", pool_population))?;

    Ok(
        Arguments {
            input_file,
            output_file_prefix,
            fraction_memory_local,
            fraction_memory_remote,
            pool_population,
        }
    )
}

fn parse_unique_nodes(input_file: &Path) -> Result<Vec<Node>> {
    let file = File::open(input_file)
        .context(format!("Unable to open input file {}", input_file.display()))?;
    let br = BufReader::new(file);

    let mut node_uids: HashSet<usize> = HashSet::new();
    let mut nodes: Vec<Node> = vec![];

    for line in br.lines() {
        let line = line?;
        match line.parse::<Node>() {
            Ok(node) => {
                if !node_uids.contains(&node.uid) {
                    let pred = |n: &Node| -> bool { n.cores < node.cores };
                    let idx = nodes.partition_point(pred);
                    node_uids.insert(node.uid);

                    nodes.insert(idx, node);
                }
            }
            Err(e) => { println!("Skipping line {} because {}", line, e); }
        }
    }

    Ok(nodes)
}

fn main() -> Result<()> {
    let arguments = parse_arguments()?;

    if let Some(parent) = arguments.output_file_prefix.parent() {
        if !parent.exists() {
            create_dir_all(parent)
                .context(format!("Unable to create directory for output files {}",
                                 parent.display()))?;
        }
    }
    let out_prefix = arguments.output_file_prefix.to_str().unwrap();
    let out_nodes = format!("{}.nodes", out_prefix);
    let out_nodes = File::create(&out_nodes)
        .context(format!("Unable to create output Nodes file at {}", out_nodes))?;
    let mut out_nodes = BufWriter::new(out_nodes);

    writeln!(&mut out_nodes, "#name:str;cores:f32;memory:f32")?;

    let out_connections = format!("{}.connections", out_prefix);
    let out_connections = File::create(&out_connections)
        .context(format!("Unable to create output Connections file at {}", out_connections))?;
    let mut out_connections = BufWriter::new(out_connections);

    writeln!(&mut out_connections, "#node_borrower:str[;lender1:str;...lender_n:str]*")?;

    let nodes = parse_unique_nodes(&arguments.input_file)?;

    // println!("{:?}", nodes);
    println!("Discovered {} unique nodes", nodes.len());

    let mut start = 0;
    let mut end = arguments.pool_population;
    let mut rack_id: usize = 0;

    loop {
        end = end.min(nodes.len());
        let terminate = end == nodes.len();

        let total_memory = nodes.iter().skip(start).take(end - start)
            .fold(0., |agg, node| agg + node.memory);
        let shared_memory = total_memory * arguments.fraction_memory_remote;

        writeln!(&mut out_nodes, "# Rack: {}", rack_id)?;
        writeln!(&mut out_connections, "# Rack: {}", rack_id)?;

        let pool_name = format!("Pool_{}", rack_id);
        writeln!(&mut out_nodes, "{};0;{}", pool_name, shared_memory)?;

        for (i, node) in nodes[start..end].iter().enumerate() {
            let memory = node.memory * arguments.fraction_memory_local;
            let name = format!("Worker_{}_{}", rack_id, i);

            writeln!(&mut out_nodes, "{};{};{}", name, node.cores, memory)?;
            writeln!(&mut out_connections, "{};{}", name, pool_name)?;
        }

        writeln!(&mut out_nodes)?;
        writeln!(&mut out_connections)?;

        if terminate {
            break;
        }
        start = end;
        end += arguments.pool_population;
        rack_id += 1;
    }

    Ok(())
}