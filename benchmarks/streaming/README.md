# Streaming benchmark harness

This harness measures the streaming paths of the `av-web-service`
HTTP/1.1+HTTP/2 and HTTP/3 listeners under load. It compares one or more
web-services source trees on one Linux machine. The measurements and their
analysis are in
[`docs/listener-streaming-benchmarks.md`](../../docs/listener-streaming-benchmarks.md).

## Contents

| Path | Content |
| --- | --- |
| `run.sh` | Builds the client and one server for each tree, runs the scenarios, and writes the results. |
| `summarize.py` | Writes `summary.md` and `summary.csv` from `runs.jsonl`. `run.sh` runs it at the end. |
| `server/` | `wsb-server`, an `H2H3Server` application with the `/stream`, `/echo` and `/` routes. `Cargo.toml.in` is a template. `run.sh` writes the tree path into it. |
| `client/` | `wsb-client`, the load generator for HTTP/1.1, HTTP/2 and HTTP/3. |
| `diag-h2-profile.sh` | Profiles one server under one closed-loop load with `/proc` counters and `perf`. |
| `diag-h2-sweep.txt` | Scenario file for the HTTP/2 streams-per-connection sweep. |
| `diag/client-head-latency.rs` | Client variant that records the time to the response head. |
| `diag/lifesrv-src/main.rs` | Server variant that records the chunk lifetime. |
| `results/<round>/` | `summary.md` and `summary.csv` of each recorded round. |

The client and the server are separate Cargo workspaces. They are not members
of the repository workspace.

## Requirements

- Linux with `/proc`, `taskset` and `sha256sum`.
- 64 cores for the default core split (server 0-23, client 24-63).
- Rust and Cargo in `~/.cargo/bin`, and Python 3.
- A hard limit of at least 1,048,576 open files.
- `perf` and password-free `sudo` for `diag-h2-profile.sh`.

## Run

Run the full matrix for one or more trees:

```sh
benchmarks/streaming/run.sh <web-services-tree> <label> [<tree> <label> ...]
```

Example with the three trees of the last round:

```sh
benchmarks/streaming/run.sh ~/src-main main \
  ~/src-listener-nodelay listener-nodelay \
  ~/src-inline-streaming-nodelay inline-streaming-nodelay
```

Each tree must contain `web-service/` and `Cargo.lock`. The full matrix is 56
scenarios. Each run takes about 20 s. Three trees with three repeats take
about 2.8 hours.

## Settings

| Variable | Default | Purpose |
| --- | --- | --- |
| `REPEATS` | `3` | Runs of each scenario for each tree. |
| `ONLY` | `.` | Extended regular expression. Only the scenarios with a matching name run. |
| `SCENARIO_FILE` | none | File with scenario lines. The lines replace the built-in table. |
| `SERVER_CPUS` | `0-23` | Server cores for `taskset`. |
| `CLIENT_CPUS` | `24-63` | Client cores for `taskset`. |
| `WARMUP`, `MEASURE` | `5`, `10` | Closed-loop warm-up and measurement time in seconds. |
| `STALL_WARMUP`, `STALL` | `3`, `5` | Slow-reader warm-up and stall time in seconds. |
| `IDLE_MAX_PCT` | `1.0` | Maximum machine CPU use, in percent of all cores, before a run starts. |
| `RESULTS_NAME` | `<UTC time>-<labels>` | Name of the results directory. |
| `WSB_ROOT` | `~/ws-bench` | Work directory for the Cargo cache, the builds and the results. |

Example that runs only the short HTTP/2 scenarios with one repeat:

```sh
REPEATS=1 ONLY='^short-h2' benchmarks/streaming/run.sh ~/src-main main
```

## Results

`run.sh` writes the results to `$WSB_ROOT/results/<RESULTS_NAME>/`:

- `runs.jsonl`: one JSON record for each run.
- `summary.md` and `summary.csv`: the median and the range of each metric.
- `environment.txt`: the kernel, the toolchain, the CPU and the network
  settings.
- `scenarios.txt`: the scenario table of the round.
- `commands.log`: the command line of each server and client process.
- `builds/`: the commit, the binary checksums and the lock file of each tree.
- `logs/`: the server and client output of each run.

To write the summary again:

```sh
python3 benchmarks/streaming/summarize.py "$WSB_ROOT/results/<RESULTS_NAME>"
```

## Profile

`diag-h2-profile.sh` uses the client and the server binaries that `run.sh`
built:

```sh
benchmarks/streaming/diag-h2-profile.sh main \
  "$WSB_ROOT/build/main/target/release/wsb-server" /path/to/out \
  --proto h2 --mode closed --conc 10000 --per-conn 64 \
  --path "/stream?chunks=4&size=16384" --expect-bytes 65536
```

The script writes per-thread CPU, write and TCP segment counters, a CPU
profile, and context-switch call stacks to the output directory.
