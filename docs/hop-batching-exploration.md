# Hop batching exploration: notes for future sessions

Status as of 2026-10-12. Branch `claude/zen-albattani-71q1go`,
[nmarasoiu/dragonfly#1](https://github.com/nmarasoiu/dragonfly/pull/1).
Verified facts are marked **(verified)**: read in code or measured. Estimates and hypotheses are
marked **(estimate)** or **(hypothesis)**.

## 1. Big picture

- Every Dragonfly thread does two jobs:
  - it runs an event loop (io_uring) for the client connections it accepted;
  - it owns one shard of the keyspace.
- A command whose keys live on other shards is coordinated by the connection's thread. That thread
  sends a small closure (a "hop") to each owning shard's inbox and waits on a barrier. Data never
  moves between threads; only closures, plus results written into the coordinator's memory.
  **(verified)**
- The topology is a full mesh. Each shard has one bounded many-writer/one-reader inbox, a Vyukov
  ring of **64 slots** with 48-byte inline closures (`TaskQueue`/`FiberQueue`, `kQueueLen = 64` in
  `src/server/engine_shard.cc`). Any thread pushes straight into it, with no relay. **(verified)**
- Parsing and reply encoding happen on the **source** (connection) thread. The shard thread only
  runs the shard callback: lookup, copying the value, transaction-queue logic. **(verified)**
- The shard-inbox consumer fiber runs at **HIGH** priority (`src/core/task_queue.cc`). So on a hot
  shard, work arriving from all threads runs ahead of that thread's own clients' parsing and I/O.
  **(verified)**

## 2. The signalling path in detail (verified in helio)

### A normal hop
1. **Producer enqueues:** a CAS on the ring's enqueue index, then it constructs the closure in the
   slot.
2. **Producer notifies:** `pull_ec_.notify()`, an atomic operation on the consumer's EventCount.
   If the consumer fiber is parked, a spinlock is taken and the fiber is pushed onto the
   destination thread's remote-ready queue. If that thread is asleep in the kernel, it is woken by
   an io_uring `msg_ring` or an eventfd write (a syscall).
3. **Consumer drains:** it takes up to 16 tasks per pass and runs them back to back.
4. **Completion:** the shard callback decrements the coordinator's barrier. The last one wakes the
   coordinator fiber through the same remote-ready mechanism: an MPSC push, plus a wake syscall
   only if the coordinator thread is asleep.

### When the 64-slot inbox is full: how a blocked producer is woken
- `FiberQueue::Add` loops:
  1. `TryAdd` fails;
  2. `push_ec_.prepareWait()`;
  3. retry `TryAdd`;
  4. `blocked_submitters_++`;
  5. `push_ec_.wait(epoch)`: the fiber parks. Other fibers on that thread keep running.
- After every drain pass (up to 16 tasks) the consumer calls `push_ec_.notifyAll()`. That wakes
  **all** blocked producers on every thread, each through a remote-ready push and possibly a wake
  syscall. They all retry `TryAdd`, and those that lose the race park again.
- So under saturation this is a **thundering herd**: about every 16 tasks, every blocked producer is
  woken. That's a plausible hidden cost on a hot shard with many source threads. **(hypothesis:
  not measured)**
- In our batcher, a full destination hands the batch to one sender fiber per (thread, destination).
  That limits each thread to one blocked producer per destination, which reduces the herd.

### Cost model per cross-thread hop (estimate)
| Part | Cost |
|---|---|
| Cache-line transfers between cores (enqueue index, slot, EventCount, barrier, remote-ready queue, result) | about 4–8 transfers × roughly 50–100 ns |
| Wake syscalls, only when the other side sleeps | 0–2 × about 1–5 µs |
| Total for a busy system | about 0.3–0.8 µs per hop |

Compare that with what a GET costs end to end. In our test, the 2 server threads at 190–210K QPS
spend about **10 µs of server CPU per request**, mostly socket I/O, parsing, reply writing and the
io_uring syscalls. Signalling is therefore roughly 5–10% of the per-request cost. That is the main
reason batching alone has little to win here. Your intuition that "64 ops should be a dozen ns" holds
for thread-local operations; cross-core signalling is 10–100× that. But the network path, not the
signalling, dominates a GET. **(estimate: needs a profiler to confirm)**

## 3. What was built (PR #1, behind `--experimental_hop_batching`, default off)

- **`HopBatcher`** (`src/server/hop_batcher.{h,cc}`), one per thread:
  - per-destination staging lists;
  - a NORMAL-priority **flusher fiber**, woken by the first staged hop. It sits at the tail of the
    ready queue, so it flushes after every fiber that was ready in that round. One round = one
    batch, with no timer;
  - up to 32 hops per inbox entry;
  - on a full inbox, a per-destination sender fiber blocks; hops to other shards are unaffected;
  - per-destination order is preserved (each shard inbox has a single consumer).
- **Call sites:** only the main transaction hops in `src/server/transaction.cc`
  (`ScheduleInternal`, `DispatchHop`, `SingleHopAsync`) go through `DispatchToShard`.
- **Lifecycle:** started and stopped in `Service::Init` / `Service::Shutdown`.
- **Stats:** INFO fields `tx_hop_batches_total` and `tx_hop_batched_tasks_total`.
- **Why a fiber and not a hook:**
  - helio has no end-of-ready-round callback;
  - on-idle tasks don't run under load;
  - a send to a full inbox must be able to block.

  The better design is a helio hook right after `scheduler->Run(NORMAL)` in
  `UringProactor::MainLoop` (`helio/util/fibers/uring_proactor.cc`, around line 897), and the same
  in the epoll loop. That needs an upstream helio change. **Not done.**

## 4. Results so far

All runs: local 4-vCPU box. Server on 2 threads (CPUs 0–1), `dfly_bench` on 2 threads (CPUs 2–3).
Release build, 3 runs × 15 s per configuration.

| Workload | Off → on (QPS) | p99 | Hops per batch |
|---|---|---|---|
| Zipf α 0.99 over 100K keys (top key 7.8%, top 10 23%), 50 connections | 190K → 194K | ≈ | 4.4 |
| Uniform keys, 50 connections | 201K → 195K | ≈ | 4.4 |
| Pipelined (8) | ≈ | ≈ | 1.03 (pipeline squashing already batches) |
| Light load, 2 connections | ≈ | 154 → 139 µs | 1.0 |
| **Single hot key**, GET only, 50 connections | 198K → 187K | 571 → 607 µs | 8.8 |
| **Single hot key**, GET only, 100 connections | 213K → 191K | 1034 → 1154 µs | 12.0 |

- In the single-hot-key runs, the hot thread's CPU went from about 91% to 100%. Requests from clients
  on the hot thread itself rose from 57% to 60% of the total.
- Run-to-run spread is about ±8%. The 50-connection regression held in 3/3 runs; the 100-connection
  case has one outlier.
- Unit tests pass with batching on: `hop_batcher_test`, `multi_test`, `string_family_test`.

## 5. Things that don't make sense yet (open RCA)

It isn't yet clear why fewer inbox operations would **lower throughput**, rather than only raise
latency. Candidate causes, none isolated yet:

1. **Mix shift with closed-loop clients (hypothesis, best evidence).**
   - Hops from the cold thread wait for the flusher, so those clients send less.
   - The hot thread's own clients take a larger share, and each costs the hot thread several times
     more: parse + I/O + execution, against just the shard callback for a remote hop.
   - The inline share rising from 57% to 60% supports this.
2. **Lumpy execution on the hot thread (hypothesis).** The HIGH-priority consumer runs batches of
   9–12 back to back. That delays the hot thread's own socket processing, so there is less
   overlap.
3. **Allocation cost (hypothesis, probably small).** One allocation per batch on the source, freed
   on the hot thread.
4. **Flusher fiber switch (hypothesis).** On the cold thread, which has spare CPU, so it should cost
   latency rather than throughput.
5. **Noise or measurement artefacts:**
   - only 2 server threads;
   - client and server share the box;
   - CPU sampled from a single 3-second `top` snapshot;
   - **no profiler** (`perf` is unavailable in the sandbox).

### Experiments to isolate the causes (not yet run)
- **E1 – client placement.** `--conn_io_threads=1 --conn_io_thread_start=<cold>`, so every request
  is a remote hop, compared with start = hot, so every request is inline. Find the cold thread by
  checking that `tx_inline_runs` is about 0%. If batching is neutral or positive with all clients on
  the cold thread, hypothesis 1 is confirmed.
- **E2 – adaptive flush.** At most one batch in flight per (source, destination). If nothing is in
  flight, send immediately; otherwise accumulate until the previous batch completes, like Nagle's
  algorithm with an ack.
- **E3 – helio hook prototype** replacing the flusher fiber.
- **E4 – reuse batch buffers** (about 1.5 KB each, negligible cache footprint).
- **E5 – a profiler on a real host**, plus many cores: 16–64 server threads with a separate client
  machine.

### Levers ranked by expected impact (what we can change)
1. Adaptive one-in-flight flush (E2).
2. Moving client connections off the hot thread: test via config now, adaptive migration later. The
   migration machinery already exists in `Connection::Migrate`.
3. The helio end-of-round hook (E3).
4. Merging duplicate GETs within a batch. The big upside for hot keys: 12 identical GETs → 1 lookup.
5. Fairness for the hot thread's consumer: yield between batches, or give I/O a CPU budget.
6. Buffer reuse (E4).
7. Tuning the maximum batch size.

## 6. Why Seastar batches and it pays off there, but not (yet) here

These are educated guesses. **Not verified against current Seastar code.**
- **Polling, not sleeping.** Seastar reactors poll, so cross-core messages never pay wake syscalls.
  Dragonfly parks idle threads, and its wake path (`msg_ring`/eventfd) is exactly what batching
  amortises best. It mostly matters at moderate load, not saturation.
- **One single-producer queue per core pair.** Seastar uses N×N SPSC queues, so producers never
  contend. Dragonfly has one shared inbox per shard: contention and herd effects grow with core
  count, and so does batching's value. Our 2-thread test can't show this.
- **Heavier operations and many more cores.** Seastar's flagship workloads (ScyllaDB) run on 64+
  cores with more cross-core traffic per request, so signalling is a larger fraction of the cost.
- **Its network stack** can bypass the kernel (DPDK), making the per-request I/O cost much smaller,
  so signalling is relatively bigger.

## 7. Known unknowns / what we're missing

- A **CPU profile** of the hot thread, with batching off and on.
- **Many-core behaviour:** 64 sources sharing a 64-slot inbox means about 1 slot each. Batching
  should matter much more there, and so should fairness (one batch in flight per pair).
- **How expensive the thundering herd on a full inbox is** (`push_ec_.notifyAll` every ≤16 tasks).
- **Open-loop load** (a fixed request rate instead of closed-loop clients). It would separate
  latency effects from throughput effects. `dfly_bench --qps=N` can do this.
- **Read/write mixes and multi-key commands** on a hot shard.
- Whether **pipeline squashing** already captures most of the batching benefit for real clients,
  which often pipeline.

## 8. How to build and run in this cloud sandbox (environment notes)

- The session's GitHub proxy blocks archive downloads (`github.com/*/archive/*`,
  `codeload.github.com`). Release assets and anonymous `git clone`/`git fetch` work.
- The user-approved workaround is a local-only mirror:
  1. fetch each blocked dependency at its **pinned** tag or commit with `git fetch --depth 1`;
  2. `git archive` it into a scratchpad tarball, named after the sanitised URL;
  3. temporarily add a hook in `helio/cmake/download_retry.cmake` that serves those tarballs when
     `DFLY_MIRROR_DIR` is set.

  **Never commit the helio change; revert it after the build.**
- System packages needed: `libboost-context-dev libunwind-dev libssl-dev autoconf-archive libtool
  bison zlib1g-dev`.
- Minimal configuration: `./helio/blaze.sh [-release] -DWITH_AWS=OFF -DWITH_GCP=OFF -DWITH_GPERF=OFF
  -DWITH_SEARCH=OFF -DWITH_TIERING=OFF`.
- `transaction_test` does not link the server, so `--experimental_hop_batching` is an unknown flag
  there. That's expected.
- `dfly_bench` needs `--qps=0` for unthrottled load; the default is 20 requests per second per
  connection.
