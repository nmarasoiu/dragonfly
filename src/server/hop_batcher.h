// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <vector>

#include "base/function2.hpp"
#include "server/tx_base.h"
#include "util/fibers/fibers.h"
#include "util/fibers/synchronization.h"

namespace dfly {

// Thread-local batcher for transaction hops (enabled with --experimental_hop_batching).
//
// Hops dispatched from the same coordinator thread to the same shard during one scheduler round
// are coalesced into a single shard queue task. This reduces shard queue slot usage and wake-ups
// of the destination thread under load. There is no timer: the flusher fiber is woken by the first
// staged hop and runs after the fibers that were already ready to run, so under light load batches
// have size one and only the cost of a fiber switch is added.
//
// If a destination queue is full, its batch is handed to a dedicated sender fiber that blocks on
// that queue, so a congested shard does not delay hops destined to other shards. While the sender
// is blocked, new hops for that shard keep accumulating and are sent as the next batch.
class HopBatcher {
 public:
  // Same layout as the shard queue tasklet: callbacks up to 32 bytes are stored inline.
  using Task = fu2::function_base<true /* owns */, false /* non-copyable */,
                                  fu2::capacity_fixed<32, 8>, false /* non-throwing */,
                                  false /* strong exceptions guarantees */, void()>;

  // Creates the thread-local batcher and starts its flusher fiber.
  static void StartThreadLocal(unsigned num_shards);

  // Sends all staged hops, stops the fibers and destroys the thread-local batcher.
  static void StopThreadLocal();

  // Returns the thread-local batcher, or nullptr if batching is disabled on this thread.
  static HopBatcher* tlocal() {
    return tl_batcher_;
  }

  // Schedules `f` to run in shard `sid` queue. It's sent with the next flush.
  void Stage(ShardId sid, Task f);

 private:
  struct Batch {
    std::vector<Task> tasks;

    void operator()() {
      for (auto& task : tasks)
        task();
    }
  };

  struct Dest {
    std::vector<Task> staged;
    bool sender_active = false;  // a sender fiber owns sending to this destination.
  };

  explicit HopBatcher(unsigned num_shards);

  void FlusherLoop();
  void FlushAll();

  // Moves up to kMaxBatchSize staged tasks of `dest` into a batch.
  Batch TakeBatch(Dest* dest);

  // Blocks on the destination queue for `batch` and keeps draining `sid` until nothing is staged.
  void SenderLoop(ShardId sid, Batch batch);

  static void RecordBatch(size_t size);

  std::vector<Dest> dests_;
  std::vector<ShardId> dirty_;  // destinations with staged tasks and no active sender.

  util::fb2::EventCount flush_ec_;
  util::fb2::EventCount senders_ec_;
  unsigned active_senders_ = 0;
  bool stopping_ = false;
  util::fb2::Fiber flusher_;

  static __thread HopBatcher* tl_batcher_;
};

}  // namespace dfly
