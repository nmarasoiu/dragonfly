// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/hop_batcher.h"

#include "base/logging.h"
#include "server/engine_shard_set.h"
#include "server/server_state.h"

using namespace std;
using namespace util::fb2;

namespace dfly {

namespace {

// Bounds the time a shard queue consumer spends on a single task.
constexpr size_t kMaxBatchSize = 32;

}  // namespace

__thread HopBatcher* HopBatcher::tl_batcher_ = nullptr;

HopBatcher::HopBatcher(unsigned num_shards) : dests_(num_shards) {
  dirty_.reserve(num_shards);
}

void HopBatcher::StartThreadLocal(unsigned num_shards) {
  CHECK(tl_batcher_ == nullptr);
  tl_batcher_ = new HopBatcher(num_shards);
  tl_batcher_->flusher_ =
      Fiber(Fiber::Opts{.priority = FiberPriority::NORMAL, .name = "hop_flusher"},
            [batcher = tl_batcher_] { batcher->FlusherLoop(); });
}

void HopBatcher::StopThreadLocal() {
  HopBatcher* batcher = tl_batcher_;
  if (batcher == nullptr)
    return;

  // From now on Stage() dispatches directly; the flusher sends what is already staged and exits.
  batcher->stopping_ = true;
  batcher->flush_ec_.notify();
  batcher->flusher_.JoinIfNeeded();
  batcher->senders_ec_.await([batcher] { return batcher->active_senders_ == 0; });

  for (const auto& dest : batcher->dests_)
    DCHECK(dest.staged.empty());

  tl_batcher_ = nullptr;
  delete batcher;
}

void HopBatcher::Stage(ShardId sid, Task f) {
  DCHECK_LT(sid, dests_.size());

  if (stopping_) {
    shard_set->Add(sid, std::move(f));
    return;
  }

  Dest& dest = dests_[sid];
  bool was_empty = dest.staged.empty();
  dest.staged.push_back(std::move(f));

  // An active sender drains the destination by itself.
  if (!was_empty || dest.sender_active)
    return;

  if (dirty_.empty())
    flush_ec_.notify();
  dirty_.push_back(sid);
}

void HopBatcher::FlusherLoop() {
  while (true) {
    flush_ec_.await([this] { return !dirty_.empty() || stopping_; });
    if (dirty_.empty())  // stopping and nothing left to send
      break;
    FlushAll();
  }
}

void HopBatcher::FlushAll() {
  // FlushAll does not preempt, so no new destinations are added while iterating.
  for (ShardId sid : dirty_) {
    Dest& dest = dests_[sid];
    DCHECK(!dest.sender_active);

    while (!dest.staged.empty()) {
      Batch batch = TakeBatch(&dest);
      size_t size = batch.tasks.size();

      // TryAdd does not move from `batch` on failure.
      if (!shard_set->TryAdd(sid, std::move(batch))) {
        // The queue is full - continue sending to this destination from a fiber that may block.
        dest.sender_active = true;
        ++active_senders_;
        Fiber("hop_sender", [this, sid, batch = std::move(batch)]() mutable {
          SenderLoop(sid, std::move(batch));
        }).Detach();
        break;
      }
      RecordBatch(size);
    }
  }
  dirty_.clear();
}

HopBatcher::Batch HopBatcher::TakeBatch(Dest* dest) {
  Batch batch;
  if (dest->staged.size() <= kMaxBatchSize) {
    batch.tasks.swap(dest->staged);
    return batch;
  }

  auto first = dest->staged.begin();
  auto last = first + kMaxBatchSize;
  batch.tasks.assign(make_move_iterator(first), make_move_iterator(last));
  dest->staged.erase(first, last);
  return batch;
}

void HopBatcher::SenderLoop(ShardId sid, Batch batch) {
  Dest& dest = dests_[sid];
  while (true) {
    size_t size = batch.tasks.size();
    shard_set->Add(sid, std::move(batch));
    RecordBatch(size);

    if (dest.staged.empty())
      break;
    batch = TakeBatch(&dest);
  }

  dest.sender_active = false;
  --active_senders_;
  if (active_senders_ == 0)
    senders_ec_.notifyAll();
}

void HopBatcher::RecordBatch(size_t size) {
  auto& stats = ServerState::tlocal()->stats;
  ++stats.hop_batches_sent;
  stats.hop_batched_tasks += size;
}

}  // namespace dfly
