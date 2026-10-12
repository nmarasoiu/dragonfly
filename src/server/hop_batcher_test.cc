// Copyright 2026, DragonflyDB authors.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "server/hop_batcher.h"

#include <absl/flags/reflection.h>
#include <absl/strings/str_cat.h>

#include "base/flags.h"
#include "base/gtest.h"
#include "base/logging.h"
#include "facade/facade_test.h"
#include "server/test_utils.h"

ABSL_DECLARE_FLAG(bool, experimental_hop_batching);

namespace dfly {

using namespace std;
using namespace testing;
using namespace util;
using absl::StrCat;

class HopBatcherTest : public BaseFamilyTest {
 protected:
  HopBatcherTest() {
    num_threads_ = 3;
    absl::SetFlag(&FLAGS_experimental_hop_batching, true);
  }

  absl::FlagSaver saver_;
};

// Many fibers per thread issue single and multi shard commands concurrently, so that hops to the
// same shard are staged during the same scheduler round. Verifies results and that batching
// happened.
TEST_F(HopBatcherTest, ConcurrentCommands) {
  constexpr unsigned kFibersPerThread = 8;
  constexpr unsigned kIterations = 200;

  vector<fb2::Fiber> fibers;
  for (unsigned t = 0; t < num_threads_; ++t) {
    for (unsigned f = 0; f < kFibersPerThread; ++f) {
      fibers.push_back(pp_->at(t)->LaunchFiber([this, t, f] {
        string id = StrCat("conn", t, "_", f);
        for (unsigned i = 0; i < kIterations; ++i) {
          string k1 = StrCat("a:", t, ":", f, ":", i % 7);
          string k2 = StrCat("b:", t, ":", f, ":", i % 5);
          string k3 = StrCat("c:", t, ":", f, ":", i % 3);
          string val = StrCat(i);

          EXPECT_EQ(Run(id, {"set", k1, val}), "OK");
          EXPECT_EQ(Run(id, {"get", k1}), val);

          EXPECT_EQ(Run(id, {"mset", k1, val, k2, val, k3, val}), "OK");
          auto resp = Run(id, {"mget", k1, k2, k3});
          EXPECT_THAT(resp, RespArray(ElementsAre(val, val, val)));

          EXPECT_THAT(Run(id, {"del", k2, k3}), IntArg(2));
        }
      }));
    }
  }

  for (auto& fb : fibers)
    fb.Join();

  auto metrics = GetMetrics();
  uint64_t batches = metrics.coordinator_stats.hop_batches_sent;
  uint64_t tasks = metrics.coordinator_stats.hop_batched_tasks;
  LOG(INFO) << "hop batches: " << batches << ", hops: " << tasks;

  EXPECT_GT(batches, 0u);
  EXPECT_GT(tasks, batches);  // at least some batches carried more than one hop.
}

TEST_F(HopBatcherTest, MultiExec) {
  for (unsigned i = 0; i < 50; ++i) {
    string val = StrCat(i);
    Run({"multi"});
    Run({"set", "x", val});
    Run({"set", "c", val});
    Run({"set", "b", val});
    Run({"mget", "x", "c", "b"});
    auto resp = Run({"exec"});
    ASSERT_THAT(resp, ArrLen(4));
    EXPECT_THAT(resp.GetVec()[3], RespArray(ElementsAre(val, val, val)));
  }
}

}  // namespace dfly
