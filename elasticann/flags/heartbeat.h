// Copyright 2023 The Elastic-AI Authors.
// part of Elastic AI Search
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//

#ifndef ELASTICANN_FLAGS_HEARTBEAT_H_
#define ELASTICANN_FLAGS_HEARTBEAT_H_

#include "gflags/gflags_declare.h"

namespace EA {
    /// for heartbeat
    DECLARE_int32(balance_periodicity);
    DECLARE_int32(region_faulty_interval_times);
    DECLARE_int32(store_faulty_interval_times);
    DECLARE_int32(store_dead_interval_times);
    DECLARE_int32(healthy_check_interval_times);
    DECLARE_int64(transfer_leader_catchup_time_threshold);
    DECLARE_int32(store_rocks_hang_check_timeout_s);
    DECLARE_int32(store_rocks_hang_cnt_limit);
    DECLARE_bool(store_rocks_hang_check);
    DECLARE_int32(upload_sst_streaming_concurrency);
    DECLARE_int32(global_select_concurrency);
    DECLARE_int64(store_heart_beat_interval_us);
}  // namespace EA

#endif  // ELASTICANN_FLAGS_HEARTBEAT_H_
