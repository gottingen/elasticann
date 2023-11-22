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


#ifndef ELASTICANN_FLAGS_CLUSTER_H_
#define ELASTICANN_FLAGS_CLUSTER_H_

#include "gflags/gflags_declare.h"

namespace EA {

    /// for cluster management
    DECLARE_int32(migrate_percent);
    DECLARE_int32(error_judge_percent);
    DECLARE_int32(error_judge_number);
    DECLARE_int32(disk_used_percent);
    DECLARE_int32(min_network_segments_per_resource_tag);
    DECLARE_int32(network_segment_max_stores_precent);
    DECLARE_string(default_logical_room);
    DECLARE_string(default_physical_room);
    DECLARE_bool(need_check_slow);
    DECLARE_int64(modify_learner_peer_interval_us);
    DECLARE_int32(balance_add_peer_num);
    DECLARE_int64(show_table_status_cache_time);
    DECLARE_bool(peer_balance_by_ip);
    DECLARE_int64(region_apply_raft_interval_ms);
    DECLARE_int32(baikaldb_max_concurrent);
    DECLARE_int32(single_table_ddl_max_concurrent);
    DECLARE_int32(submit_task_number_per_round);
    DECLARE_int32(ddl_status_update_interval_us);
    DECLARE_int32(max_region_num_ratio);
    DECLARE_int32(max_ddl_retry_time);
    DECLARE_int32(baikal_heartbeat_interval_us);
    DECLARE_int64(table_tombstone_gc_time_s);
    DECLARE_int32(pre_split_threashold);
    /// for cluster management-table
    DECLARE_int32(region_replica_num);
    DECLARE_int32(learner_region_replica_num);
    DECLARE_int32(region_region_size);
    DECLARE_uint64(statistics_heart_beat_bytesize);
    DECLARE_int32(concurrency_num);

}  // namespace EA

#endif  // ELASTICANN_FLAGS_CLUSTER_H_
