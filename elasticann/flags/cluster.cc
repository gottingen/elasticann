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
#include "elasticann/flags/cluster.h"
#include "gflags/gflags.h"
#include "brpc/reloadable_flags.h"

namespace EA {

    /// for cluster management
    DEFINE_int32(migrate_percent, 60, "migrate percent. default:60%");
    DEFINE_int32(error_judge_percent, 10, "error judge percen. default:10");
    DEFINE_int32(error_judge_number, 3, "error judge number. default:3");
    DEFINE_int32(disk_used_percent, 80, "disk used percent. default:80%");
    DEFINE_int32(min_network_segments_per_resource_tag, 10, "min network segments per resource_tag");
    DEFINE_int32(network_segment_max_stores_precent, 20, "network segment max stores precent");
    DEFINE_string(default_logical_room, "default", "default_logical_room");
    DEFINE_string(default_physical_room, "default", "default_physical_room");
    DEFINE_bool(need_check_slow, false, "need_check_slow. default:false");
    DEFINE_int64(modify_learner_peer_interval_us, 100 * 1000 * 1000LL, "modify learner peer interval");
    DEFINE_int32(balance_add_peer_num, 10, "add peer num each time, default(10)");
    BRPC_VALIDATE_GFLAG(balance_add_peer_num, brpc::PositiveInteger);
    DEFINE_int64(show_table_status_cache_time, 3600 * 1000 * 1000LL, "show table status cache time : 3600s");
    DEFINE_bool(peer_balance_by_ip, false, "default by ip:port");
    DEFINE_int64(region_apply_raft_interval_ms, 1000LL, "region apply raft interval, default(1s)");
    DEFINE_int32(baikaldb_max_concurrent, 5, "ddl work baikaldb concurrent");
    DEFINE_int32(single_table_ddl_max_concurrent, 50, "ddl work baikaldb concurrent");
    DEFINE_int32(submit_task_number_per_round, 20, "submit task number per round");
    DEFINE_int32(ddl_status_update_interval_us, 10 * 1000 * 1000, "ddl_status_update_interval(us)");
    DEFINE_int32(max_region_num_ratio, 2, "max region number ratio");
    DEFINE_int32(max_ddl_retry_time, 30, "max ddl retry time");
    DEFINE_int32(baikal_heartbeat_interval_us, 10 * 1000 * 1000, "baikal_heartbeat_interval(us)");
    DEFINE_int64(table_tombstone_gc_time_s, 3600 * 24 * 5, "time interval to clear table_tombstone. default(5d)");
    DEFINE_int32(pre_split_threashold, 300, "pre_split_threashold for sync create table");
    /// for cluster management-table
    DEFINE_int32(region_replica_num, 3, "region replica num, default:3");
    DEFINE_int32(learner_region_replica_num, 1, "learner region replica num, default:1");
    DEFINE_int32(region_region_size, 100 * 1024 * 1024, "region size, default:100M");
    DEFINE_uint64(statistics_heart_beat_bytesize, 256 * 1024 * 1024, "default(256M)");
    DEFINE_int32(concurrency_num, 40, "concurrency num, default: 40");

}  // namespace