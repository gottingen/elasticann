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
#include "elasticann/flags/store.h"
#include "gflags/gflags.h"
#include "brpc/reloadable_flags.h"
#include "rocksdb/perf_context.h"

namespace EA {

    /// for store
    DEFINE_string(store_db_path, "./rocks_db", "rocksdb path");
    DEFINE_int32(store_request_timeout, 60000,
                 "store as server request timeout, default:60000ms");
    DEFINE_int32(store_connect_timeout, 5000,
                 "store as server connect timeout, default:5000ms");
    DEFINE_int32(store_port, 8110, "Server port");
    DEFINE_bool(store_use_fulltext_wordweight_segment, true, "load wordweight dict");
    DEFINE_bool(store_use_fulltext_wordseg_wordrank_segment, true, "load wordseg wordrank dict");
    DEFINE_string(store_wordrank_conf, "./config/drpc_client.xml", "wordrank conf path");
    DEFINE_bool(store_stop_server_before_core, true, "stop_server_before_core");
    DEFINE_int64(store_reverse_merge_interval_us, 2 * 1000 * 1000, "reverse_merge_interval(2 s)");
    DEFINE_int64(store_ttl_remove_interval_s, 24 * 3600, "ttl_remove_interval_s(24h)");
    DEFINE_string(store_ttl_remove_interval_period, "", "ttl_remove_interval_period hour(0-23)");
    DEFINE_int64(store_delay_remove_region_interval_s, 600, "delay_remove_region_interval");
    DEFINE_string(store_resource_tag, "", "resource tag");

    DEFINE_int32(store_update_used_size_interval_us, 10 * 1000 * 1000, "update used size interval (10 s)");
    DEFINE_int32(store_init_region_concurrency, 10, "init region concurrency when start");
    DEFINE_int32(store_split_threshold, 150, "split_threshold, default: 150% * region_size / 100");
    DEFINE_int64(store_min_split_lines, 200000, "min_split_lines, protected when wrong param put in table");
    DEFINE_int64(store_flush_region_interval_us, 10 * 60 * 1000 * 1000LL,
                 "flush region interval, default(10 min)");
    DEFINE_int64(store_transaction_clear_interval_ms, 5000LL,
                 "transaction clear interval, default(5s)");
    DEFINE_int64(store_binlog_timeout_check_ms, 10 * 1000LL,
                 "binlog timeout check interval, default(10s)");
    DEFINE_int64(store_binlog_fake_ms, 1 * 1000LL,
                 "fake binlog interval, default(1s)");
    DEFINE_int64(store_oldest_binlog_ts_interval_s, 3600LL,
                 "oldest_binlog_ts_interval_s, default(1h)");
    DEFINE_int32(store_max_split_concurrency, 2, "max split region concurrency, default:2");
    DEFINE_int64(store_none_region_merge_interval_us, 5 * 60 * 1000 * 1000LL,
                 "none region merge interval, default(5 min)");
    DEFINE_int64(store_region_delay_remove_timeout_s, 3600 * 24LL,
                 "region_delay_remove_time_s, default(1d)");
    DEFINE_bool(store_use_approximate_size, true,
                "use_approximate_size");
    DEFINE_bool(store_use_approximate_size_to_split, false,
                "if approximate_size > 512M, then split");
    DEFINE_int64(store_gen_tso_interval_us, 500 * 1000LL, "gen_tso_interval_us, default(500ms)");
    DEFINE_int64(store_gen_tso_count, 100, "gen_tso_count, default(500)");
    DEFINE_int64(store_rocks_cf_flush_remove_range_times, 10, "rocks_cf_flush_remove_range_times, default(10)");
    DEFINE_int64(store_rocks_force_flush_max_wals, 100, "rocks_force_flush_max_wals, default(100)");
    DEFINE_string(store_network_segment, "", "network segment of store set by user");
    DEFINE_string(store_container_id, "", "container_id for zoombie instance");
    DEFINE_int32(store_rocksdb_perf_level, rocksdb::kDisable, "rocksdb_perf_level");
    DEFINE_bool(store_stop_ttl_data, false, "stop ttl data");
    DEFINE_int64(store_check_peer_delay_min, 1, "check peer delay min");

    BRPC_VALIDATE_GFLAG(store_rocksdb_perf_level, brpc::NonNegativeInteger);
    DEFINE_int64(store_streaming_max_buf_size, 60 * 1024 * 1024LL, "streaming max buf size : 60M");
    DEFINE_int64(store_streaming_idle_timeout_ms, 1800 * 1000LL, "streaming idle time : 1800s");

    DEFINE_int32(store_compact_interval, 1, "compact_interval xx (s)");
    DEFINE_bool(store_allow_compact_range, true, "allow_compact_range");
    DEFINE_bool(store_allow_blocking_flush, true, "allow_blocking_flush");
    DEFINE_int64(store_binlog_timeout_us, 10 * 1000 * 1000LL, "binlog timeout us : 10s");
    DEFINE_int64(store_binlog_warn_timeout_minute, 15, "binlog warn timeout min : 15min");
    DEFINE_int64(store_read_binlog_max_size_bytes, 100 * 1024 * 1024, "100M");
    DEFINE_int64(store_read_binlog_timeout_us, 10 * 1000 * 1000, "10s");
    DEFINE_int64(store_check_point_rollback_interval_s, 60, "60s");
    DEFINE_int64(store_binlog_multiget_batch, 1024, "1024");
    DEFINE_int64(store_binlog_seek_batch, 10000, "10000");
    DEFINE_int64(store_binlog_use_seek_interval_min, 60, "1h");
    DEFINE_bool(store_binlog_force_get, false, "false");
    DEFINE_int32(store_election_timeout_ms, 1000, "raft election timeout(ms)");
    DEFINE_int32(store_skew, 5, "split skew, default : 45% - 55%");
    DEFINE_int32(store_reverse_level2_len, 5000, "reverse index level2 length, default : 5000");
    DEFINE_string(store_log_uri, "myraftlog://my_raft_log?id=", "raft log uri");
    DEFINE_string(store_binlog_uri, "mybinlog://my_bin_log?id=", "bin log uri");
    //不兼容配置，默认用写到rocksdb的信息; raft自带的local://./raft_data/stable/region_
    DEFINE_string(store_stable_uri, "myraftmeta://my_raft_meta?id=", "raft stable path");
    DEFINE_string(store_snapshot_uri, "local://./raft_data/snapshot", "raft snapshot path");
    DEFINE_int64(store_disable_write_wait_timeout_us, 1000 * 1000,
                 "disable write wait timeout(us) default 1s");
    DEFINE_int32(store_snapshot_interval_s, 600, "raft snapshot interval(s)");
    DEFINE_int32(store_fetch_log_timeout_s, 60, "raft learner fetch log time out(s)");
    DEFINE_int32(store_fetch_log_interval_ms, 10, "raft learner fetch log interval(ms)");
    DEFINE_int32(store_snapshot_timed_wait, 120 * 1000 * 1000LL, "snapshot timed wait default 120S");
    DEFINE_int64(store_snapshot_diff_lines, 10000, "save_snapshot when num_table_lines diff");
    DEFINE_int64(store_snapshot_diff_logs, 2000, "save_snapshot when log entries diff");
    DEFINE_int64(store_snapshot_log_exec_time_s, 60, "save_snapshot when log entries apply time");
    //分裂判断标准，如果3600S没有收到请求，则认为分裂失败
    DEFINE_int64(store_split_duration_us, 3600 * 1000 * 1000LL, "split duration time : 3600s");
    DEFINE_int64(store_compact_delete_lines, 200000, "compact when _num_delete_lines > compact_delete_lines");
    DEFINE_int64(store_throttle_throughput_bytes, 50 * 1024 * 1024LL, "throttle throughput bytes");
    DEFINE_int64(store_tail_split_wait_threshold, 600 * 1000 * 1000LL, "tail split wait threshold(10min)");
    DEFINE_int64(store_split_send_first_log_entry_threshold, 3600 * 1000 * 1000LL, "split send log entry threshold(1h)");
    DEFINE_int64(store_split_send_log_batch_size, 20, "split send log batch size");
    DEFINE_int64(store_no_write_log_entry_threshold, 1000, "max left logEntry to be exec before no write");
    DEFINE_int64(store_check_peer_notice_delay_s, 1, "check peer delay notice second");
    DEFINE_bool(store_force_clear_txn_for_fast_recovery, false, "clear all txn info for fast recovery");
    DEFINE_bool(store_split_add_peer_asyc, false, "asyc split add peer");
    DEFINE_int32(store_no_op_timer_timeout_ms, 100, "no op timer timeout(ms)");
    DEFINE_int32(store_follow_read_timeout_s, 10, "follow read timeout(s)");
    DEFINE_bool(store_apply_partial_rollback, false, "apply partial rollback");
    DEFINE_bool(store_demotion_read_index_without_leader, true, "demotion read index without leader");
    DEFINE_int64(store_sign_concurrency_timeout_rate, 5,
                 "sign_concurrency_timeout_rate, default: 5. (0 means without timeout)");
    DEFINE_int64(store_min_sign_concurrency_timeout_ms, 1000, "min_sign_concurrency_timeout_ms, default: 1s");
    DEFINE_int32(store_not_leader_alarm_print_interval_s, 60, "not leader alarm print interval(s)");

}  // namespace EA
