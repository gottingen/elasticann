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

#ifndef ELASTICANN_FLAGS_STORE_H_
#define ELASTICANN_FLAGS_STORE_H_

#include "gflags/gflags_declare.h"

namespace EA {

    /// for store
    DECLARE_string(store_db_path);
    DECLARE_int32(store_request_timeout);
    DECLARE_int32(store_connect_timeout);
    DECLARE_int32(store_port);
    DECLARE_bool(store_use_fulltext_wordweight_segment);
    DECLARE_bool(store_use_fulltext_wordseg_wordrank_segment);
    DECLARE_string(store_wordrank_conf);
    DECLARE_bool(store_stop_server_before_core);
    DECLARE_int64(store_reverse_merge_interval_us);
    DECLARE_int64(store_ttl_remove_interval_s);
    DECLARE_string(store_ttl_remove_interval_period);
    DECLARE_int64(store_delay_remove_region_interval_s);
    DECLARE_string(store_resource_tag);
    DECLARE_int32(store_update_used_size_interval_us);
    DECLARE_int32(store_init_region_concurrency);
    DECLARE_int32(store_split_threshold);
    DECLARE_int64(store_min_split_lines);
    DECLARE_int64(store_flush_region_interval_us);
    DECLARE_int64(store_transaction_clear_interval_ms);
    DECLARE_int64(store_binlog_timeout_check_ms);
    DECLARE_int64(store_binlog_fake_ms);
    DECLARE_int64(store_oldest_binlog_ts_interval_s);
    DECLARE_int32(store_max_split_concurrency);
    DECLARE_int64(store_none_region_merge_interval_us);
    DECLARE_int64(store_region_delay_remove_timeout_s);
    DECLARE_bool(store_use_approximate_size);
    DECLARE_bool(store_use_approximate_size_to_split);
    DECLARE_int64(store_gen_tso_interval_us);
    DECLARE_int64(store_gen_tso_count);
    DECLARE_int64(store_rocks_cf_flush_remove_range_times);
    DECLARE_int64(store_rocks_force_flush_max_wals);
    DECLARE_string(store_network_segment);
    DECLARE_string(store_container_id);
    DECLARE_int32(store_rocksdb_perf_level);
    DECLARE_bool(store_stop_ttl_data);
    DECLARE_int64(store_check_peer_delay_min);
    DECLARE_int64(store_streaming_max_buf_size);
    DECLARE_int64(store_streaming_idle_timeout_ms);
    DECLARE_int32(store_compact_interval);
    DECLARE_bool(store_allow_compact_range);
    DECLARE_bool(store_allow_blocking_flush);
    DECLARE_int64(store_binlog_timeout_us);
    DECLARE_int64(store_binlog_warn_timeout_minute);
    DECLARE_int64(store_read_binlog_max_size_bytes);
    DECLARE_int64(store_read_binlog_timeout_us);
    DECLARE_int64(store_check_point_rollback_interval_s);
    DECLARE_int64(store_binlog_multiget_batch);
    DECLARE_int64(store_binlog_seek_batch);
    DECLARE_int64(store_binlog_use_seek_interval_min);
    DECLARE_bool(store_binlog_force_get);
    DECLARE_int32(store_election_timeout_ms);
    DECLARE_int32(store_skew);
    DECLARE_int32(store_reverse_level2_len);
    DECLARE_string(store_log_uri);
    DECLARE_string(store_binlog_uri);
    DECLARE_string(store_stable_uri);
    DECLARE_string(store_snapshot_uri);
    DECLARE_int64(store_disable_write_wait_timeout_us);
    DECLARE_int32(store_snapshot_interval_s);
    DECLARE_int32(store_fetch_log_timeout_s);
    DECLARE_int32(store_fetch_log_interval_ms);
    DECLARE_int32(store_snapshot_timed_wait);
    DECLARE_int64(store_snapshot_diff_lines);
    DECLARE_int64(store_snapshot_diff_logs);
    DECLARE_int64(store_snapshot_log_exec_time_s);
    DECLARE_int64(store_split_duration_us);
    DECLARE_int64(store_compact_delete_lines);
    DECLARE_int64(store_throttle_throughput_bytes);
    DECLARE_int64(store_tail_split_wait_threshold);
    DECLARE_int64(store_split_send_first_log_entry_threshold);
    DECLARE_int64(store_split_send_log_batch_size);
    DECLARE_int64(store_no_write_log_entry_threshold);
    DECLARE_int64(store_check_peer_notice_delay_s);
    DECLARE_bool(store_force_clear_txn_for_fast_recovery);
    DECLARE_bool(store_split_add_peer_asyc);
    DECLARE_int32(store_no_op_timer_timeout_ms);
    DECLARE_int32(store_follow_read_timeout_s);
    DECLARE_bool(store_apply_partial_rollback);
    DECLARE_bool(store_demotion_read_index_without_leader);
    DECLARE_int64(store_sign_concurrency_timeout_rate);
    DECLARE_int64(store_min_sign_concurrency_timeout_ms);
    DECLARE_int32(store_not_leader_alarm_print_interval_s);

}  // namespace EA

#endif  // ELASTICANN_FLAGS_STORE_H_
