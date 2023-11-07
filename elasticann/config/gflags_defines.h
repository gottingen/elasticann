// Copyright 2023 The Turbo Authors.
//
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

#ifndef ELASTICANN_CONFIG_GFLAGS_DEFINES_H_
#define ELASTICANN_CONFIG_GFLAGS_DEFINES_H_

#include "gflags/gflags_declare.h"

DECLARE_string(ea_plugin_root);

DECLARE_int32(ea_file_server_election_timeout_ms);
DECLARE_int32(ea_file_server_snapshot_interval_s);

namespace EA {

    /// for log
    DECLARE_bool(enable_console_log);
    DECLARE_string(log_root);
    DECLARE_int32(log_rotation_hour);
    DECLARE_int32(log_rotation_minute);
    DECLARE_string(log_base_name);
    DECLARE_int32(log_save_days);

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

    /// for common
    DECLARE_int64(memory_gc_interval_s);
    DECLARE_int64(memory_stats_interval_s);
    DECLARE_int64(min_memory_use_size);
    DECLARE_int64(min_memory_free_size_to_release);
    DECLARE_int64(mem_tracker_gc_interval_s);
    DECLARE_int64(process_memory_limit_bytes);
    DECLARE_int64(query_memory_limit_ratio);
    DECLARE_bool(need_health_check);
    DECLARE_int32(raft_write_concurrency);
    DECLARE_int32(service_write_concurrency);
    DECLARE_int32(snapshot_load_num);
    DECLARE_int32(baikal_heartbeat_concurrency);
    DECLARE_int64(incremental_info_gc_time);
    DECLARE_bool(enable_self_trace);
    DECLARE_bool(servitysinglelog);
    DECLARE_bool(open_service_write_concurrency);
    DECLARE_bool(schema_ignore_case);
    DECLARE_bool(disambiguate_select_name);
    DECLARE_int32(new_sign_read_concurrency);
    DECLARE_bool(open_new_sign_read_concurrency);
    DECLARE_bool(need_verify_ddl_permission);
    DECLARE_int32(histogram_split_threshold_percent);
    DECLARE_int32(limit_slow_sql_size);
#ifdef BAIKALDB_REVISION
    DECLARE_string(db_version);
#else
    DECLARE_string(db_version);
#endif
    DECLARE_bool(like_predicate_use_re2);
    DECLARE_bool(transfor_hll_raw_to_sparse);

    /// for meta
    DECLARE_string(meta_server_bns);
    DECLARE_int32(meta_replica_number);
    DECLARE_int32(meta_snapshot_interval_s);
    DECLARE_int32(meta_election_timeout_ms);
    DECLARE_string(meta_log_uri);
    DECLARE_string(meta_stable_uri);
    DECLARE_string(meta_snapshot_uri);
    DECLARE_int64(meta_check_migrate_interval_us);
    DECLARE_int32(meta_tso_snapshot_interval_s);
    DECLARE_string(meta_db_path);
    DECLARE_string(meta_listen);
    //DECLARE_int32(meta_port);
    DECLARE_int32(meta_request_timeout);
    DECLARE_int32(meta_connect_timeout);
    DECLARE_string(backup_meta_server_bns);
    DECLARE_int64(time_between_meta_connect_error_ms);

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

    /// for store engine
    DECLARE_int64(flush_memtable_interval_us);
    DECLARE_bool(cstore_scan_fill_cache);
    DECLARE_int32(rocks_transaction_lock_timeout_ms);
    DECLARE_int32(rocks_default_lock_timeout_ms);
    DECLARE_bool(rocks_use_partitioned_index_filters);
    DECLARE_bool(rocks_skip_stats_update_on_db_open);
    DECLARE_int32(rocks_block_size);
    DECLARE_int64(rocks_block_cache_size_mb);
    DECLARE_uint64(rocks_hard_pending_compaction_g);
    DECLARE_uint64(rocks_soft_pending_compaction_g);
    DECLARE_uint64(rocks_compaction_readahead_size);
    DECLARE_int32(rocks_data_compaction_pri);
    DECLARE_double(rocks_level_multiplier);
    DECLARE_double(rocks_high_pri_pool_ratio);
    DECLARE_int32(rocks_max_open_files);
    DECLARE_int32(rocks_max_subcompactions);
    DECLARE_int32(rocks_max_background_compactions);
    DECLARE_bool(rocks_optimize_filters_for_hits);
    DECLARE_int32(slowdown_write_sst_cnt);
    DECLARE_int32(stop_write_sst_cnt);
    DECLARE_bool(rocks_use_ribbon_filter);
    DECLARE_bool(rocks_use_hyper_clock_cache);
    DECLARE_bool(rocks_use_sst_partitioner_fixed_prefix);
    DECLARE_bool(rocks_kSkipAnyCorruptedRecords);
    DECLARE_bool(rocks_data_dynamic_level_bytes);
    DECLARE_int32(max_background_jobs);
    DECLARE_int32(max_write_buffer_number);
    DECLARE_int32(write_buffer_size);
    DECLARE_int32(min_write_buffer_number_to_merge);
    DECLARE_int32(rocks_binlog_max_files_size_gb);
    DECLARE_int32(rocks_binlog_ttl_days);
    DECLARE_int32(level0_file_num_compaction_trigger);
    DECLARE_int32(max_bytes_for_level_base);
    DECLARE_bool(enable_bottommost_compression);
    DECLARE_int32(target_file_size_base);
    DECLARE_int32(addpeer_rate_limit_level);
    DECLARE_bool(delete_files_in_range);
    DECLARE_bool(l0_compaction_use_lz4);
    DECLARE_bool(real_delete_old_binlog_cf);
    DECLARE_bool(rocksdb_fifo_allow_compaction);
    DECLARE_bool(use_direct_io_for_flush_and_compaction);
    DECLARE_bool(use_direct_reads);
    DECLARE_int32(level0_max_sst_num);
    DECLARE_int32(rocksdb_cost_sample);
    DECLARE_int64(qps_statistics_minutes_ago);
    DECLARE_int64(max_tokens_per_second);
    DECLARE_int64(use_token_bucket);
    DECLARE_int64(get_token_weight);
    DECLARE_int64(min_global_extended_percent);
    DECLARE_int64(token_bucket_adjust_interval_s);
    DECLARE_int64(token_bucket_burst_window_ms);
    DECLARE_int64(dml_use_token_bucket);
    DECLARE_int64(sql_token_bucket_timeout_min);
    DECLARE_int64(qos_reject_interval_s);
    DECLARE_int64(qos_reject_ratio);
    DECLARE_int64(qos_reject_timeout_s);
    DECLARE_int64(qos_reject_max_scan_ratio);
    DECLARE_int64(qos_reject_growth_multiple);
    DECLARE_int64(qos_need_reject);
    DECLARE_int64(sign_concurrency);
    DECLARE_bool(disable_wal);
    DECLARE_int64(exec_1pc_out_fsm_timeout_ms);
    DECLARE_int64(exec_1pc_in_fsm_timeout_ms);
    DECLARE_int64(retry_interval_us);
    DECLARE_int32(transaction_clear_delay_ms);
    DECLARE_int32(long_live_txn_interval_ms);
    DECLARE_int64(clean_finished_txn_interval_us);
    DECLARE_int64(one_pc_out_fsm_interval_us);
    // split slow down max time：5s
    DECLARE_int32(transaction_query_primary_region_interval_ms);
    /// for execute
    DECLARE_int64(txn_kv_max_dml_row_size);
    DECLARE_bool(disable_writebatch_index);
    DECLARE_int32(wait_after_prepare_us);
    DECLARE_bool(global_index_read_consistent);
    DECLARE_string(redis_addr);
    DECLARE_bool(check_condition_again_for_global_index);
    //DECLARE_int32(region_per_batch);
    DECLARE_bool(replace_no_get);
    DECLARE_bool(reverse_seek_first_level);
    DECLARE_bool(scan_use_multi_get);
    DECLARE_int32(in_predicate_check_threshold);
    DECLARE_uint64(max_in_records_num);
    DECLARE_int64(index_use_for_learner_delay_s);
    DECLARE_uint64(row_batch_size);
    DECLARE_int32(expect_bucket_count);
    DECLARE_bool(field_charsetnr_set_by_client);
    DECLARE_int32(single_store_concurrency);
    DECLARE_int64(max_select_rows);
    DECLARE_int64(print_time_us);
    DECLARE_int64(binlog_alarm_time_s);
    DECLARE_int64(baikaldb_alive_time_s);
    DECLARE_int32(fetcher_request_timeout);
    DECLARE_int32(fetcher_connect_timeout);
    DECLARE_bool(fetcher_follower_read);
    DECLARE_bool(fetcher_learner_read);
    DECLARE_string(insulate_fetcher_resource_tag);
    DECLARE_string(fetcher_resource_tag);
    DECLARE_bool(use_dynamic_timeout);
    DECLARE_bool(use_read_index);
    DECLARE_bool(open_nonboolean_sql_forbid);
    //DECLARE_bool(open_nonboolean_sql_statistics);
    
    /// for execute-plan
    DECLARE_bool(unique_index_default_global);
    DECLARE_bool(normal_index_default_global);
    DECLARE_bool(delete_all_to_truncate);
    DECLARE_bool(open_non_where_sql_forbid);
    DECLARE_bool(default_2pc);
    DECLARE_int32(cmsketch_depth);
    DECLARE_int32(cmsketch_width);
    DECLARE_int32(sample_rows);
    /// for execute-runtime
    DECLARE_int32(per_txn_max_num_locks);
    DECLARE_int64(row_number_to_check_memory);
    DECLARE_int32(time_length_to_delete_message);
    DECLARE_bool(limit_unappropriate_sql);

    /// for raft
    DECLARE_int64(snapshot_timeout_min);

    /// for index
    DECLARE_string(q2b_utf8_path);
    DECLARE_string(q2b_gbk_path);
    DECLARE_string(punctuation_path);
    DECLARE_bool(reverse_print_log);
    DECLARE_bool(enable_print_convert_log);
    
    /// for query
    DECLARE_string(log_plat_name);


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

    /// for file service
    DECLARE_int64(time_between_service_connect_error_ms);
    DECLARE_string(service_server_bns);
    DECLARE_string(backup_service_server_bns);
    DECLARE_int32(service_request_timeout);
    DECLARE_int32(service_connect_timeout);
    DECLARE_int32(service_snapshot_interval_s);
    DECLARE_int32(service_election_timeout_ms);
    DECLARE_string(service_log_uri);
    DECLARE_string(service_stable_uri);
    DECLARE_string(service_snapshot_uri);
    DECLARE_int64(service_check_migrate_interval_us);
    DECLARE_string(service_plugin_data_root);
    DECLARE_string(service_db_path);
    DECLARE_string(service_snapshot_sst);
    DECLARE_string(service_listen);
    DECLARE_int32(service_replica_number);

    /// for router
    DECLARE_string(router_listen);
}  // namespace EA

namespace bthread {
    DECLARE_int32(bthread_concurrency); //bthread.cpp
}
namespace braft {
    DECLARE_int32(raft_election_heartbeat_factor);
    DECLARE_bool(raft_enable_leader_lease);
}


#endif  // ELASTICANN_CONFIG_GFLAGS_DEFINES_H_
