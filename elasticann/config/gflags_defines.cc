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
#include "elasticann/config/gflags_defines.h"
#include "gflags/gflags.h"
#include "brpc/reloadable_flags.h"
#include "rocksdb/perf_context.h"



DEFINE_string(ea_plugin_root, "./plugins", "ea flags plugin root");

DEFINE_int32(ea_file_server_election_timeout_ms, 1000, "raft election timeout(ms)");
DEFINE_int32(ea_file_server_snapshot_interval_s, 600, "raft snapshot interval(s)");
DEFINE_string(ea_file_server_log_uri, "myraftlog://my_raft_log?id=", "raft log uri");

namespace EA {
    /// for log
    DEFINE_bool(enable_console_log, true, "console or file log");
    DEFINE_string(log_root, "./logs", "ea flags log root");
    DEFINE_int32(log_rotation_hour, 2, "rotation hour");
    DEFINE_int32(log_rotation_minute, 30, "rotation minutes");
    DEFINE_string(log_base_name, "ea_log.txt", "base name for EA");
    DEFINE_int32(log_save_days, 7, "ea log save days");

    /// for heartbeat
    DEFINE_int32(balance_periodicity, 60, "times of store heart beat");
    DEFINE_int32(region_faulty_interval_times, 3, "region faulty interval times of heart beat interval");
    DEFINE_int32(store_faulty_interval_times, 3, "store faulty interval times of heart beat");
    DEFINE_int32(store_dead_interval_times, 60, "store dead interval times of heart beat");
    DEFINE_int32(healthy_check_interval_times, 1, "meta state machine healthy check interval times of heart beat");
    DEFINE_int64(transfer_leader_catchup_time_threshold, 1 * 1000 * 1000LL, "transfer leader catchup time threshold");
    DEFINE_int32(store_rocks_hang_check_timeout_s, 5, "store rocks hang check timeout");
    DEFINE_int32(store_rocks_hang_cnt_limit, 3, "store rocks hang check cnt limit for slow");
    DEFINE_bool(store_rocks_hang_check, false, "store rocks hang check");
    DEFINE_int32(upload_sst_streaming_concurrency, 10, "upload_sst_streaming_concurrency");
    DEFINE_int32(global_select_concurrency, 24, "global_select_concurrency");
    DEFINE_int64(store_heart_beat_interval_us, 30 * 1000 * 1000, "store heart interval (30 s)");


    /// common
    DEFINE_int64(memory_gc_interval_s, 10, "mempry GC interval , default: 10s");
    DEFINE_int64(memory_stats_interval_s, 60, "mempry GC interval , default: 60s");
    DEFINE_int64(min_memory_use_size, 8589934592, "minimum memory use size , default: 8G");
    DEFINE_int64(min_memory_free_size_to_release, 2147483648, "minimum memory free size to release, default: 2G");
    DEFINE_int64(mem_tracker_gc_interval_s, 60, "do memory limit when row number more than #, default: 60");
    DEFINE_int64(process_memory_limit_bytes, -1, "all memory use size, default: -1");
    DEFINE_int64(query_memory_limit_ratio, 90, "query memory use ratio , default: 90%");
    DEFINE_bool(need_health_check, true, "need_health_check");
    DEFINE_int32(raft_write_concurrency, 40, "raft_write concurrency, default:40");
    DEFINE_int32(service_write_concurrency, 40, "service_write concurrency, default:40");
    DEFINE_int32(snapshot_load_num, 4, "snapshot load concurrency, default 4");
    DEFINE_int32(baikal_heartbeat_concurrency, 10, "baikal heartbeat concurrency, default:10");
    DEFINE_int64(incremental_info_gc_time, 600 * 1000 * 1000, "time interval to clear incremental info");
    DEFINE_bool(enable_self_trace, true, "open SELF_TRACE log");
    DEFINE_bool(servitysinglelog, true, "diff servity message in seperate logfile");
    DEFINE_bool(open_service_write_concurrency, true, "open service_write_concurrency, default: true");
    DEFINE_bool(schema_ignore_case, false, "whether ignore case when match db/table name");
    DEFINE_bool(disambiguate_select_name, false, "whether use the first when select name is ambiguous, default false");
    DEFINE_int32(new_sign_read_concurrency, 10, "new_sign_read concurrency, default:20");
    DEFINE_bool(open_new_sign_read_concurrency, false, "open new_sign_read concurrency, default: false");
    DEFINE_bool(need_verify_ddl_permission, false, "default true");
    DEFINE_int32(histogram_split_threshold_percent, 50, "histogram_split_threshold default 0.5");
    DEFINE_int32(limit_slow_sql_size, 50, "each sign to slow query sql counts, default: 50");
#ifdef BAIKALDB_REVISION
    DEFINE_string(db_version, "5.7.16-BaikalDB-v"BAIKALDB_REVISION, "db version");
#else
    DEFINE_string(db_version, "5.7.16-BaikalDB", "db version");
#endif
    DEFINE_bool(like_predicate_use_re2, false, "LikePredicate use re2");
    DEFINE_bool(transfor_hll_raw_to_sparse, false, "try transfor raw hll to sparse");

    /// for meta
    DEFINE_string(meta_server_bns, "group.opera-qa-baikalMeta-000-yz.FENGCHAO.all", "meta server bns");
    DEFINE_int32(meta_replica_number, 3, "Meta replica num");
    DEFINE_int32(meta_snapshot_interval_s, 600, "raft snapshot interval(s)");
    DEFINE_int32(meta_election_timeout_ms, 1000, "raft election timeout(ms)");
    DEFINE_string(meta_log_uri, "myraftlog://my_raft_log?id=", "raft log uri");
    DEFINE_string(meta_stable_uri, "local://./raft_data/stable", "raft stable path");
    DEFINE_string(meta_snapshot_uri, "local://./raft_data/snapshot", "raft snapshot path");
    DEFINE_int64(meta_check_migrate_interval_us, 60 * 1000 * 1000LL, "check meta server migrate interval (60s)");
    DEFINE_int32(meta_tso_snapshot_interval_s, 60, "tso raft snapshot interval(s)");
    DEFINE_string(meta_db_path, "./rocks_db", "rocks db path");
    DEFINE_string(meta_listen,"127.0.0.1:8010", "meta listen addr");
    //DEFINE_int32(meta_port, 8010, "Meta port");
    DEFINE_int32(meta_request_timeout, 30000,
                 "meta as server request timeout, default:30000ms");
    DEFINE_int32(meta_connect_timeout, 5000,
                 "meta as server connect timeout, default:5000ms");

    DEFINE_string(backup_meta_server_bns, "", "backup_meta_server_bns");
    DEFINE_int64(time_between_meta_connect_error_ms, 0, "time_between_meta_connect_error_ms. default(0ms)");

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

    /// for store engine
    DEFINE_int64(flush_memtable_interval_us, 10 * 60 * 1000 * 1000LL, "flush memtable interval, default(10 min)");
    DEFINE_bool(cstore_scan_fill_cache, true, "cstore_scan_fill_cache");
    DEFINE_int32(rocks_transaction_lock_timeout_ms, 20000,
                 "rocksdb transaction_lock_timeout, real lock_time is 'time + rand_less(time)' (ms)");
    DEFINE_int32(rocks_default_lock_timeout_ms, 30000, "rocksdb default_lock_timeout(ms)");
    DEFINE_bool(rocks_use_partitioned_index_filters, false, "rocksdb use Partitioned Index Filters");
    DEFINE_bool(rocks_skip_stats_update_on_db_open, false, "rocks_skip_stats_update_on_db_open");
    DEFINE_int32(rocks_block_size, 64 * 1024, "rocksdb block_cache size, default: 64KB");
    DEFINE_int64(rocks_block_cache_size_mb, 8 * 1024, "rocksdb block_cache_size_mb, default: 8G");
    DEFINE_uint64(rocks_hard_pending_compaction_g, 256, "rocksdb hard_pending_compaction_bytes_limit , default: 256G");
    DEFINE_uint64(rocks_soft_pending_compaction_g, 64, "rocksdb soft_pending_compaction_bytes_limit , default: 64G");
    DEFINE_uint64(rocks_compaction_readahead_size, 0, "rocksdb compaction_readahead_size, default: 0");
    DEFINE_int32(rocks_data_compaction_pri, 3, "rocksdb data_cf compaction_pri, default: 3(kMinOverlappingRatio)");
    DEFINE_double(rocks_level_multiplier, 10, "data_cf rocksdb max_bytes_for_level_multiplier, default: 10");
    DEFINE_double(rocks_high_pri_pool_ratio, 0.5, "rocksdb cache high_pri_pool_ratio, default: 0.5");
    DEFINE_int32(rocks_max_open_files, 1024, "rocksdb max_open_files, default: 1024");
    DEFINE_int32(rocks_max_subcompactions, 4, "rocks_max_subcompactions");
    DEFINE_int32(rocks_max_background_compactions, 20, "max_background_compactions");
    DEFINE_bool(rocks_optimize_filters_for_hits, false, "rocks_optimize_filters_for_hits");
    DEFINE_int32(slowdown_write_sst_cnt, 10, "level0_slowdown_writes_trigger");
    DEFINE_int32(stop_write_sst_cnt, 40, "level0_stop_writes_trigger");
    DEFINE_bool(rocks_use_ribbon_filter, false,
                "use Ribbon filter:https://github.com/facebook/rocksdb/wiki/RocksDB-Bloom-Filter");
    DEFINE_bool(rocks_use_hyper_clock_cache, false,
                "use HyperClockCache:https://github.com/facebook/rocksdb/pull/10963");
    DEFINE_bool(rocks_use_sst_partitioner_fixed_prefix, false,
                "use SstPartitionerFixedPrefix:https://github.com/facebook/rocksdb/pull/6957");
    DEFINE_bool(rocks_kSkipAnyCorruptedRecords, false,
                "We ignore any corruption in the WAL and try to salvage as much data as possible");
    DEFINE_bool(rocks_data_dynamic_level_bytes, true,
                "rocksdb level_compaction_dynamic_level_bytes for data column_family, default true");
    DEFINE_int32(max_background_jobs, 24, "max_background_jobs");
    DEFINE_int32(max_write_buffer_number, 6, "max_write_buffer_number");
    DEFINE_int32(write_buffer_size, 128 * 1024 * 1024, "write_buffer_size");
    DEFINE_int32(min_write_buffer_number_to_merge, 2, "min_write_buffer_number_to_merge");
    DEFINE_int32(rocks_binlog_max_files_size_gb, 100, "binlog max size default 100G");
    DEFINE_int32(rocks_binlog_ttl_days, 7, "binlog ttl default 7 days");

    DEFINE_int32(level0_file_num_compaction_trigger, 5, "Number of files to trigger level-0 compaction");
    DEFINE_int32(max_bytes_for_level_base, 1024 * 1024 * 1024, "total size of level 1.");
    DEFINE_bool(enable_bottommost_compression, false, "enable zstd for bottommost_compression");
    DEFINE_int32(target_file_size_base, 128 * 1024 * 1024, "target_file_size_base");
    DEFINE_int32(addpeer_rate_limit_level, 1, "addpeer_rate_limit_level; "
                                              "0:no limit, 1:limit when stalling, 2:limit when compaction pending. default(1)");
    DEFINE_bool(delete_files_in_range, true, "delete_files_in_range");
    DEFINE_bool(l0_compaction_use_lz4, false, "L0 sst compaction use lz4 or not");
    DEFINE_bool(real_delete_old_binlog_cf, true, "default true");
    DEFINE_bool(rocksdb_fifo_allow_compaction, false, "default false");
    DEFINE_bool(use_direct_io_for_flush_and_compaction, false, "default false");
    DEFINE_bool(use_direct_reads, false, "default false");
    DEFINE_int32(level0_max_sst_num, 500, "max level0 num for fast importer");
    DEFINE_int32(rocksdb_cost_sample, 100, "rocksdb_cost_sample");
    DEFINE_int64(qps_statistics_minutes_ago, 60, "qps_statistics_minutes_ago, default: 1h"); // 默认以前一小时的统计信息作为参考
    DEFINE_int64(max_tokens_per_second, 100000, "max_tokens_per_second, default: 10w");
    DEFINE_int64(use_token_bucket, 0, "use_token_bucket, 0:close; 1:open, default: 0");
    DEFINE_int64(get_token_weight, 5, "get_token_weight, default: 5");
    DEFINE_int64(min_global_extended_percent, 40, "min_global_extended_percent, default: 40%");
    DEFINE_int64(token_bucket_adjust_interval_s, 60, "token_bucket_adjust_interval_s, default: 60s");
    DEFINE_int64(token_bucket_burst_window_ms, 10, "token_bucket_burst_window_ms, default: 10ms");
    DEFINE_int64(dml_use_token_bucket, 0, "dml_use_token_bucket, default: 0");
    DEFINE_int64(sql_token_bucket_timeout_min, 5, "sql_token_bucket_timeout_min, default: 5min");
    DEFINE_int64(qos_reject_interval_s, 30, "qos_reject_interval_s, default: 30s");
    DEFINE_int64(qos_reject_ratio, 90, "qos_reject_ratio, default: 90%");
    DEFINE_int64(qos_reject_timeout_s, 30 * 60, "qos_reject_timeout_s, default: 30min");
    DEFINE_int64(qos_reject_max_scan_ratio, 50, "qos_reject_max_scan_ratio, default: 50%");
    DEFINE_int64(qos_reject_growth_multiple, 100, "qos_reject_growth_multiple, default: 100倍");
    DEFINE_int64(qos_need_reject, 0, "qos_need_reject, default: 0");
    DEFINE_int64(sign_concurrency, 8, "sign_concurrency, default: 8");
    DEFINE_bool(disable_wal, false, "disable rocksdb interanal WAL log, only use raft log");
    DEFINE_int64(exec_1pc_out_fsm_timeout_ms, 5 * 1000, "exec 1pc out of fsm, timeout");
    DEFINE_int64(exec_1pc_in_fsm_timeout_ms, 100, "exec 1pc in fsm, timeout");
    DEFINE_int64(retry_interval_us, 500 * 1000, "retry interval ");
    DEFINE_int32(transaction_clear_delay_ms, 600 * 1000,
                 "delay duration to clear prepared and expired transactions");
    DEFINE_int32(long_live_txn_interval_ms, 300 * 1000,
                 "delay duration to clear prepared and expired transactions");
    DEFINE_int64(clean_finished_txn_interval_us, 600 * 1000 * 1000LL,
                 "clean_finished_txn_interval_us");
    DEFINE_int64(one_pc_out_fsm_interval_us, 20 * 1000 * 1000LL,
                 "clean_finished_txn_interval_us");
    // 分裂slow down max time：5s
    DEFINE_int32(transaction_query_primary_region_interval_ms, 15 * 1000,
                 "interval duration send request to primary region");
    /// for execute
    DEFINE_int64(txn_kv_max_dml_row_size, 4096, "max dml rows to use kv mode, default(4096)");
    DEFINE_bool(disable_writebatch_index, false,
                "disable the indexing of transaction writebatch, if true the uncommitted data cannot be read");
    DEFINE_int32(wait_after_prepare_us, 0, "wait time after prepare(us)");
    DEFINE_bool(global_index_read_consistent, true, "double check for global and primary region consistency");
    DEFINE_string(redis_addr, "127.0.0.1:6379", "redis addr");
    DEFINE_bool(check_condition_again_for_global_index, false, "avoid write skew for global index if true");
    //DEFINE_int32(region_per_batch, 4, "request region number in a batch");
    DEFINE_bool(replace_no_get, false, "no get before replace if true");
    DEFINE_bool(reverse_seek_first_level, false, "reverse index seek first level, default(false)");
    DEFINE_bool(scan_use_multi_get, true, "use MultiGet API, default(true)");
    DEFINE_int32(in_predicate_check_threshold, 4096, "in predicate threshold to check memory, default(4096)");
    DEFINE_uint64(max_in_records_num, 10000, "max_in_records_num");
    DEFINE_int64(index_use_for_learner_delay_s, 3600, "1h");
    DEFINE_uint64(row_batch_size, 200, "row_batch_size");
    DEFINE_int32(expect_bucket_count, 100, "expect_bucket_count");
    DEFINE_bool(field_charsetnr_set_by_client, false, "set charsetnr by client");
    DEFINE_int32(single_store_concurrency, 20, "max request for one store");
    DEFINE_int64(max_select_rows, 10000000, "query will be fail when select too much rows");
    DEFINE_int64(print_time_us, 10000, "print log when time_cost > print_time_us(us)");
    DEFINE_int64(binlog_alarm_time_s, 30, "alarm, > binlog_alarm_time_s from prewrite to commit");
    DEFINE_int64(baikaldb_alive_time_s, 10 * 60, "obervation time length in baikaldb, default:10 min");
    BRPC_VALIDATE_GFLAG(print_time_us, brpc::NonNegativeInteger);
    DEFINE_int32(fetcher_request_timeout, 100000,
                 "store as server request timeout, default:10000ms");
    DEFINE_int32(fetcher_connect_timeout, 1000,
                 "store as server connect timeout, default:1000ms");
    DEFINE_bool(fetcher_follower_read, true, "where allow follower read for fether");
    DEFINE_bool(fetcher_learner_read, false, "where allow learner read for fether");
    DEFINE_string(insulate_fetcher_resource_tag, "", "store read insulate resource_tag");
    DEFINE_string(fetcher_resource_tag, "", "store read resource_tag perfered, only first time valid");
    DEFINE_bool(use_dynamic_timeout, false, "whether use dynamic_timeout");
    DEFINE_bool(use_read_index, false, "whether use follower read");
    BRPC_VALIDATE_GFLAG(use_dynamic_timeout, brpc::PassValidate);
    DEFINE_bool(open_nonboolean_sql_forbid, false, "open nonboolean sqls forbid default:false");
    /// for execute-plan
    DEFINE_bool(unique_index_default_global, true, "unique_index_default_global");
    DEFINE_bool(normal_index_default_global, false, "normal_index_default_global");
    DEFINE_bool(delete_all_to_truncate, false, "delete from xxx; treat as truncate");
    DEFINE_bool(open_non_where_sql_forbid, false, "open non where conjunct sql forbid switch default:false");
    DEFINE_bool(default_2pc, false, "default enable/disable 2pc for autocommit queries");
    DEFINE_int32(cmsketch_depth, 5, "cmsketch_depth");
    DEFINE_int32(cmsketch_width, 2048, "cmsketch_width");
    DEFINE_int32(sample_rows, 1000000, "sample rows 100w");
    /// for execute-runtime
    DEFINE_int32(per_txn_max_num_locks, 1000000, "max num locks per txn default 100w");
    DEFINE_int64(row_number_to_check_memory, 4096, "do memory limit when row number more than #, default: 4096");
    DEFINE_int32(time_length_to_delete_message, 1,
                 "hours length to delete mem_row_descriptor of sql : default one hour");
    DEFINE_bool(limit_unappropriate_sql, false, "limit concurrency as one when select sql is unappropriate");

    /// for raft
    DEFINE_int64(snapshot_timeout_min, 10, "snapshot_timeout_min : 10min");

    /// for index
    DEFINE_string(q2b_utf8_path, "./conf/q2b_utf8.dic", "q2b_utf8_path");
    DEFINE_string(q2b_gbk_path, "./conf/q2b_gbk.dic", "q2b_gbk_path");
    DEFINE_string(punctuation_path, "./conf/punctuation.dic", "punctuation_path");
    DEFINE_bool(reverse_print_log, false, "reverse_print_log");
    DEFINE_bool(enable_print_convert_log, false, "enable_print_convert_log");

    /// for query
    DEFINE_string(log_plat_name, "test", "plat name for print log, distinguish monitor");

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

    /// for config
    DEFINE_int64(config_time_between_connect_error_ms, 0, "time_between_meta_connect_error_ms. default(0ms)");
    DEFINE_string(config_server_bns, "127.0.0.1:8020", "config server bns");
    DEFINE_string(config_backup_server_bns, "", "config_backup_server_bns");
    DEFINE_int32(config_request_timeout, 30000, "config as server request timeout, default:30000ms");
    DEFINE_int32(config_connect_timeout, 5000, "config as server connect timeout, default:5000ms");
    DEFINE_int32(config_snapshot_interval_s, 600, "raft snapshot interval(s)");
    DEFINE_int32(config_election_timeout_ms, 1000, "raft election timeout(ms)");
    DEFINE_string(config_log_uri, "myraftlog://config_raft_log?id=", "raft log uri");
    DEFINE_string(config_stable_uri, "local://./raft_data/config/stable", "raft stable path");
    DEFINE_string(config_snapshot_uri, "local://./raft_data/config/snapshot", "raft snapshot path");
    DEFINE_int64(config_check_migrate_interval_us, 60 * 1000 * 1000LL, "check config server migrate interval (60s)");
    DEFINE_string(config_db_path, "./rocks_db/config", "rocks db path");
    DEFINE_string(config_snapshot_sst, "/config.sst","rocks sst file for config");
    DEFINE_string(config_listen,"127.0.0.1:8020", "config listen addr");
    DEFINE_int32(config_replica_number, 1, "config service replica num");

    /// for plugin
    DEFINE_int64(plugin_time_between_connect_error_ms, 0, "time_between_meta_connect_error_ms. default(0ms)");
    DEFINE_string(plugin_server_bns, "127.0.0.1:8030", "plugin server bns");
    DEFINE_string(plugin_backup_server_bns, "", "plugin_backup_server_bns");
    DEFINE_int32(plugin_request_timeout, 30000, "service as server request timeout, default:30000ms");
    DEFINE_int32(plugin_connect_timeout, 5000, "service as server connect timeout, default:5000ms");
    DEFINE_int32(plugin_snapshot_interval_s, 600, "raft snapshot interval(s)");
    DEFINE_int32(plugin_election_timeout_ms, 1000, "raft election timeout(ms)");
    DEFINE_string(plugin_log_uri, "myraftlog://plugin_raft_log?id=", "raft log uri");
    DEFINE_string(plugin_stable_uri, "local://./raft_data/plugin/stable", "raft stable path");
    DEFINE_string(plugin_snapshot_uri, "local://./raft_data/plugin/snapshot", "raft snapshot path");
    DEFINE_int64(plugin_check_migrate_interval_us, 60 * 1000 * 1000LL, "check plugin server migrate interval (60s)");
    DEFINE_string(plugin_plugin_data_root,"./plugin_data/plugin","plugin data dir");
    DEFINE_string(plugin_db_path, "./rocks_db/plugin", "rocks db path");
    DEFINE_string(plugin_snapshot_sst, "/plugin_meta.sst","rocks sst file for service");
    DEFINE_string(plugin_listen,"127.0.0.1:8030", "plugin listen addr");
    DEFINE_int32(plugin_replica_number, 1, "plugin service replica num");
    /// for router
    DEFINE_string(router_listen, "0.0.0.0:8888", "router default ip port");

}  // namespace EA