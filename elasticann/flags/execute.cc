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
#include "elasticann/flags/execute.h"
#include "gflags/gflags.h"
#include "brpc/reloadable_flags.h"

namespace EA {

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

    DEFINE_string(log_plat_name, "test", "plat name for print log, distinguish monitor");
}  // namespace EA
