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


#ifndef ELASTICANN_FLAGS_EXECUTE_H_
#define ELASTICANN_FLAGS_EXECUTE_H_
#include "gflags/gflags_declare.h"
namespace EA {

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

    DECLARE_string(log_plat_name);

}  // namespace EA

#endif  // ELASTICANN_FLAGS_EXECUTE_H_
