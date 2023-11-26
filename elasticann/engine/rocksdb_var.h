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


#ifndef ELASTICANN_ENGINE_ROCKSDB_VAR_H_
#define ELASTICANN_ENGINE_ROCKSDB_VAR_H_

#include "bvar/bvar.h"

namespace EA {
    struct RocksdbVars {
        static RocksdbVars *get_instance() {
            static RocksdbVars _instance;
            return &_instance;
        }

        bvar::IntRecorder rocksdb_put_time;
        bvar::Adder<int64_t> rocksdb_put_count;
        bvar::Window<bvar::IntRecorder> rocksdb_put_time_cost_latency;
        bvar::PerSecond<bvar::Adder<int64_t> > rocksdb_put_time_cost_qps;
        bvar::IntRecorder rocksdb_get_time;
        bvar::Adder<int64_t> rocksdb_get_count;
        bvar::Window<bvar::IntRecorder> rocksdb_get_time_cost_latency;
        bvar::PerSecond<bvar::Adder<int64_t> > rocksdb_get_time_cost_qps;
        bvar::IntRecorder rocksdb_multiget_time;
        bvar::Adder<int64_t> rocksdb_multiget_count;
        bvar::Window<bvar::IntRecorder> rocksdb_multiget_time_cost_latency;
        bvar::PerSecond<bvar::Adder<int64_t> > rocksdb_multiget_time_cost_qps;
        bvar::IntRecorder rocksdb_scan_time;
        bvar::Adder<int64_t> rocksdb_scan_count;
        bvar::Window<bvar::IntRecorder> rocksdb_scan_time_cost_latency;
        bvar::PerSecond<bvar::Adder<int64_t> > rocksdb_scan_time_cost_qps;
        bvar::IntRecorder rocksdb_seek_time;
        bvar::Adder<int64_t> rocksdb_seek_count;
        bvar::Window<bvar::IntRecorder> rocksdb_seek_time_cost_latency;
        bvar::PerSecond<bvar::Adder<int64_t> > rocksdb_seek_time_cost_qps;
        bvar::LatencyRecorder qos_fetch_tokens_wait_time_cost;
        bvar::Adder<int64_t> qos_fetch_tokens_wait_count;
        bvar::Adder<int64_t> qos_fetch_tokens_count;
        bvar::PerSecond<bvar::Adder<int64_t> > qos_fetch_tokens_qps;
        bvar::Adder<int64_t> qos_token_waste_count;
        bvar::PerSecond<bvar::Adder<int64_t> > qos_token_waste_qps;
        // 统计未提交的binlog最大时间
        bvar::Maxer<int64_t> binlog_not_commit_max_cost;
        bvar::Window<bvar::Maxer<int64_t>> binlog_not_commit_max_cost_minute;

    private:
        RocksdbVars() : rocksdb_put_time_cost_latency("rocksdb_put_time_cost_latency", &rocksdb_put_time, -1),
                        rocksdb_put_time_cost_qps("rocksdb_put_time_cost_qps", &rocksdb_put_count),
                        rocksdb_get_time_cost_latency("rocksdb_get_time_cost_latency", &rocksdb_get_time, -1),
                        rocksdb_get_time_cost_qps("rocksdb_get_time_cost_qps", &rocksdb_get_count),
                        rocksdb_multiget_time_cost_latency("rocksdb_multiget_time_cost_latency", &rocksdb_multiget_time,
                                                           -1),
                        rocksdb_multiget_time_cost_qps("rocksdb_multiget_time_cost_qps", &rocksdb_multiget_count),
                        rocksdb_scan_time_cost_latency("rocksdb_scan_time_cost_latency", &rocksdb_scan_time, -1),
                        rocksdb_scan_time_cost_qps("rocksdb_scan_time_cost_qps", &rocksdb_scan_count),
                        rocksdb_seek_time_cost_latency("rocksdb_seek_time_cost_latency", &rocksdb_seek_time, -1),
                        rocksdb_seek_time_cost_qps("rocksdb_seek_time_cost_qps", &rocksdb_seek_count),
                        qos_fetch_tokens_wait_time_cost("qos_fetch_tokens_wait_time_cost"),
                        qos_fetch_tokens_wait_count("qos_fetch_tokens_wait_count"),
                        qos_fetch_tokens_qps("qos_fetch_tokens_qps", &qos_fetch_tokens_count),
                        qos_token_waste_qps("qos_token_waste_qps", &qos_token_waste_count),
                        binlog_not_commit_max_cost_minute("binlog_not_commit_max_cost_minute",
                                                          &binlog_not_commit_max_cost, 60) {
        }
    };

}  // namespace EA

#endif  // ELASTICANN_ENGINE_ROCKSDB_VAR_H_
