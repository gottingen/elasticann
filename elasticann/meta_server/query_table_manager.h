// Copyright 2023 The Turbo Authors.
// Copyright (c) 2018-present Baidu, Inc. All Rights Reserved.
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


#pragma once

#include "elasticann/meta_server/table_manager.h"

namespace EA {
    class QueryTableManager {
    public:
        ~QueryTableManager() {}

        static QueryTableManager *get_instance() {
            static QueryTableManager instance;
            return &instance;
        }

        void get_schema_info(const proto::QueryRequest *request, proto::QueryResponse *response);

        void get_flatten_schema(const proto::QueryRequest *request, proto::QueryResponse *response);

        void get_flatten_table(const proto::QueryRequest *request, proto::QueryResponse *response);

        void get_primary_key_string(int64_t table_id, std::string &primary_key_string);

        void decode_key(int64_t table_id, const TableKey &start_key, std::string &start_key_string);

        proto::PrimitiveType get_field_type(int64_t table_id,
                                            int32_t field_id,
                                            const proto::SchemaInfo &table_info);

        void process_console_heartbeat(const proto::ConsoleHeartBeatRequest *request,
                                       proto::ConsoleHeartBeatResponse *response, uint64_t log_id);

        void get_ddlwork_info(const proto::QueryRequest *request, proto::QueryResponse *response);

        void get_virtual_index_influence_info(const proto::QueryRequest *request, proto::QueryResponse *response);

        void get_table_in_fast_importer(const proto::QueryRequest *request, proto::QueryResponse *response);

        void clean_cache() {
            BAIDU_SCOPED_LOCK(_mutex);
            _table_info_cache_time.clear();
            _table_infos_cache.clear();
        }

    private:
        QueryTableManager() {}

        void check_table_and_update(
                const std::unordered_map<int64_t, std::tuple<proto::SchemaInfo, int64_t, int64_t>> table_schema_map,
                std::unordered_map<int64_t, int64_t> &report_table_map,
                proto::ConsoleHeartBeatResponse *response, uint64_t log_id);

        void construct_query_table(const TableMem &table, proto::QueryTable *query_table);

        // 由于show table status太重了，进行cache
        bthread::Mutex _mutex;
        std::unordered_map<std::string, int64_t> _table_info_cache_time;
        // db -> table -> table info
        std::unordered_map<std::string, std::map<std::string, proto::QueryTable>> _table_infos_cache;
    }; //class

}  // namespace EA
