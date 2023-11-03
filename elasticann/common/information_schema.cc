// Copyright 2023 The Elastic AI Search Authors.
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


#include "elasticann/common/information_schema.h"
#include "elasticann/runtime/runtime_state.h"
#include "elasticann/common/meta_server_interact.h"
#include "elasticann/common/store_interact.h"
#include "elasticann/common/schema_factory.h"
#include "elasticann/session/network_socket.h"
#include "elasticann/expr/scalar_fn_call.h"
#include "elasticann/sqlparser/parser.h"
#include "turbo/format/format.h"
#include "turbo/strings/str_split.h"

namespace EA {
    int InformationSchema::init() {
        init_partition_split_info();
        init_region_status();
        init_learner_region_status();
        init_invalid_learner_region();
        init_columns();
        init_statistics();
        init_schemata();
        init_tables();
        init_virtual_index_influence_info();
        init_sign_list();
        init_routines();
        init_key_column_usage();
        init_referential_constraints();
        init_triggers();
        init_views();
        init_character_sets();
        init_collation_character_set_applicability();
        init_collations();
        init_column_privileges();
        init_engines();
        init_events();
        init_files();
        init_global_status();
        init_global_variables();
        init_innodb_buffer_page();
        init_innodb_buffer_page_lru();
        init_innodb_buffer_pool_stats();
        init_innodb_cmp();
        init_innodb_cmpmem();
        init_innodb_cmpmem_reset();
        init_innodb_cmp_per_index();
        init_innodb_cmp_per_index_reset();
        init_innodb_cmp_reset();
        init_innodb_ft_being_deleted();
        init_innodb_ft_config();
        init_innodb_ft_default_stopword();
        init_innodb_ft_deleted();
        init_innodb_ft_index_cache();
        init_innodb_ft_index_table();
        init_innodb_locks();
        init_innodb_lock_waits();
        init_innodb_metrics();
        init_innodb_sys_columns();
        init_innodb_sys_datafiles();
        init_innodb_sys_fields();
        init_innodb_sys_foreign();
        init_innodb_sys_foreign_cols();
        init_innodb_sys_indexes();
        init_innodb_sys_tables();
        init_innodb_sys_tablespaces();
        init_innodb_sys_tablestats();
        init_innodb_trx();
        init_optimizer_trace();
        init_parameters();
        init_partitions();
        init_plugins();
        init_processlist();
        init_profiling();
        init_schema_privileges();
        init_session_status();
        init_session_variables();
        init_table_constraints();
        init_table_privileges();
        init_tablespaces();
        init_user_privileges();
        init_binlog_region_infos();
        return 0;
    }

    int64_t InformationSchema::construct_table(const std::string &table_name, FieldVec &fields) {
        auto &table = _tables[table_name];//_tables[table_name]取出的是Schema_info
        table.set_table_id(--_max_table_id);
        table.set_table_name(table_name);
        table.set_database("information_schema");
        table.set_database_id(_db_id);
        table.set_namespace_name("INTERNAL");
        table.set_engine(proto::INFORMATION_SCHEMA);
        int id = 0;
        for (auto &pair: fields) {
            auto *field = table.add_fields();
            field->set_field_name(pair.first);
            field->set_mysql_type(pair.second);
            field->set_field_id(++id);
        }
        SchemaFactory::get_instance()->update_table(table);
        return table.table_id();
    }

    void InformationSchema::init_partition_split_info() {
        // 定义字段信息
        FieldVec fields{
                {"partition_key", proto::STRING},
                {"table_name",    proto::STRING},
                {"split_info",    proto::STRING},
                {"split_rows",    proto::STRING},
        };
        int64_t table_id = construct_table("PARTITION_SPLIT_INFO", fields);
        // 定义操作
        _calls[table_id] = [table_id](RuntimeState *state, std::vector<ExprNode *> &conditions) ->
                std::vector<SmartRecord> {
            std::vector<SmartRecord> records;
            if (state->client_conn() == nullptr) {
                return records;
            }
            std::string namespace_ = state->client_conn()->user_info->namespace_;
            std::string table_name;
            for (auto expr: conditions) {
                if (expr->node_type() != proto::FUNCTION_CALL) {
                    continue;
                }
                int32_t fn_op = static_cast<ScalarFnCall *>(expr)->fn().fn_op();
                if (fn_op != parser::FT_EQ) {
                    continue;
                }
                if (!expr->children(0)->is_slot_ref()) {
                    continue;
                }
                SlotRef *slot_ref = static_cast<SlotRef *>(expr->children(0));
                int32_t field_id = slot_ref->field_id();
                if (field_id != 2) {
                    continue;
                }
                if (expr->children(1)->is_constant()) {
                    table_name = expr->children(1)->get_value(nullptr).get_string();
                }
            }
            if (table_name.empty()) {
                return records;
            }
            auto *factory = SchemaFactory::get_instance();
            int64_t condition_table_id = 0;
            if (factory->get_table_id(namespace_ + "." + table_name, condition_table_id) != 0) {
                return records;
            }
            auto index_ptr = factory->get_index_info_ptr(condition_table_id);
            if (index_ptr == nullptr) {
                return records;
            }
            if (index_ptr->fields.size() < 2) {
                return records;
            }
            std::map<std::string, proto::RegionInfo> region_infos;
            factory->get_all_region_by_table_id(condition_table_id, &region_infos);
            std::string last_partition_key;
            std::vector<std::string> last_keys;
            std::vector<int64_t> last_region_ids;
            last_keys.reserve(3);
            last_region_ids.reserve(3);
            int64_t last_id = 0;
            std::string partition_key;
            auto type1 = index_ptr->fields[0].type;
            auto type2 = index_ptr->fields[1].type;
            records.reserve(10000);
            std::vector<std::vector<int64_t>> region_ids;
            region_ids.reserve(10000);
            proto::QueryRequest req;
            proto::QueryResponse res;
            req.set_op_type(proto::QUERY_REGION);
            for (auto &pair: region_infos) {
                TableKey start_key(pair.second.start_key());
                int pos = 0;
                partition_key = start_key.decode_start_key_string(type1, pos);
                if (partition_key != last_partition_key) {
                    if (last_keys.size() > 1) {
                        for (auto id: last_region_ids) {
                            req.add_region_ids(id);
                        }
                        region_ids.emplace_back(last_region_ids);
                        auto record = factory->new_record(table_id);
                        record->set_string(record->get_field_by_name("partition_key"), last_partition_key);
                        record->set_string(record->get_field_by_name("table_name"), table_name);
                        record->set_string(record->get_field_by_name("split_info"),
                                           turbo::FormatRange("{}", last_keys, ","));
                        //record->set_string(record->get_field_by_name("split_rows"), turbo::FormatRange("{}",rows, ","));
                        records.emplace_back(record);
                    }
                    last_partition_key = partition_key;
                    last_keys.clear();
                    last_region_ids.clear();
                    last_region_ids.emplace_back(last_id);
                }
                last_keys.emplace_back(start_key.decode_start_key_string(type2, pos));
                last_region_ids.emplace_back(pair.second.region_id());
                last_id = pair.second.region_id();
            }
            if (last_keys.size() > 1) {
                for (auto id: last_region_ids) {
                    TURBO_UNUSED(id);
                    req.set_op_type(proto::QUERY_REGION);
                }
                region_ids.emplace_back(last_region_ids);
                auto record = factory->new_record(table_id);
                record->set_string(record->get_field_by_name("partition_key"), last_partition_key);
                record->set_string(record->get_field_by_name("table_name"), table_name);
                record->set_string(record->get_field_by_name("split_info"), turbo::FormatRange("{}", last_keys, ","));
                //record->set_string(record->get_field_by_name("split_rows"), turbo::FormatRange("{}", rows, ","));
                records.emplace_back(record);
            }
            MetaServerInteract::get_instance()->send_request("query", req, res);
            std::unordered_map<int64_t, std::string> region_lines;
            for (auto &info: res.region_infos()) {
                region_lines[info.region_id()] = std::to_string(info.num_table_lines());
            }
            for (uint32_t i = 0; i < records.size(); i++) {
                std::vector<std::string> rows;
                rows.reserve(3);
                if (i < region_ids.size()) {
                    for (auto &id: region_ids[i]) {
                        rows.emplace_back(region_lines[id]);
                    }
                }
                records[i]->set_string(records[i]->get_field_by_name("split_rows"),
                                       turbo::FormatRange("{}", rows, ","));
            }
            return records;
        };
    }

    void InformationSchema::init_region_status() {
        // 定义字段信息
        FieldVec fields{
                {"region_id",       proto::INT64},
                {"parent",          proto::INT64},
                {"table_id",        proto::INT64},
                {"main_table_id",   proto::INT64},
                {"table_name",      proto::STRING},
                {"start_key",       proto::STRING},
                {"end_key",         proto::STRING},
                {"create_time",     proto::STRING},
                {"peers",           proto::STRING},
                {"leader",          proto::STRING},
                {"version",         proto::INT64},
                {"conf_version",    proto::INT64},
                {"num_table_lines", proto::INT64},
                {"used_size",       proto::INT64},
        };
        int64_t table_id = construct_table("REGION_STATUS", fields);
        // 定义操作
        _calls[table_id] = [table_id](RuntimeState *state, std::vector<ExprNode *> &conditions) ->
                std::vector<SmartRecord> {
            std::vector<SmartRecord> records;
            if (state->client_conn() == nullptr) {
                return records;
            }
            std::string namespace_ = state->client_conn()->user_info->namespace_;
            std::string table_name;
            for (auto expr: conditions) {
                if (expr->node_type() != proto::FUNCTION_CALL) {
                    continue;
                }
                int32_t fn_op = static_cast<ScalarFnCall *>(expr)->fn().fn_op();
                if (fn_op != parser::FT_EQ) {
                    continue;
                }
                if (!expr->children(0)->is_slot_ref()) {
                    continue;
                }
                SlotRef *slot_ref = static_cast<SlotRef *>(expr->children(0));
                int32_t field_id = slot_ref->field_id();
                if (field_id != 5) {
                    continue;
                }
                if (expr->children(1)->is_constant()) {
                    table_name = expr->children(1)->get_value(nullptr).get_string();
                }
            }
            if (table_name.empty()) {
                return records;
            }
            auto *factory = SchemaFactory::get_instance();
            int64_t condition_table_id = 0;
            if (factory->get_table_id(namespace_ + "." + table_name, condition_table_id) != 0) {
                return records;
            }
            auto index_ptr = factory->get_index_info_ptr(condition_table_id);
            if (index_ptr == nullptr) {
                return records;
            }
            std::map<std::string, proto::RegionInfo> region_infos;
            factory->get_all_region_by_table_id(condition_table_id, &region_infos);
            records.reserve(region_infos.size());
            for (auto &pair: region_infos) {
                auto &region = pair.second;
                TableKey start_key(region.start_key());
                TableKey end_key(region.end_key());
                auto record = factory->new_record(table_id);
                record->set_int64(record->get_field_by_name("region_id"), region.region_id());
                record->set_int64(record->get_field_by_name("parent"), region.parent());
                record->set_int64(record->get_field_by_name("table_id"), region.table_id());
                record->set_int64(record->get_field_by_name("main_table_id"), region.main_table_id());
                record->set_string(record->get_field_by_name("table_name"), table_name);
                record->set_int64(record->get_field_by_name("version"), region.version());
                record->set_int64(record->get_field_by_name("conf_version"), region.conf_version());
                record->set_int64(record->get_field_by_name("num_table_lines"), region.num_table_lines());
                record->set_int64(record->get_field_by_name("used_size"), region.used_size());
                record->set_string(record->get_field_by_name("leader"), region.leader());
                record->set_string(record->get_field_by_name("peers"), turbo::FormatRange("{}", region.peers(), ","));
                time_t t = region.timestamp();
                struct tm t_result;
                localtime_r(&t, &t_result);
                char s[100];
                strftime(s, sizeof(s), "%F %T", &t_result);
                record->set_string(record->get_field_by_name("create_time"), s);
                record->set_string(record->get_field_by_name("start_key"),
                                   start_key.decode_start_key_string(*index_ptr));
                record->set_string(record->get_field_by_name("end_key"),
                                   end_key.decode_start_key_string(*index_ptr));
                records.emplace_back(record);
            }
            return records;
        };
    }

    void InformationSchema::init_binlog_region_infos() {
        // 定义字段信息
        FieldVec fields{
                {"table_id",                  proto::INT64},
                {"partition_id",              proto::INT64},
                {"region_id",                 proto::INT64},
                {"instance_ip",               proto::STRING},
                {"table_name",                proto::STRING},
                {"check_point_datetime",      proto::STRING},
                {"max_oldest_datetime",       proto::STRING},
                {"region_oldest_datetime",    proto::STRING},
                {"binlog_cf_oldest_datetime", proto::STRING},
                {"data_cf_oldest_datetime",   proto::STRING},
        };
        int64_t table_id = construct_table("BINLOG_REGION_INFOS", fields);

        _calls[table_id] = [table_id, this](RuntimeState *state, std::vector<ExprNode *> &conditions) ->
                std::vector<SmartRecord> {
            std::vector<SmartRecord> records;
            if (state->client_conn() == nullptr) {
                return records;
            }

            std::string binlog_table_name;
            int64_t input_partition_id = -1;
            for (auto expr: conditions) {
                if (expr->node_type() != proto::FUNCTION_CALL) {
                    continue;
                }
                int32_t fn_op = static_cast<ScalarFnCall *>(expr)->fn().fn_op();
                if (fn_op != parser::FT_EQ) {
                    continue;
                }
                if (!expr->children(0)->is_slot_ref()) {
                    continue;
                }
                SlotRef *slot_ref = static_cast<SlotRef *>(expr->children(0));
                int32_t field_id = slot_ref->field_id();
                if (field_id != 5 && field_id != 2) {
                    continue;
                }
                if (expr->children(1)->is_constant()) {
                    if (field_id == 5) {
                        binlog_table_name = expr->children(1)->get_value(nullptr).get_string();
                    } else if (field_id == 2) {
                        input_partition_id = strtoll(expr->children(1)->get_value(nullptr).get_string().c_str(),
                                                     nullptr,
                                                     10);
                    }
                }
            }

            std::unordered_map<int64_t, std::unordered_map<int64_t, std::vector<proto::RegionInfo>>> table_id_partition_binlogs;
            std::unordered_map<int64_t, std::unordered_map<int64_t, std::vector<proto::StoreRes>>> table_id_to_query_info;
            std::vector<std::vector<std::string>> result_rows;
            SchemaFactory::get_instance()->get_partition_binlog_regions(binlog_table_name, input_partition_id,
                                                                        table_id_partition_binlogs);
            query_regions_concurrency(table_id_to_query_info, table_id_partition_binlogs);
            process_binlogs_region_info(result_rows, table_id_to_query_info);
            TLOG_WARN("binlog_table_name: {}, input_partition_id : {}", binlog_table_name,
                       input_partition_id);
            for (const auto &result_row: result_rows) {
                if (result_row.size() != 10) {
                    return records;
                }
                int64_t current_table_id = strtoll(result_row[0].c_str(), nullptr, 10);
                int64_t current_partition_id = strtoll(result_row[1].c_str(), nullptr, 10);
                int64_t current_region_id = strtoll(result_row[2].c_str(), nullptr, 10);
                auto record = SchemaFactory::get_instance()->new_record(table_id);
                record->set_int64(record->get_field_by_name("table_id"), current_table_id);
                record->set_int64(record->get_field_by_name("partition_id"), current_partition_id);
                record->set_int64(record->get_field_by_name("region_id"), current_region_id);
                record->set_string(record->get_field_by_name("instance_ip"), result_row[3]);
                record->set_string(record->get_field_by_name("table_name"), result_row[4]);
                record->set_string(record->get_field_by_name("check_point_datetime"), result_row[5]);
                record->set_string(record->get_field_by_name("max_oldest_datetime"), result_row[6]);
                record->set_string(record->get_field_by_name("region_oldest_datetime"), result_row[7]);
                record->set_string(record->get_field_by_name("binlog_cf_oldest_datetime"), result_row[8]);
                record->set_string(record->get_field_by_name("data_cf_oldest_datetime"), result_row[9]);
                records.emplace_back(record);
            }
            return records;
        };
    }

    void InformationSchema::query_regions_concurrency(
            std::unordered_map<int64_t, std::unordered_map<int64_t, std::vector<proto::StoreRes>>> &table_id_to_binlog_info,
            std::unordered_map<int64_t, std::unordered_map<int64_t, std::vector<proto::RegionInfo>>> &partition_binlog_region_infos) {
        std::mutex mutex_query_info;
        for (const auto &partition_binlog_regions_info: partition_binlog_region_infos) {
            int64_t current_table_id = partition_binlog_regions_info.first;
            for (const auto &partition_binlog_region_info: partition_binlog_regions_info.second) {
                int64_t current_partition_id = partition_binlog_region_info.first;
                for (const auto &binlog_region_info: partition_binlog_region_info.second) {
                    const int64_t &table_id = binlog_region_info.table_id();
                    const int64_t &region_id = binlog_region_info.region_id();
                    const int64_t &version = binlog_region_info.version();
                    std::vector<std::string> peers_vec;
                    std::string str_peer;
                    peers_vec.reserve(3);
                    for (const auto &peer: binlog_region_info.peers()) {
                        str_peer += peer + ",";
                        peers_vec.emplace_back(peer);
                    }
                    str_peer.pop_back();
                    const std::string &table_name = binlog_region_info.table_name();
                    ConcurrencyBthread bth_each_peer(6);
                    for (auto &peer: peers_vec) {
                        static std::mutex mutex_binlog_ts;
                        auto send_to_binlog_peer = [&]() {
                            brpc::Channel channel;
                            brpc::Controller cntl;
                            brpc::ChannelOptions option;
                            option.max_retry = 1;
                            option.connect_timeout_ms = 30000;
                            option.timeout_ms = 30000;
                            channel.Init(peer.c_str(), &option);
                            proto::StoreReq req;
                            proto::StoreRes res;
                            req.set_region_version(version);
                            req.set_region_id(region_id);
                            req.set_op_type(proto::OP_QUERY_BINLOG);
                            proto::StoreService_Stub(&channel).query_binlog(&cntl, &req, &res, nullptr);
                            if (!cntl.Failed()) {
                                BAIDU_SCOPED_LOCK(mutex_query_info);
                                auto binlog_info = res.mutable_binlog_info();
                                binlog_info->set_region_ip(peer);
                                table_id_to_binlog_info[table_id][region_id].emplace_back(res);
                            }
                        };
                        bth_each_peer.run(send_to_binlog_peer);
                    }
                    bth_each_peer.join();
                }
            }
        }
    }

    void InformationSchema::process_binlogs_region_info(std::vector<std::vector<std::string>> &result_rows,
                                                        std::unordered_map<int64_t,
                                                                std::unordered_map<int64_t, std::vector<proto::StoreRes>>> &table_id_to_query_info) {
        for (const auto &region_id_peers_info: table_id_to_query_info) {
            int64_t table_id = region_id_peers_info.first;
            const std::string table_name = SchemaFactory::get_instance()->get_table_info(table_id).name;
            for (const auto &region_id_peer_info: region_id_peers_info.second) {
                int64_t region_id = region_id_peer_info.first;
                proto::RegionInfo region_info_tmp;
                SchemaFactory::get_instance()->get_region_info(table_id, region_id, region_info_tmp);
                int64_t current_partition_id = region_info_tmp.partition_id();
                const std::vector<proto::StoreRes> &pb_peer_info_vec = region_id_peer_info.second;
                for (const auto &binlog_peer_info: pb_peer_info_vec) {
                    const auto &binlog_info = binlog_peer_info.binlog_info();
                    const std::string &instance_ip = binlog_info.region_ip();
                    const std::string check_point_datetime = ts_to_datetime_str(binlog_info.check_point_ts());
                    const std::string oldest_datetime = ts_to_datetime_str(binlog_info.oldest_ts());
                    const std::string region_oldest_datetime = ts_to_datetime_str(binlog_info.region_oldest_ts());
                    const std::string binlog_cf_oldest_datetime = ts_to_datetime_str(binlog_info.binlog_cf_oldest_ts());
                    const std::string data_cf_oldest_datetime = ts_to_datetime_str(binlog_info.data_cf_oldest_ts());
                    std::vector<std::string> row;
                    row.reserve(10);
                    row.emplace_back(std::to_string(table_id));
                    row.emplace_back(std::to_string(current_partition_id));
                    row.emplace_back(std::to_string(region_id));
                    row.emplace_back(instance_ip);
                    row.emplace_back(table_name);
                    row.emplace_back(check_point_datetime);
                    row.emplace_back(oldest_datetime);
                    row.emplace_back(region_oldest_datetime);
                    row.emplace_back(binlog_cf_oldest_datetime);
                    row.emplace_back(data_cf_oldest_datetime);
                    result_rows.emplace_back(row);
                }
            }
        }
        //泛型排序，让展示结果有序
        std::sort(result_rows.begin(), result_rows.end(),
                  [](const std::vector<std::string> &a, const std::vector<std::string> &b) {
                      const std::string str_prefix_a = a[0] + a[2];
                      const std::string str_prefix_b = b[0] + b[2];
                      errno = 0;
                      int64_t value_prefix_a = strtoll(str_prefix_a.c_str(), nullptr, 10);
                      int64_t value_prefix_b = strtoll(str_prefix_b.c_str(), nullptr, 10);
                      return value_prefix_a < value_prefix_b;
                  });
    }

    void InformationSchema::init_learner_region_status() {
        // 定义字段信息
        FieldVec fields{
                {"database_name", proto::STRING},
                {"table_name",    proto::STRING},
                {"region_id",     proto::INT64},
                {"partition_id",  proto::INT64},
                {"resource_tag",  proto::STRING},
                {"instance",      proto::STRING},
                {"version",       proto::INT64},
                {"apply_index",   proto::INT64},
                {"status",        proto::STRING},
        };
        int64_t table_id = construct_table("LEARNER_REGION_STATUS", fields);
        // 定义操作
        _calls[table_id] = [table_id](RuntimeState *state, std::vector<ExprNode *> &conditions) ->
                std::vector<SmartRecord> {
            std::vector<SmartRecord> records;
            if (state->client_conn() == nullptr) {
                return records;
            }
            std::string namespace_ = state->client_conn()->user_info->namespace_;
            std::string database_name;
            std::string table_name;
            for (auto expr: conditions) {
                if (expr->node_type() != proto::FUNCTION_CALL) {
                    continue;
                }
                int32_t fn_op = static_cast<ScalarFnCall *>(expr)->fn().fn_op();
                if (fn_op != parser::FT_EQ) {
                    continue;
                }
                if (!expr->children(0)->is_slot_ref()) {
                    continue;
                }
                SlotRef *slot_ref = static_cast<SlotRef *>(expr->children(0));
                int32_t field_id = slot_ref->field_id();
                if (field_id != 1 && field_id != 2) {
                    continue;
                }
                if (expr->children(1)->is_constant()) {
                    if (field_id == 1) {
                        database_name = expr->children(1)->get_value(nullptr).get_string();
                    } else if (field_id == 2) {
                        table_name = expr->children(1)->get_value(nullptr).get_string();
                    }
                }
            }
            TLOG_WARN("database_name: {}, table_name: {}", database_name, table_name);
            auto *factory = SchemaFactory::get_instance();
            std::vector<int64_t> condition_table_ids;
            std::map<int64_t, std::string> condition_table_id_db_map;
            std::map<int64_t, std::string> condition_table_id_tbl_map;
            if (!database_name.empty() && !table_name.empty()) {
                int64_t condition_table_id = 0;
                if (factory->get_table_id(namespace_ + "." + database_name + "." + table_name, condition_table_id) !=
                    0) {
                    return records;
                }
                condition_table_ids.emplace_back(condition_table_id);
                condition_table_id_db_map[condition_table_id] = database_name;
                condition_table_id_tbl_map[condition_table_id] = table_name;
            } else {
                auto func = [&condition_table_ids, &condition_table_id_db_map, &condition_table_id_tbl_map, &database_name, &table_name]
                        (const SmartTable &table) -> bool {
                    if (table != nullptr && !table->learner_resource_tags.empty()) {
                        if (database_name.empty()) {
                            condition_table_ids.emplace_back(table->id);
                            std::vector<std::string> items = turbo::StrSplit(table->name, turbo::ByAnyChar("."));
                            condition_table_id_db_map[table->id] = items[0];
                            condition_table_id_tbl_map[table->id] = table->short_name;
                        } else {
                            if ((database_name + "." + table->short_name) == table->name) {
                                condition_table_ids.emplace_back(table->id);
                                condition_table_id_db_map[table->id] = database_name;
                                condition_table_id_tbl_map[table->id] = table->short_name;
                            }
                        }
                    }
                    return false;
                };
                std::vector<std::string> database_table;
                factory->get_table_by_filter(database_table, func);
            }

            std::map<int64_t, int64_t> region_id_partition_id_map;
            std::map<int64_t, int64_t> region_id_table_id_map;
            std::map<std::string, std::set<int64_t>> instance_region_ids_map;
            records.reserve(1000);
            for (int64_t condition_table_id: condition_table_ids) {
                std::map<int64_t, proto::RegionInfo> region_infos;
                factory->get_all_partition_regions(condition_table_id, &region_infos);
                for (const auto &pair: region_infos) {
                    auto &region = pair.second;
                    region_id_partition_id_map[region.region_id()] = region.partition_id();
                    region_id_table_id_map[region.region_id()] = condition_table_id;
                    for (const auto &peer: region.peers()) {
                        instance_region_ids_map[peer].insert(region.region_id());
                    }
                    for (const auto &learner: region.learners()) {
                        instance_region_ids_map[learner].insert(region.region_id());
                    }
                }
            }

            ConcurrencyBthread bth(instance_region_ids_map.size());
            bthread::Mutex lock;
            for (const auto &pair: instance_region_ids_map) {
                std::string store_addr = pair.first;
                if (pair.second.empty()) {
                    continue;
                }
                std::set<int64_t> region_ids = pair.second;
                auto func = [store_addr, region_ids, table_id, &condition_table_id_db_map, &condition_table_id_tbl_map,
                        &region_id_table_id_map, &region_id_partition_id_map, &records, &lock]() {
                    proto::RegionIds req;
                    proto::StoreRes res;
                    req.set_query_apply_index(true);
                    for (int64_t region_id: region_ids) {
                        req.add_region_ids(region_id);
                    }
                    StoreInteract interact(store_addr);
                    interact.send_request("query_region", req, res);
                    TLOG_WARN("store_addr: {}, req_size: {}, res_size: {}", store_addr, req.region_ids_size(),
                               res.extra_res().infos_size());
                    std::lock_guard<bthread::Mutex> l(lock);
                    for (const auto &info: res.extra_res().infos()) {
                        auto record = SchemaFactory::get_instance()->new_record(table_id);
                        record->set_string(record->get_field_by_name("database_name"),
                                           condition_table_id_db_map[region_id_table_id_map[info.region_id()]]);
                        record->set_string(record->get_field_by_name("table_name"),
                                           condition_table_id_tbl_map[region_id_table_id_map[info.region_id()]]);
                        record->set_int64(record->get_field_by_name("region_id"), info.region_id());
                        record->set_int64(record->get_field_by_name("partition_id"),
                                          region_id_partition_id_map[info.region_id()]);
                        record->set_string(record->get_field_by_name("resource_tag"), info.resource_tag());
                        record->set_string(record->get_field_by_name("instance"), store_addr);
                        record->set_int64(record->get_field_by_name("version"), info.version());
                        record->set_int64(record->get_field_by_name("apply_index"), info.apply_index());
                        record->set_string(record->get_field_by_name("status"), info.status());
                        records.emplace_back(record);
                    }

                };
                bth.run(func);
            }
            bth.join();
            return records;
        };
    }


    void InformationSchema::init_invalid_learner_region() {
        // 定义字段信息
        FieldVec fields{
                {"database_name", proto::STRING},
                {"table_name",    proto::STRING},
                {"region_id",     proto::INT64},
                {"partition_id",  proto::INT64},
                {"resource_tag",  proto::STRING},
                {"instance",      proto::STRING},
                {"version",       proto::INT64},
                {"apply_index",   proto::INT64},
                {"status",        proto::STRING},
        };
        int64_t table_id = construct_table("INVALID_LEARNER_REGION", fields);
        // 定义操作
        _calls[table_id] = [table_id](RuntimeState *state, std::vector<ExprNode *> &conditions) ->
                std::vector<SmartRecord> {
            std::vector<SmartRecord> records;
            if (state->client_conn() == nullptr) {
                return records;
            }

            records.reserve(1000);
            SchemaFactory *factory = SchemaFactory::get_instance();
            std::unordered_map<std::string, InstanceDBStatus> instance_info_map;
            factory->get_all_instance_status(&instance_info_map);

            ConcurrencyBthread bth(instance_info_map.size());
            bthread::Mutex lock;
            for (const auto &pair: instance_info_map) {
                std::string store_addr = pair.first;
                if (pair.second.status == proto::DEAD || pair.second.status == proto::FAULTY) {
                    continue;
                }

                auto func = [store_addr, table_id, &records, &lock]() {
                    proto::RegionIds req;
                    proto::StoreRes res;
                    req.set_query_apply_index(true);
                    StoreInteract interact(store_addr);
                    interact.send_request("query_region", req, res);
                    TLOG_WARN("store_addr: {}, req_size: {}, res_size: {}", store_addr, req.region_ids_size(),
                               res.extra_res().infos_size());
                    std::lock_guard<bthread::Mutex> l(lock);
                    for (const auto &info: res.extra_res().infos()) {
                        std::string database_name;
                        std::string table_name;
                        std::string status = info.status();
                        auto factory = SchemaFactory::get_instance();
                        SmartTable table = factory->get_table_info_ptr(info.table_id());
                        if (table == nullptr) {
                            status = "NOT FOUND TABLE";
                        } else {
                            std::vector<std::string> items = turbo::StrSplit(table->name, turbo::ByAnyChar("."));
                            database_name = items[0];
                            table_name = items[1];
                        }

                        proto::RegionInfo region_info;
                        int ret = factory->get_region_info(info.table_id(), info.region_id(), region_info);
                        if (ret < 0) {
                            status = "NOT FOUND REGION";
                        } else {
                            bool find = false;
                            for (const auto &address: region_info.learners()) {
                                if (address == store_addr) {
                                    find = true;
                                    break;
                                }
                            }

                            if (find) {
                                continue;
                            } else {
                                status = "NOT FOUND LEARNER";
                            }
                        }

                        auto record = factory->new_record(table_id);
                        record->set_string(record->get_field_by_name("database_name"), database_name);
                        record->set_string(record->get_field_by_name("table_name"), table_name);
                        record->set_int64(record->get_field_by_name("region_id"), info.region_id());
                        record->set_int64(record->get_field_by_name("partition_id"), region_info.partition_id());
                        record->set_string(record->get_field_by_name("resource_tag"), info.resource_tag());
                        record->set_string(record->get_field_by_name("instance"), store_addr);
                        record->set_int64(record->get_field_by_name("version"), info.version());
                        record->set_int64(record->get_field_by_name("apply_index"), info.apply_index());
                        record->set_string(record->get_field_by_name("status"), status);
                        records.emplace_back(record);
                    }

                };
                bth.run(func);
            }
            bth.join();
            return records;
        };
    }

    // MYSQL兼容表
    void InformationSchema::init_columns() {
        // 定义字段信息
        FieldVec fields{
                {"TABLE_CATALOG",            proto::STRING},
                {"TABLE_SCHEMA",             proto::STRING},
                {"TABLE_NAME",               proto::STRING},
                {"COLUMN_NAME",              proto::STRING},
                {"ORDINAL_POSITION",         proto::INT64},
                {"COLUMN_DEFAULT",           proto::STRING},
                {"IS_NULLABLE",              proto::STRING},
                {"DATA_TYPE",                proto::STRING},
                {"CHARACTER_MAXIMUM_LENGTH", proto::INT64},
                {"CHARACTER_OCTET_LENGTH",   proto::INT64},
                {"NUMERIC_PRECISION",        proto::INT64},
                {"NUMERIC_SCALE",            proto::INT64},
                {"DATETIME_PRECISION",       proto::INT64},
                {"CHARACTER_SET_NAME",       proto::STRING},
                {"COLLATION_NAME",           proto::STRING},
                {"COLUMN_TYPE",              proto::STRING},
                {"COLUMN_KEY",               proto::STRING},
                {"EXTRA",                    proto::STRING},
                {"PRIVILEGES",               proto::STRING},
                {"COLUMN_COMMENT",           proto::STRING},
        };
        int64_t table_id = construct_table("COLUMNS", fields);
        // 定义操作
        _calls[table_id] = [table_id](RuntimeState *state, std::vector<ExprNode *> &conditions) ->
                std::vector<SmartRecord> {
            std::vector<SmartRecord> records;
            if (state->client_conn() == nullptr) {
                return records;
            }
            std::string namespace_ = state->client_conn()->user_info->namespace_;
            std::string table_name;
            auto *factory = SchemaFactory::get_instance();
            auto tb_vec = factory->get_table_list(namespace_, state->client_conn()->user_info.get());
            records.reserve(tb_vec.size() * 10);
            for (auto &table_info: tb_vec) {
                int i = 0;
                std::vector<std::string> items = turbo::StrSplit(table_info->name, turbo::ByAnyChar("."));
                std::string db = items[0];

                std::multimap<int32_t, IndexInfo> field_index;
                for (auto &index_id: table_info->indices) {
                    IndexInfo index_info = factory->get_index_info(index_id);
                    for (auto &field: index_info.fields) {
                        field_index.insert(std::make_pair(field.id, index_info));
                    }
                }
                for (auto &field: table_info->fields) {
                    if (field.deleted) {
                        continue;
                    }
                    auto record = factory->new_record(table_id);
                    record->set_string(record->get_field_by_name("TABLE_CATALOG"), "def");
                    record->set_string(record->get_field_by_name("TABLE_SCHEMA"), db);
                    record->set_string(record->get_field_by_name("TABLE_NAME"), table_info->short_name);
                    record->set_string(record->get_field_by_name("COLUMN_NAME"), field.short_name);
                    record->set_int64(record->get_field_by_name("ORDINAL_POSITION"), ++i);
                    if (field.default_expr_value.type != proto::NULL_TYPE) {
                        record->set_string(record->get_field_by_name("COLUMN_DEFAULT"), field.default_value);
                    }
                    record->set_string(record->get_field_by_name("IS_NULLABLE"), field.can_null ? "YES" : "NO");
                    record->set_string(record->get_field_by_name("DATA_TYPE"), to_mysql_type_string(field.type));
                    switch (field.type) {
                        case proto::STRING:
                            record->set_int64(record->get_field_by_name("CHARACTER_MAXIMUM_LENGTH"), 1048576);
                            record->set_int64(record->get_field_by_name("CHARACTER_OCTET_LENGTH"), 3145728);
                            break;
                        case proto::INT8:
                        case proto::UINT8:
                            record->set_int64(record->get_field_by_name("NUMERIC_SCALE"), 3);
                            record->set_int64(record->get_field_by_name("NUMERIC_PRECISION"), 0);
                            break;
                        case proto::INT16:
                        case proto::UINT16:
                            record->set_int64(record->get_field_by_name("NUMERIC_SCALE"), 5);
                            record->set_int64(record->get_field_by_name("NUMERIC_PRECISION"), 0);
                            break;
                        case proto::INT32:
                        case proto::UINT32:
                            record->set_int64(record->get_field_by_name("NUMERIC_SCALE"), 10);
                            record->set_int64(record->get_field_by_name("NUMERIC_PRECISION"), 0);
                            break;
                        case proto::INT64:
                            record->set_int64(record->get_field_by_name("NUMERIC_SCALE"), 19);
                            record->set_int64(record->get_field_by_name("NUMERIC_PRECISION"), 0);
                            break;
                        case proto::UINT64:
                            record->set_int64(record->get_field_by_name("NUMERIC_SCALE"), 20);
                            record->set_int64(record->get_field_by_name("NUMERIC_PRECISION"), 0);
                            break;
                        case proto::FLOAT:
                            record->set_int64(record->get_field_by_name("NUMERIC_SCALE"), 38);
                            record->set_int64(record->get_field_by_name("NUMERIC_PRECISION"), 6);
                            break;
                        case proto::DOUBLE:
                            record->set_int64(record->get_field_by_name("NUMERIC_SCALE"), 308);
                            record->set_int64(record->get_field_by_name("NUMERIC_PRECISION"), 15);
                            break;
                        case proto::DATETIME:
                        case proto::TIMESTAMP:
                        case proto::DATE:
                            record->set_int64(record->get_field_by_name("DATETIME_PRECISION"), 0);
                            break;
                        default:
                            break;
                    }
                    record->set_string(record->get_field_by_name("CHARACTER_SET_NAME"), "utf8");
                    record->set_string(record->get_field_by_name("COLLATION_NAME"), "utf8_general_ci");
                    record->set_string(record->get_field_by_name("COLUMN_TYPE"), to_mysql_type_full_string(field.type));
                    std::vector<std::string> extra_vec;
                    if (field_index.count(field.id) == 0) {
                        record->set_string(record->get_field_by_name("COLUMN_KEY"), " ");
                    } else {
                        std::vector<std::string> index_types;
                        index_types.reserve(4);
                        auto range = field_index.equal_range(field.id);
                        for (auto index_iter = range.first; index_iter != range.second; ++index_iter) {
                            auto &index_info = index_iter->second;
                            std::string index = proto::IndexType_Name(index_info.type);
                            if (index_info.type == proto::I_FULLTEXT) {
                                index += "(" + proto::SegmentType_Name(index_info.segment_type) + ")";
                            }
                            index_types.push_back(index);
                            extra_vec.push_back(proto::IndexState_Name(index_info.state));
                        }
                        record->set_string(record->get_field_by_name("COLUMN_KEY"),
                                           turbo::FormatRange("{}", index_types, "|"));
                    }
                    if (table_info->auto_inc_field_id == field.id) {
                        extra_vec.push_back("auto_increment");
                    } else {
                        //extra_vec.push_back(" ");
                    }
                    if (field.on_update_value == "(current_timestamp())") {
                        extra_vec.push_back("on update CURRENT_TIMESTAMP");
                    }
                    record->set_string(record->get_field_by_name("EXTRA"), turbo::FormatRange("{}", extra_vec, "|"));
                    record->set_string(record->get_field_by_name("PRIVILEGES"), "select,insert,update,references");
                    record->set_string(record->get_field_by_name("COLUMN_COMMENT"), field.comment);
                    records.emplace_back(record);
                }
            }
            return records;
        };
    }

    void InformationSchema::init_referential_constraints() {
        // 定义字段信息
        FieldVec fields{
                {"CONSTRAINT_CATALOG",        proto::STRING},
                {"CONSTRAINT_SCHEMA",         proto::STRING},
                {"CONSTRAINT_NAME",           proto::STRING},
                {"UNIQUE_CONSTRAINT_CATALOG", proto::STRING},
                {"UNIQUE_CONSTRAINT_SCHEMA",  proto::STRING},
                {"UNIQUE_CONSTRAINT_NAME",    proto::STRING},
                {"MATCH_OPTION",              proto::STRING},
                {"UPDATE_RULE",               proto::STRING},
                {"DELETE_RULE",               proto::STRING},
                {"TABLE_NAME",                proto::STRING},
                {"REFERENCED_TABLE_NAME",     proto::STRING}
        };
        int64_t table_id = construct_table("REFERENTIAL_CONSTRAINTS", fields);
        // 定义操作
        _calls[table_id] = [table_id](RuntimeState *state, std::vector<ExprNode *> &conditions) ->
                std::vector<SmartRecord> {
            std::vector<SmartRecord> records;
            return records;
        };
    }

    void InformationSchema::init_key_column_usage() {
        // 定义字段信息
        FieldVec fields{
                {"CONSTRAINT_CATALOG",            proto::STRING},
                {"CONSTRAINT_SCHEMA",             proto::STRING},
                {"CONSTRAINT_NAME",               proto::STRING},
                {"TABLE_CATALOG",                 proto::STRING},
                {"TABLE_SCHEMA",                  proto::STRING},
                {"TABLE_NAME",                    proto::STRING},
                {"COLUMN_NAME",                   proto::STRING},
                {"ORDINAL_POSITION",              proto::INT64},
                {"POSITION_IN_UNIQUE_CONSTRAINT", proto::INT64},
                {"REFERENCED_TABLE_SCHEMA",       proto::STRING},
                {"REFERENCED_TABLE_NAME",         proto::STRING},
                {"REFERENCED_COLUMN_NAME",        proto::STRING}
        };
        int64_t table_id = construct_table("KEY_COLUMN_USAGE", fields);
        // 定义操作
        _calls[table_id] = [table_id](RuntimeState *state, std::vector<ExprNode *> &conditions) ->
                std::vector<SmartRecord> {
            std::vector<SmartRecord> records;
            if (state->client_conn() == nullptr) {
                return records;
            }
            std::string namespace_ = state->client_conn()->user_info->namespace_;
            std::string table_name;
            auto *factory = SchemaFactory::get_instance();
            auto tb_vec = factory->get_table_list(namespace_, state->client_conn()->user_info.get());
            records.reserve(tb_vec.size() * 10);
            for (auto &table_info: tb_vec) {
                std::vector<std::string> items = turbo::StrSplit(table_info->name, turbo::ByAnyChar("."));
                std::string db = items[0];

                std::multimap<int32_t, IndexInfo> field_index;
                for (auto &index_id: table_info->indices) {
                    IndexInfo index_info = factory->get_index_info(index_id);
                    auto index_type = index_info.type;
                    if (index_type != proto::I_PRIMARY && index_type != proto::I_UNIQ) {
                        continue;
                    }
                    int idx = 0;
                    for (auto &field: index_info.fields) {
                        idx++;
                        auto record = factory->new_record(table_id);
                        record->set_string(record->get_field_by_name("CONSTRAINT_CATALOG"), "def");
                        record->set_string(record->get_field_by_name("CONSTRAINT_SCHEMA"), db);
                        record->set_string(record->get_field_by_name("CONSTRAINT_NAME"),
                                           index_type == proto::I_PRIMARY ? "PRIMARY" : "name_key");
                        record->set_string(record->get_field_by_name("TABLE_CATALOG"), "def");
                        record->set_string(record->get_field_by_name("TABLE_SCHEMA"), db);
                        record->set_string(record->get_field_by_name("TABLE_NAME"), table_info->short_name);
                        record->set_string(record->get_field_by_name("COLUMN_NAME"), field.short_name);
                        record->set_int64(record->get_field_by_name("ORDINAL_POSITION"), idx);
                        records.emplace_back(record);

                    }
                }
            }
            return records;
        };
    }

    void InformationSchema::init_statistics() {
        // 定义字段信息
        FieldVec fields{
                {"TABLE_CATALOG", proto::STRING},
                {"TABLE_SCHEMA",  proto::STRING},
                {"TABLE_NAME",    proto::STRING},
                {"NON_UNIQUE",    proto::STRING},
                {"INDEX_SCHEMA",  proto::STRING},
                {"INDEX_NAME",    proto::STRING},
                {"SEQ_IN_INDEX",  proto::INT64},
                {"COLUMN_NAME",   proto::STRING},
                {"COLLATION",     proto::STRING},
                {"CARDINALITY",   proto::INT64},
                {"SUB_PART",      proto::INT64},
                {"PACKED",        proto::STRING},
                {"NULLABLE",      proto::STRING},
                {"INDEX_TYPE",    proto::STRING},
                {"COMMENT",       proto::STRING},
                {"INDEX_COMMENT", proto::STRING},
        };
        int64_t table_id = construct_table("STATISTICS", fields);
        // 定义操作
        _calls[table_id] = [table_id](RuntimeState *state, std::vector<ExprNode *> &conditions) ->
                std::vector<SmartRecord> {
            std::vector<SmartRecord> records;
            if (state->client_conn() == nullptr) {
                return records;
            }
            std::string namespace_ = state->client_conn()->user_info->namespace_;
            std::string table_name;
            auto *factory = SchemaFactory::get_instance();
            auto tb_vec = factory->get_table_list(namespace_, state->client_conn()->user_info.get());
            records.reserve(tb_vec.size() * 10);
            for (auto &table_info: tb_vec) {
                std::vector<std::string> items = turbo::StrSplit(table_info->name, turbo::ByAnyChar("."));
                std::string db = items[0];
                for (auto &index_id: table_info->indices) {
                    auto index_ptr = factory->get_index_info_ptr(index_id);
                    if (index_ptr == nullptr) {
                        continue;
                    }
                    if (index_ptr->index_hint_status != proto::IHS_NORMAL) {
                        continue;
                    }
                    int i = 0;
                    for (auto &field: index_ptr->fields) {
                        auto record = factory->new_record(table_id);
                        record->set_string(record->get_field_by_name("TABLE_CATALOG"), "def");
                        record->set_string(record->get_field_by_name("TABLE_SCHEMA"), db);
                        record->set_string(record->get_field_by_name("TABLE_NAME"), table_info->short_name);
                        record->set_string(record->get_field_by_name("INDEX_SCHEMA"), db);
                        std::string index_name = index_ptr->short_name;
                        std::string index_type = "BTREE";
                        std::string non_unique = "0";
                        if (index_ptr->type == proto::I_PRIMARY) {
                            index_name = "PRIMARY";
                        } else if (index_ptr->type == proto::I_KEY) {
                            non_unique = "1";
                        } else if (index_ptr->type == proto::I_FULLTEXT) {
                            non_unique = "1";
                            index_type = "FULLTEXT";
                        }
                        record->set_string(record->get_field_by_name("INDEX_NAME"), index_name);
                        record->set_string(record->get_field_by_name("COLUMN_NAME"), field.short_name);
                        record->set_string(record->get_field_by_name("NON_UNIQUE"), non_unique);
                        record->set_int64(record->get_field_by_name("SEQ_IN_INDEX"), i++);
                        record->set_string(record->get_field_by_name("NULLABLE"), field.can_null ? "YES" : "");
                        record->set_string(record->get_field_by_name("COLLATION"), "A");
                        record->set_string(record->get_field_by_name("INDEX_TYPE"), index_type);
                        record->set_string(record->get_field_by_name("INDEX_COMMENT"), index_ptr->comments);
                        std::ostringstream comment;
                        comment << "'{\"segment_type\":\"";
                        comment << proto::SegmentType_Name(index_ptr->segment_type) << "\", ";
                        comment << "\"storage_type\":\"";
                        comment << proto::StorageType_Name(index_ptr->storage_type) << "\", ";
                        comment << "\"is_global\":\"" << index_ptr->is_global << "\"}'";
                        record->set_string(record->get_field_by_name("COMMENT"), comment.str());
                        records.emplace_back(record);
                    }
                }
            }
            return records;
        };
    }

    void InformationSchema::init_schemata() {
        // 定义字段信息
        FieldVec fields{
                {"CATALOG_NAME",               proto::STRING},
                {"SCHEMA_NAME",                proto::STRING},
                {"DEFAULT_CHARACTER_SET_NAME", proto::STRING},
                {"DEFAULT_COLLATION_NAME",     proto::STRING},
                {"SQL_PATH",                   proto::INT64},
        };
        int64_t table_id = construct_table("SCHEMATA", fields);
        // 定义操作
        _calls[table_id] = [table_id](RuntimeState *state, std::vector<ExprNode *> &conditions) ->
                std::vector<SmartRecord> {
            std::vector<SmartRecord> records;
            if (state->client_conn() == nullptr) {
                return records;
            }
            auto *factory = SchemaFactory::get_instance();
            std::vector<std::string> db_vec = factory->get_db_list(state->client_conn()->user_info->all_database);
            records.reserve(db_vec.size());
            for (auto &db: db_vec) {
                auto record = factory->new_record(table_id);
                record->set_string(record->get_field_by_name("CATALOG_NAME"), "def");
                record->set_string(record->get_field_by_name("SCHEMA_NAME"), db);
                record->set_string(record->get_field_by_name("DEFAULT_CHARACTER_SET_NAME"), "utf8mb4");
                record->set_string(record->get_field_by_name("DEFAULT_COLLATION_NAME"), "utf8mb4_bin");
                records.emplace_back(record);
            }
            return records;
        };
    }

    void InformationSchema::init_tables() {
        // 定义字段信息
        FieldVec fields{
                {"TABLE_CATALOG",   proto::STRING},
                {"TABLE_SCHEMA",    proto::STRING},
                {"TABLE_NAME",      proto::STRING},
                {"TABLE_TYPE",      proto::STRING},
                {"ENGINE",          proto::STRING},
                {"VERSION",         proto::INT64},
                {"ROW_FORMAT",      proto::STRING},
                {"TABLE_ROWS",      proto::INT64},
                {"AVG_ROW_LENGTH",  proto::INT64},
                {"DATA_LENGTH",     proto::INT64},
                {"MAX_DATA_LENGTH", proto::INT64},
                {"INDEX_LENGTH",    proto::INT64},
                {"DATA_FREE",       proto::INT64},
                {"AUTO_INCREMENT",  proto::INT64},
                {"CREATE_TIME",     proto::DATETIME},
                {"UPDATE_TIME",     proto::DATETIME},
                {"CHECK_TIME",      proto::DATETIME},
                {"TABLE_COLLATION", proto::STRING},
                {"CHECKSUM",        proto::INT64},
                {"CREATE_OPTIONS",  proto::STRING},
                {"TABLE_COMMENT",   proto::STRING},
                {"TABLE_ID",        proto::INT64},
        };
        int64_t table_id = construct_table("TABLES", fields);
        // 定义操作
        _calls[table_id] = [table_id](RuntimeState *state, std::vector<ExprNode *> &conditions) ->
                std::vector<SmartRecord> {
            std::vector<SmartRecord> records;
            if (state->client_conn() == nullptr) {
                return records;
            }
            std::string namespace_ = state->client_conn()->user_info->namespace_;
            std::string table_name;
            auto *factory = SchemaFactory::get_instance();
            auto tb_vec = factory->get_table_list(namespace_, state->client_conn()->user_info.get());
            records.reserve(tb_vec.size());
            for (auto &table_info: tb_vec) {
                std::vector<std::string> items = turbo::StrSplit(table_info->name, turbo::ByAnyChar("."));
                std::string db = items[0];
                auto record = factory->new_record(table_id);
                record->set_string(record->get_field_by_name("TABLE_CATALOG"), "def");
                record->set_string(record->get_field_by_name("TABLE_SCHEMA"), db);
                record->set_string(record->get_field_by_name("TABLE_NAME"), table_info->short_name);
                record->set_string(record->get_field_by_name("TABLE_TYPE"), "BASE TABLE");
                record->set_string(record->get_field_by_name("ENGINE"), "Innodb");
                record->set_int64(record->get_field_by_name("VERSION"), table_info->version);
                record->set_string(record->get_field_by_name("ROW_FORMAT"), "Compact");
                record->set_int64(record->get_field_by_name("TABLE_ROWS"), 0);
                record->set_int64(record->get_field_by_name("AVG_ROW_LENGTH"), table_info->byte_size_per_record);
                record->set_int64(record->get_field_by_name("DATA_LENGTH"), 0);
                record->set_int64(record->get_field_by_name("MAX_DATA_LENGTH"), 0);
                record->set_int64(record->get_field_by_name("INDEX_LENGTH"), 0);
                record->set_int64(record->get_field_by_name("DATA_FREE"), 0);
                record->set_int64(record->get_field_by_name("AUTO_INCREMENT"), 0);
                ExprValue ct(proto::TIMESTAMP);
                ct._u.uint32_val = table_info->timestamp;
                std::string coll = "utf8_bin";
                if (table_info->charset == proto::GBK) {
                    coll = "gbk_bin";
                }
                record->set_value(record->get_field_by_name("CREATE_TIME"), ct.cast_to(proto::DATETIME));
                record->set_string(record->get_field_by_name("TABLE_COLLATION"), coll);
                record->set_string(record->get_field_by_name("CREATE_OPTIONS"), "");
                record->set_string(record->get_field_by_name("TABLE_COMMENT"), "");
                record->set_int64(record->get_field_by_name("TABLE_ID"), table_info->id);
                records.emplace_back(record);
            }
            return records;
        };
    }

    void InformationSchema::init_virtual_index_influence_info() {
        //定义字段信息
        FieldVec fields{
                {"database_name",      proto::STRING},
                {"table_name",         proto::STRING},
                {"virtual_index_name", proto::STRING},
                {"sign",               proto::STRING},
                {"sample_sql",         proto::STRING},
        };
        int64_t table_id = construct_table("VIRTUAL_INDEX_AFFECT_SQL", fields);
        //定义操作
        _calls[table_id] = [table_id](RuntimeState *state, std::vector<ExprNode *> &conditions) ->
                std::vector<SmartRecord> {
            std::vector<SmartRecord> records;
            //更新表中数据前，要交互一次，从TableMem取影响面数据
            proto::QueryRequest request;
            proto::QueryResponse response;
            //1、设定查询请求的操作类型
            request.set_op_type(proto::QUERY_SHOW_VIRINDX_INFO_SQL);
            //2、发送请求
            MetaServerInteract::get_instance()->send_request("query", request, response);
            //3.取出response中的影响面信息
            auto &virtual_index_info_sqls = response.virtual_index_influence_info();//virtual_index_info   and   affected_sqls
            if (state->client_conn() == nullptr) {
                return records;
            }
            std::string namespace_ = state->client_conn()->user_info->namespace_;
            std::string table_name;
            auto *factory = SchemaFactory::get_instance();
            auto tb_vec = factory->get_table_list(namespace_, state->client_conn()->user_info.get());
            records.reserve(tb_vec.size());
            for (auto &it1: virtual_index_info_sqls) {
                std::string key = it1.virtual_index_info();
                std::string infuenced_sql = it1.affected_sqls();
                std::string sign = it1.affected_sign();
                std::vector<std::string> items1 = turbo::StrSplit(key, turbo::ByAnyChar(","));
                auto record = factory->new_record(table_id);
                record->set_string(record->get_field_by_name("database_name"), items1[0]);
                record->set_string(record->get_field_by_name("table_name"), items1[1]);
                record->set_string(record->get_field_by_name("virtual_index_name"), items1[2]);
                record->set_string(record->get_field_by_name("sign"), sign);
                record->set_string(record->get_field_by_name("sample_sql"), infuenced_sql);
                records.emplace_back(record);
            }
            return records;
        };
    }

    void InformationSchema::init_sign_list() {
        //定义字段信息
        FieldVec fields{
                {"namespace",     proto::STRING},
                {"database_name", proto::STRING},
                {"table_name",    proto::STRING},
                {"sign",          proto::STRING},
        };

        int64_t blacklist_table_id = construct_table("SIGN_BLACKLIST", fields);
        int64_t forcelearner_table_id = construct_table("SIGN_FORCELEARNER", fields);
        int64_t forceindex_table_id = construct_table("SIGN_FORCEINDEX", fields);
        //定义操作
        _calls[blacklist_table_id] = [blacklist_table_id](RuntimeState *state, std::vector<ExprNode *> &conditions) ->
                std::vector<SmartRecord> {
            std::vector<SmartRecord> records;
            records.reserve(10);
            auto blacklist_table = SchemaFactory::get_instance()->get_table_info_ptr(blacklist_table_id);
            auto func = [&records, &blacklist_table](const SmartTable &table) -> bool {
                for (auto sign: table->sign_blacklist) {
                    auto record = SchemaFactory::get_instance()->new_record(*blacklist_table);
                    record->set_string(record->get_field_by_name("namespace"), table->namespace_);
                    std::string db_name;
                    std::vector<std::string> vec = turbo::StrSplit(table->name, turbo::ByAnyChar("."));
                    if (!vec.empty()) {
                        db_name = vec[0];
                    }
                    record->set_string(record->get_field_by_name("database_name"), db_name);
                    record->set_string(record->get_field_by_name("table_name"), table->short_name);
                    record->set_string(record->get_field_by_name("sign"), std::to_string(sign));
                    records.emplace_back(record);
                }
                return false;
            };
            std::vector<std::string> database_table;
            SchemaFactory::get_instance()->get_table_by_filter(database_table, func);
            return records;
        };

        _calls[forcelearner_table_id] = [forcelearner_table_id](RuntimeState *state,
                                                                std::vector<ExprNode *> &conditions) ->
                std::vector<SmartRecord> {
            std::vector<SmartRecord> records;
            records.reserve(10);
            auto forcelearner_table = SchemaFactory::get_instance()->get_table_info_ptr(forcelearner_table_id);
            auto func = [&records, &forcelearner_table](const SmartTable &table) -> bool {
                for (auto sign: table->sign_forcelearner) {
                    auto record = SchemaFactory::get_instance()->new_record(*forcelearner_table);
                    record->set_string(record->get_field_by_name("namespace"), table->namespace_);
                    std::string db_name;
                    std::vector<std::string> vec = turbo::StrSplit(table->name, turbo::ByAnyChar("."));
                    if (!vec.empty()) {
                        db_name = vec[0];
                    }
                    record->set_string(record->get_field_by_name("database_name"), db_name);
                    record->set_string(record->get_field_by_name("table_name"), table->short_name);
                    record->set_string(record->get_field_by_name("sign"), std::to_string(sign));
                    records.emplace_back(record);
                }
                return false;
            };
            std::vector<std::string> database_table;
            SchemaFactory::get_instance()->get_table_by_filter(database_table, func);
            return records;
        };

        _calls[forceindex_table_id] = [forceindex_table_id](RuntimeState *state, std::vector<ExprNode *> &conditions) ->
                std::vector<SmartRecord> {
            std::vector<SmartRecord> records;
            records.reserve(10);
            auto forceindex_table = SchemaFactory::get_instance()->get_table_info_ptr(forceindex_table_id);
            auto func = [&records, &forceindex_table](const SmartTable &table) -> bool {
                for (auto sign_index: table->sign_forceindex) {
                    auto record = SchemaFactory::get_instance()->new_record(*forceindex_table);
                    record->set_string(record->get_field_by_name("namespace"), table->namespace_);
                    std::string db_name;
                    std::vector<std::string> vec = turbo::StrSplit(table->name, turbo::ByAnyChar("."));
                    if (!vec.empty()) {
                        db_name = vec[0];
                    }
                    record->set_string(record->get_field_by_name("database_name"), db_name);
                    record->set_string(record->get_field_by_name("table_name"), table->short_name);
                    record->set_string(record->get_field_by_name("sign"), sign_index);
                    records.emplace_back(record);
                }
                return false;
            };
            std::vector<std::string> database_table;
            SchemaFactory::get_instance()->get_table_by_filter(database_table, func);
            return records;
        };

        _calls[forceindex_table_id] = [forceindex_table_id](RuntimeState *state, std::vector<ExprNode *> &conditions) ->
                std::vector<SmartRecord> {
            std::vector<SmartRecord> records;
            records.reserve(10);
            auto forceindex_table = SchemaFactory::get_instance()->get_table_info_ptr(forceindex_table_id);
            auto func = [&records, &forceindex_table](const SmartTable &table) -> bool {
                for (auto sign_index: table->sign_forceindex) {
                    auto record = SchemaFactory::get_instance()->new_record(*forceindex_table);
                    record->set_string(record->get_field_by_name("namespace"), table->namespace_);
                    std::string db_name;
                    std::vector<std::string> vec = turbo::StrSplit(table->name, turbo::ByAnyChar("."));
                    if (!vec.empty()) {
                        db_name = vec[0];
                    }
                    record->set_string(record->get_field_by_name("database_name"), db_name);
                    record->set_string(record->get_field_by_name("table_name"), table->short_name);
                    record->set_string(record->get_field_by_name("sign"), sign_index);
                    records.emplace_back(record);
                }
                return false;
            };
            std::vector<std::string> database_table;
            std::vector<std::string> binlog_table;
            SchemaFactory::get_instance()->get_table_by_filter(database_table, func);
            return records;
        };
    }

    void InformationSchema::init_routines() {
        // 定义字段信息
        FieldVec fields{
                {"SPECIFIC_NAME",            proto::STRING},
                {"ROUTINE_CATALOG",          proto::STRING},
                {"ROUTINE_SCHEMA",           proto::STRING},
                {"ROUTINE_NAME",             proto::STRING},
                {"ROUTINE_TYPE",             proto::STRING},
                {"DATA_TYPE",                proto::STRING},
                {"CHARACTER_MAXIMUM_LENGTH", proto::INT64},
                {"CHARACTER_OCTET_LENGTH",   proto::INT64},
                {"NUMERIC_PRECISION",        proto::UINT64},
                {"NUMERIC_SCALE",            proto::INT64},
                {"DATETIME_PRECISION",       proto::UINT64},
                {"CHARACTER_SET_NAME",       proto::STRING},
                {"COLLATION_NAME",           proto::STRING},
                {"DTD_IDENTIFIER",           proto::STRING},
                {"ROUTINE_BODY",             proto::STRING},
                {"ROUTINE_DEFINITION",       proto::STRING},
                {"EXTERNAL_NAME",            proto::STRING},
                {"EXTERNAL_LANGUAGE",        proto::STRING},
                {"PARAMETER_STYLE",          proto::STRING},
                {"IS_DETERMINISTIC",         proto::STRING},
                {"SQL_DATA_ACCESS",          proto::STRING},
                {"SQL_PATH",                 proto::STRING},
                {"SECURITY_TYPE",            proto::STRING},
                {"CREATED",                  proto::STRING},
                {"LAST_ALTERED",             proto::DATETIME},
                {"SQL_MODE",                 proto::DATETIME},
                {"ROUTINE_COMMENT",          proto::STRING},
                {"DEFINER",                  proto::STRING},
                {"CHARACTER_SET_CLIENT",     proto::STRING},
                {"COLLATION_CONNECTION",     proto::STRING},
                {"DATABASE_COLLATION",       proto::STRING},
        };
        int64_t table_id = construct_table("ROUTINES", fields);
        // 定义操作
        _calls[table_id] = [table_id](RuntimeState *state, std::vector<ExprNode *> &conditions) ->
                std::vector<SmartRecord> {
            std::vector<SmartRecord> records;
            return records;
        };
    }

    void InformationSchema::init_triggers() {
        // 定义字段信息
        FieldVec fields{
                {"TRIGGER_CATALOG",            proto::STRING},
                {"TRIGGER_SCHEMA",             proto::STRING},
                {"TRIGGER_NAME",               proto::STRING},
                {"EVENT_MANIPULATION",         proto::STRING},
                {"EVENT_OBJECT_CATALOG",       proto::STRING},
                {"EVENT_OBJECT_SCHEMA",        proto::STRING},
                {"EVENT_OBJECT_TABLE",         proto::STRING},
                {"ACTION_ORDER",               proto::INT64},
                {"ACTION_CONDITION",           proto::STRING},
                {"ACTION_STATEMENT",           proto::STRING},
                {"ACTION_ORIENTATION",         proto::STRING},
                {"ACTION_TIMING",              proto::STRING},
                {"ACTION_REFERENCE_OLD_TABLE", proto::STRING},
                {"ACTION_REFERENCE_NEW_TABLE", proto::STRING},
                {"ACTION_REFERENCE_OLD_ROW",   proto::STRING},
                {"ACTION_REFERENCE_NEW_ROW",   proto::STRING},
                {"CREATED",                    proto::DATETIME},
                {"SQL_MODE",                   proto::STRING},
                {"DEFINER",                    proto::STRING},
                {"CHARACTER_SET_CLIENT",       proto::STRING},
                {"COLLATION_CONNECTION",       proto::STRING},
                {"DATABASE_COLLATION",         proto::STRING},
        };
        int64_t table_id = construct_table("TRIGGERS", fields);
        // 定义操作
        _calls[table_id] = [table_id](RuntimeState *state, std::vector<ExprNode *> &conditions) ->
                std::vector<SmartRecord> {
            std::vector<SmartRecord> records;
            return records;
        };
    }

    void InformationSchema::init_views() {
        // 定义字段信息
        FieldVec fields{
                {"TABLE_CATALOG",        proto::STRING},
                {"TABLE_SCHEMA",         proto::STRING},
                {"TABLE_NAME",           proto::STRING},
                {"VIEW_DEFINITION",      proto::STRING},
                {"CHECK_OPTION",         proto::STRING},
                {"IS_UPDATABLE",         proto::STRING},
                {"DEFINER",              proto::STRING},
                {"SECURITY_TYPE",        proto::INT64},
                {"CHARACTER_SET_CLIENT", proto::STRING},
                {"COLLATION_CONNECTION", proto::STRING},
        };
        int64_t table_id = construct_table("VIEWS", fields);
        // 定义操作
        _calls[table_id] = [table_id](RuntimeState *state, std::vector<ExprNode *> &conditions) ->
                std::vector<SmartRecord> {
            std::vector<SmartRecord> records;
            return records;
        };
    }

    void InformationSchema::init_character_sets() {
        FieldVec fields{
                {"CHARACTER_SET_NAME",   proto::STRING},
                {"DEFAULT_COLLATE_NAME", proto::STRING},
                {"DESCRIPTION",          proto::STRING},
                {"MAXLEN",               proto::INT64},
        };
        int64_t table_id = construct_table("CHARACTER_SETS", fields);
        _calls[table_id] = [table_id](RuntimeState *state, std::vector<ExprNode *> &conditions) ->
                std::vector<SmartRecord> {
            std::vector<SmartRecord> records;
            return records;
        };
    }

    void InformationSchema::init_collation_character_set_applicability() {
        FieldVec fields{
                {"COLLATION_NAME",     proto::STRING},
                {"CHARACTER_SET_NAME", proto::STRING},
        };
        int64_t table_id = construct_table("COLLATION_CHARACTER_SET_APPLICABILITY", fields);
        _calls[table_id] = [table_id](RuntimeState *state, std::vector<ExprNode *> &conditions) ->
                std::vector<SmartRecord> {
            std::vector<SmartRecord> records;
            return records;
        };
    }

    void InformationSchema::init_collations() {
        FieldVec fields{
                {"COLLATION_NAME",     proto::STRING},
                {"CHARACTER_SET_NAME", proto::STRING},
                {"ID",                 proto::INT64},
                {"IS_DEFAULT",         proto::STRING},
                {"IS_COMPILED",        proto::STRING},
                {"SORTLEN",            proto::INT64},
        };
        int64_t table_id = construct_table("COLLATIONS", fields);
        _calls[table_id] = [table_id](RuntimeState *state, std::vector<ExprNode *> &conditions) ->
                std::vector<SmartRecord> {
            std::vector<SmartRecord> records;
            return records;
        };
    }

    void InformationSchema::init_column_privileges() {
        FieldVec fields{
                {"GRANTEE",        proto::STRING},
                {"TABLE_CATALOG",  proto::STRING},
                {"TABLE_SCHEMA",   proto::STRING},
                {"TABLE_NAME",     proto::STRING},
                {"COLUMN_NAME",    proto::STRING},
                {"PRIVILEGE_TYPE", proto::STRING},
                {"IS_GRANTABLE",   proto::STRING},
        };
        int64_t table_id = construct_table("COLUMN_PRIVILEGES", fields);
        _calls[table_id] = [table_id](RuntimeState *state, std::vector<ExprNode *> &conditions) ->
                std::vector<SmartRecord> {
            std::vector<SmartRecord> records;
            return records;
        };
    }

    void InformationSchema::init_engines() {
        FieldVec fields{
                {"ENGINE",       proto::STRING},
                {"SUPPORT",      proto::STRING},
                {"COMMENT",      proto::STRING},
                {"TRANSACTIONS", proto::STRING},
                {"XA",           proto::STRING},
                {"SAVEPOINTS",   proto::STRING},
        };
        int64_t table_id = construct_table("ENGINES", fields);
        _calls[table_id] = [table_id](RuntimeState *state, std::vector<ExprNode *> &conditions) ->
                std::vector<SmartRecord> {
            std::vector<SmartRecord> records;
            return records;
        };
    }

    void InformationSchema::init_events() {
        FieldVec fields{
                {"EVENT_CATALOG",        proto::STRING},
                {"EVENT_SCHEMA",         proto::STRING},
                {"EVENT_NAME",           proto::STRING},
                {"DEFINER",              proto::STRING},
                {"TIME_ZONE",            proto::STRING},
                {"EVENT_BODY",           proto::STRING},
                {"EVENT_DEFINITION",     proto::STRING},
                {"EVENT_TYPE",           proto::STRING},
                {"EXECUTE_AT",           proto::DATETIME},
                {"INTERVAL_VALUE",       proto::STRING},
                {"INTERVAL_FIELD",       proto::STRING},
                {"SQL_MODE",             proto::STRING},
                {"STARTS",               proto::DATETIME},
                {"ENDS",                 proto::DATETIME},
                {"STATUS",               proto::STRING},
                {"ON_COMPLETION",        proto::STRING},
                {"CREATED",              proto::DATETIME},
                {"LAST_ALTERED",         proto::DATETIME},
                {"LAST_EXECUTED",        proto::DATETIME},
                {"EVENT_COMMENT",        proto::STRING},
                {"ORIGINATOR",           proto::INT64},
                {"CHARACTER_SET_CLIENT", proto::STRING},
                {"COLLATION_CONNECTION", proto::STRING},
                {"DATABASE_COLLATION",   proto::STRING},
        };
        int64_t table_id = construct_table("EVENTS", fields);
        _calls[table_id] = [table_id](RuntimeState *state, std::vector<ExprNode *> &conditions) ->
                std::vector<SmartRecord> {
            std::vector<SmartRecord> records;
            return records;
        };
    }

    void InformationSchema::init_files() {
        FieldVec fields{
                {"FILE_ID",              proto::INT64},
                {"FILE_NAME",            proto::STRING},
                {"FILE_TYPE",            proto::STRING},
                {"TABLESPACE_NAME",      proto::STRING},
                {"TABLE_CATALOG",        proto::STRING},
                {"TABLE_SCHEMA",         proto::STRING},
                {"TABLE_NAME",           proto::STRING},
                {"LOGFILE_GROUP_NAME",   proto::STRING},
                {"LOGFILE_GROUP_NUMBER", proto::INT64},
                {"ENGINE",               proto::STRING},
                {"FULLTEXT_KEYS",        proto::STRING},
                {"DELETED_ROWS",         proto::INT64},
                {"UPDATE_COUNT",         proto::INT64},
                {"FREE_EXTENTS",         proto::INT64},
                {"TOTAL_EXTENTS",        proto::INT64},
                {"EXTENT_SIZE",          proto::INT64},
                {"INITIAL_SIZE",         proto::UINT64},
                {"MAXIMUM_SIZE",         proto::UINT64},
                {"AUTOEXTEND_SIZE",      proto::UINT64},
                {"CREATION_TIME",        proto::DATETIME},
                {"LAST_UPDATE_TIME",     proto::DATETIME},
                {"LAST_ACCESS_TIME",     proto::DATETIME},
                {"RECOVER_TIME",         proto::INT64},
                {"TRANSACTION_COUNTER",  proto::INT64},
                {"VERSION",              proto::UINT64},
                {"ROW_FORMAT",           proto::STRING},
                {"TABLE_ROWS",           proto::UINT64},
                {"AVG_ROW_LENGTH",       proto::UINT64},
                {"DATA_LENGTH",          proto::UINT64},
                {"MAX_DATA_LENGTH",      proto::UINT64},
                {"INDEX_LENGTH",         proto::UINT64},
                {"DATA_FREE",            proto::UINT64},
                {"CREATE_TIME",          proto::DATETIME},
                {"UPDATE_TIME",          proto::DATETIME},
                {"CHECK_TIME",           proto::DATETIME},
                {"CHECKSUM",             proto::UINT64},
                {"STATUS",               proto::STRING},
                {"EXTRA",                proto::STRING},
        };
        int64_t table_id = construct_table("FILES", fields);
        _calls[table_id] = [table_id](RuntimeState *state, std::vector<ExprNode *> &conditions) ->
                std::vector<SmartRecord> {
            std::vector<SmartRecord> records;
            return records;
        };
    }

    void InformationSchema::init_global_status() {
        FieldVec fields{
                {"VARIABLE_NAME",  proto::STRING},
                {"VARIABLE_VALUE", proto::STRING},
        };
        int64_t table_id = construct_table("GLOBAL_STATUS", fields);
        _calls[table_id] = [table_id](RuntimeState *state, std::vector<ExprNode *> &conditions) ->
                std::vector<SmartRecord> {
            std::vector<SmartRecord> records;
            return records;
        };
    }

    void InformationSchema::init_global_variables() {
        FieldVec fields{
                {"VARIABLE_NAME",  proto::STRING},
                {"VARIABLE_VALUE", proto::STRING},
        };
        int64_t table_id = construct_table("GLOBAL_VARIABLES", fields);
        _calls[table_id] = [table_id](RuntimeState *state, std::vector<ExprNode *> &conditions) ->
                std::vector<SmartRecord> {
            std::vector<SmartRecord> records;
            return records;
        };
    }

    void InformationSchema::init_innodb_buffer_page() {
        FieldVec fields{
                {"POOL_ID",             proto::UINT64},
                {"BLOCK_ID",            proto::UINT64},
                {"SPACE",               proto::UINT64},
                {"PAGE_NUMBER",         proto::UINT64},
                {"PAGE_TYPE",           proto::STRING},
                {"FLUSH_TYPE",          proto::UINT64},
                {"FIX_COUNT",           proto::UINT64},
                {"IS_HASHED",           proto::STRING},
                {"NEWEST_MODIFICATION", proto::UINT64},
                {"OLDEST_MODIFICATION", proto::UINT64},
                {"ACCESS_TIME",         proto::UINT64},
                {"TABLE_NAME",          proto::STRING},
                {"INDEX_NAME",          proto::STRING},
                {"NUMBER_RECORDS",      proto::UINT64},
                {"DATA_SIZE",           proto::UINT64},
                {"COMPRESSED_SIZE",     proto::UINT64},
                {"PAGE_STATE",          proto::STRING},
                {"IO_FIX",              proto::STRING},
                {"IS_OLD",              proto::STRING},
                {"FREE_PAGE_CLOCK",     proto::UINT64},
        };
        int64_t table_id = construct_table("INNODB_BUFFER_PAGE", fields);
        _calls[table_id] = [table_id](RuntimeState *state, std::vector<ExprNode *> &conditions) ->
                std::vector<SmartRecord> {
            std::vector<SmartRecord> records;
            return records;
        };
    }

    void InformationSchema::init_innodb_buffer_page_lru() {
        FieldVec fields{
                {"POOL_ID",             proto::UINT64},
                {"LRU_POSITION",        proto::UINT64},
                {"SPACE",               proto::UINT64},
                {"PAGE_NUMBER",         proto::UINT64},
                {"PAGE_TYPE",           proto::STRING},
                {"FLUSH_TYPE",          proto::UINT64},
                {"FIX_COUNT",           proto::UINT64},
                {"IS_HASHED",           proto::STRING},
                {"NEWEST_MODIFICATION", proto::UINT64},
                {"OLDEST_MODIFICATION", proto::UINT64},
                {"ACCESS_TIME",         proto::UINT64},
                {"TABLE_NAME",          proto::STRING},
                {"INDEX_NAME",          proto::STRING},
                {"NUMBER_RECORDS",      proto::UINT64},
                {"DATA_SIZE",           proto::UINT64},
                {"COMPRESSED_SIZE",     proto::UINT64},
                {"COMPRESSED",          proto::STRING},
                {"IO_FIX",              proto::STRING},
                {"IS_OLD",              proto::STRING},
                {"FREE_PAGE_CLOCK",     proto::UINT64},
        };
        int64_t table_id = construct_table("INNODB_BUFFER_PAGE_LRU", fields);
        _calls[table_id] = [table_id](RuntimeState *state, std::vector<ExprNode *> &conditions) ->
                std::vector<SmartRecord> {
            std::vector<SmartRecord> records;
            return records;
        };
    }

    void InformationSchema::init_innodb_buffer_pool_stats() {
        FieldVec fields{
                {"POOL_ID",                          proto::UINT64},
                {"POOL_SIZE",                        proto::UINT64},
                {"FREE_BUFFERS",                     proto::UINT64},
                {"DATABASE_PAGES",                   proto::UINT64},
                {"OLD_DATABASE_PAGES",               proto::UINT64},
                {"MODIFIED_DATABASE_PAGES",          proto::UINT64},
                {"PENDING_DECOMPRESS",               proto::UINT64},
                {"PENDING_READS",                    proto::UINT64},
                {"PENDING_FLUSH_LRU",                proto::UINT64},
                {"PENDING_FLUSH_LIST",               proto::UINT64},
                {"PAGES_MADE_YOUNG",                 proto::UINT64},
                {"PAGES_NOT_MADE_YOUNG",             proto::UINT64},
                {"PAGES_MADE_YOUNG_RATE",            proto::DOUBLE},
                {"PAGES_MADE_NOT_YOUNG_RATE",        proto::DOUBLE},
                {"NUMBER_PAGES_READ",                proto::UINT64},
                {"NUMBER_PAGES_CREATED",             proto::UINT64},
                {"NUMBER_PAGES_WRITTEN",             proto::UINT64},
                {"PAGES_READ_RATE",                  proto::DOUBLE},
                {"PAGES_CREATE_RATE",                proto::DOUBLE},
                {"PAGES_WRITTEN_RATE",               proto::DOUBLE},
                {"NUMBER_PAGES_GET",                 proto::UINT64},
                {"HIT_RATE",                         proto::UINT64},
                {"YOUNG_MAKE_PER_THOUSAND_GETS",     proto::UINT64},
                {"NOT_YOUNG_MAKE_PER_THOUSAND_GETS", proto::UINT64},
                {"NUMBER_PAGES_READ_AHEAD",          proto::UINT64},
                {"NUMBER_READ_AHEAD_EVICTED",        proto::UINT64},
                {"READ_AHEAD_RATE",                  proto::DOUBLE},
                {"READ_AHEAD_EVICTED_RATE",          proto::DOUBLE},
                {"LRU_IO_TOTAL",                     proto::UINT64},
                {"LRU_IO_CURRENT",                   proto::UINT64},
                {"UNCOMPRESS_TOTAL",                 proto::UINT64},
                {"UNCOMPRESS_CURRENT",               proto::UINT64},
        };
        int64_t table_id = construct_table("INNODB_BUFFER_POOL_STATS", fields);
        _calls[table_id] = [table_id](RuntimeState *state, std::vector<ExprNode *> &conditions) ->
                std::vector<SmartRecord> {
            std::vector<SmartRecord> records;
            return records;
        };
    }

    void InformationSchema::init_innodb_cmp() {
        FieldVec fields{
                {"page_size",       proto::INT32},
                {"compress_ops",    proto::INT32},
                {"compress_ops_ok", proto::INT32},
                {"compress_time",   proto::INT32},
                {"uncompress_ops",  proto::INT32},
                {"uncompress_time", proto::INT32},
        };
        int64_t table_id = construct_table("INNODB_CMP", fields);
        _calls[table_id] = [table_id](RuntimeState *state, std::vector<ExprNode *> &conditions) ->
                std::vector<SmartRecord> {
            std::vector<SmartRecord> records;
            return records;
        };
    }

    void InformationSchema::init_innodb_cmpmem() {
        FieldVec fields{
                {"page_size",            proto::INT32},
                {"buffer_pool_instance", proto::INT32},
                {"pages_used",           proto::INT32},
                {"pages_free",           proto::INT32},
                {"relocation_ops",       proto::INT64},
                {"relocation_time",      proto::INT32},
        };
        int64_t table_id = construct_table("INNODB_CMPMEM", fields);
        _calls[table_id] = [table_id](RuntimeState *state, std::vector<ExprNode *> &conditions) ->
                std::vector<SmartRecord> {
            std::vector<SmartRecord> records;
            return records;
        };
    }

    void InformationSchema::init_innodb_cmpmem_reset() {
        FieldVec fields{
                {"page_size",            proto::INT32},
                {"buffer_pool_instance", proto::INT32},
                {"pages_used",           proto::INT32},
                {"pages_free",           proto::INT32},
                {"relocation_ops",       proto::INT64},
                {"relocation_time",      proto::INT32},
        };
        int64_t table_id = construct_table("INNODB_CMPMEM_RESET", fields);
        _calls[table_id] = [table_id](RuntimeState *state, std::vector<ExprNode *> &conditions) ->
                std::vector<SmartRecord> {
            std::vector<SmartRecord> records;
            return records;
        };
    }

    void InformationSchema::init_innodb_cmp_per_index() {
        FieldVec fields{
                {"database_name",   proto::STRING},
                {"table_name",      proto::STRING},
                {"index_name",      proto::STRING},
                {"compress_ops",    proto::INT32},
                {"compress_ops_ok", proto::INT32},
                {"compress_time",   proto::INT32},
                {"uncompress_ops",  proto::INT32},
                {"uncompress_time", proto::INT32},
        };
        int64_t table_id = construct_table("INNODB_CMP_PER_INDEX", fields);
        _calls[table_id] = [table_id](RuntimeState *state, std::vector<ExprNode *> &conditions) ->
                std::vector<SmartRecord> {
            std::vector<SmartRecord> records;
            return records;
        };
    }

    void InformationSchema::init_innodb_cmp_per_index_reset() {
        FieldVec fields{
                {"database_name",   proto::STRING},
                {"table_name",      proto::STRING},
                {"index_name",      proto::STRING},
                {"compress_ops",    proto::INT32},
                {"compress_ops_ok", proto::INT32},
                {"compress_time",   proto::INT32},
                {"uncompress_ops",  proto::INT32},
                {"uncompress_time", proto::INT32},
        };
        int64_t table_id = construct_table("INNODB_CMP_PER_INDEX_RESET", fields);
        _calls[table_id] = [table_id](RuntimeState *state, std::vector<ExprNode *> &conditions) ->
                std::vector<SmartRecord> {
            std::vector<SmartRecord> records;
            return records;
        };
    }

    void InformationSchema::init_innodb_cmp_reset() {
        FieldVec fields{
                {"page_size",       proto::INT32},
                {"compress_ops",    proto::INT32},
                {"compress_ops_ok", proto::INT32},
                {"compress_time",   proto::INT32},
                {"uncompress_ops",  proto::INT32},
                {"uncompress_time", proto::INT32},
        };
        int64_t table_id = construct_table("INNODB_CMP_RESET", fields);
        _calls[table_id] = [table_id](RuntimeState *state, std::vector<ExprNode *> &conditions) ->
                std::vector<SmartRecord> {
            std::vector<SmartRecord> records;
            return records;
        };
    }

    void InformationSchema::init_innodb_ft_being_deleted() {
        FieldVec fields{
                {"DOC_ID", proto::UINT64},
        };
        int64_t table_id = construct_table("INNODB_FT_BEING_DELETED", fields);
        _calls[table_id] = [table_id](RuntimeState *state, std::vector<ExprNode *> &conditions) ->
                std::vector<SmartRecord> {
            std::vector<SmartRecord> records;
            return records;
        };
    }

    void InformationSchema::init_innodb_ft_config() {
        FieldVec fields{
                {"KEY",   proto::STRING},
                {"VALUE", proto::STRING},
        };
        int64_t table_id = construct_table("INNODB_FT_CONFIG", fields);
        _calls[table_id] = [table_id](RuntimeState *state, std::vector<ExprNode *> &conditions) ->
                std::vector<SmartRecord> {
            std::vector<SmartRecord> records;
            return records;
        };
    }

    void InformationSchema::init_innodb_ft_default_stopword() {
        FieldVec fields{
                {"value", proto::STRING},
        };
        int64_t table_id = construct_table("INNODB_FT_DEFAULT_STOPWORD", fields);
        _calls[table_id] = [table_id](RuntimeState *state, std::vector<ExprNode *> &conditions) ->
                std::vector<SmartRecord> {
            std::vector<SmartRecord> records;
            return records;
        };
    }

    void InformationSchema::init_innodb_ft_deleted() {
        FieldVec fields{
                {"DOC_ID", proto::UINT64},
        };
        int64_t table_id = construct_table("INNODB_FT_DELETED", fields);
        _calls[table_id] = [table_id](RuntimeState *state, std::vector<ExprNode *> &conditions) ->
                std::vector<SmartRecord> {
            std::vector<SmartRecord> records;
            return records;
        };
    }

    void InformationSchema::init_innodb_ft_index_cache() {
        FieldVec fields{
                {"WORD",         proto::STRING},
                {"FIRST_DOC_ID", proto::UINT64},
                {"LAST_DOC_ID",  proto::UINT64},
                {"DOC_COUNT",    proto::UINT64},
                {"DOC_ID",       proto::UINT64},
                {"POSITION",     proto::UINT64},
        };
        int64_t table_id = construct_table("INNODB_FT_INDEX_CACHE", fields);
        _calls[table_id] = [table_id](RuntimeState *state, std::vector<ExprNode *> &conditions) ->
                std::vector<SmartRecord> {
            std::vector<SmartRecord> records;
            return records;
        };
    }

    void InformationSchema::init_innodb_ft_index_table() {
        FieldVec fields{
                {"WORD",         proto::STRING},
                {"FIRST_DOC_ID", proto::UINT64},
                {"LAST_DOC_ID",  proto::UINT64},
                {"DOC_COUNT",    proto::UINT64},
                {"DOC_ID",       proto::UINT64},
                {"POSITION",     proto::UINT64},
        };
        int64_t table_id = construct_table("INNODB_FT_INDEX_TABLE", fields);
        _calls[table_id] = [table_id](RuntimeState *state, std::vector<ExprNode *> &conditions) ->
                std::vector<SmartRecord> {
            std::vector<SmartRecord> records;
            return records;
        };
    }

    void InformationSchema::init_innodb_locks() {
        FieldVec fields{
                {"lock_id",     proto::STRING},
                {"lock_trx_id", proto::STRING},
                {"lock_mode",   proto::STRING},
                {"lock_type",   proto::STRING},
                {"lock_table",  proto::STRING},
                {"lock_index",  proto::STRING},
                {"lock_space",  proto::UINT64},
                {"lock_page",   proto::UINT64},
                {"lock_rec",    proto::UINT64},
                {"lock_data",   proto::STRING},
        };
        int64_t table_id = construct_table("INNODB_LOCKS", fields);
        _calls[table_id] = [table_id](RuntimeState *state, std::vector<ExprNode *> &conditions) ->
                std::vector<SmartRecord> {
            std::vector<SmartRecord> records;
            return records;
        };
    }

    void InformationSchema::init_innodb_lock_waits() {
        FieldVec fields{
                {"requesting_trx_id", proto::STRING},
                {"requested_lock_id", proto::STRING},
                {"blocking_trx_id",   proto::STRING},
                {"blocking_lock_id",  proto::STRING},
        };
        int64_t table_id = construct_table("INNODB_LOCK_WAITS", fields);
        _calls[table_id] = [table_id](RuntimeState *state, std::vector<ExprNode *> &conditions) ->
                std::vector<SmartRecord> {
            std::vector<SmartRecord> records;
            return records;
        };
    }

    void InformationSchema::init_innodb_metrics() {
        FieldVec fields{
                {"NAME",            proto::STRING},
                {"SUBSYSTEM",       proto::STRING},
                {"COUNT",           proto::INT64},
                {"MAX_COUNT",       proto::INT64},
                {"MIN_COUNT",       proto::INT64},
                {"AVG_COUNT",       proto::DOUBLE},
                {"COUNT_RESET",     proto::INT64},
                {"MAX_COUNT_RESET", proto::INT64},
                {"MIN_COUNT_RESET", proto::INT64},
                {"AVG_COUNT_RESET", proto::DOUBLE},
                {"TIME_ENABLED",    proto::DATETIME},
                {"TIME_DISABLED",   proto::DATETIME},
                {"TIME_ELAPSED",    proto::INT64},
                {"TIME_RESET",      proto::DATETIME},
                {"STATUS",          proto::STRING},
                {"TYPE",            proto::STRING},
                {"COMMENT",         proto::STRING},
        };
        int64_t table_id = construct_table("INNODB_METRICS", fields);
        _calls[table_id] = [table_id](RuntimeState *state, std::vector<ExprNode *> &conditions) ->
                std::vector<SmartRecord> {
            std::vector<SmartRecord> records;
            return records;
        };
    }

    void InformationSchema::init_innodb_sys_columns() {
        FieldVec fields{
                {"TABLE_ID", proto::UINT64},
                {"NAME",     proto::STRING},
                {"POS",      proto::UINT64},
                {"MTYPE",    proto::INT32},
                {"PRTYPE",   proto::INT32},
                {"LEN",      proto::INT32},
        };
        int64_t table_id = construct_table("INNODB_SYS_COLUMNS", fields);
        _calls[table_id] = [table_id](RuntimeState *state, std::vector<ExprNode *> &conditions) ->
                std::vector<SmartRecord> {
            std::vector<SmartRecord> records;
            return records;
        };
    }

    void InformationSchema::init_innodb_sys_datafiles() {
        FieldVec fields{
                {"SPACE", proto::UINT32},
                {"PATH",  proto::STRING},
        };
        int64_t table_id = construct_table("INNODB_SYS_DATAFILES", fields);
        _calls[table_id] = [table_id](RuntimeState *state, std::vector<ExprNode *> &conditions) ->
                std::vector<SmartRecord> {
            std::vector<SmartRecord> records;
            return records;
        };
    }

    void InformationSchema::init_innodb_sys_fields() {
        FieldVec fields{
                {"INDEX_ID", proto::UINT64},
                {"NAME",     proto::STRING},
                {"POS",      proto::UINT32},
        };
        int64_t table_id = construct_table("INNODB_SYS_FIELDS", fields);
        _calls[table_id] = [table_id](RuntimeState *state, std::vector<ExprNode *> &conditions) ->
                std::vector<SmartRecord> {
            std::vector<SmartRecord> records;
            return records;
        };
    }

    void InformationSchema::init_innodb_sys_foreign() {
        FieldVec fields{
                {"ID",       proto::STRING},
                {"FOR_NAME", proto::STRING},
                {"REF_NAME", proto::STRING},
                {"N_COLS",   proto::UINT32},
                {"TYPE",     proto::UINT32},
        };
        int64_t table_id = construct_table("INNODB_SYS_FOREIGN", fields);
        _calls[table_id] = [table_id](RuntimeState *state, std::vector<ExprNode *> &conditions) ->
                std::vector<SmartRecord> {
            std::vector<SmartRecord> records;
            return records;
        };
    }

    void InformationSchema::init_innodb_sys_foreign_cols() {
        FieldVec fields{
                {"ID",           proto::STRING},
                {"FOR_COL_NAME", proto::STRING},
                {"REF_COL_NAME", proto::STRING},
                {"POS",          proto::UINT32},
        };
        int64_t table_id = construct_table("INNODB_SYS_FOREIGN_COLS", fields);
        _calls[table_id] = [table_id](RuntimeState *state, std::vector<ExprNode *> &conditions) ->
                std::vector<SmartRecord> {
            std::vector<SmartRecord> records;
            return records;
        };
    }

    void InformationSchema::init_innodb_sys_indexes() {
        FieldVec fields{
                {"INDEX_ID", proto::UINT64},
                {"NAME",     proto::STRING},
                {"TABLE_ID", proto::UINT64},
                {"TYPE",     proto::INT32},
                {"N_FIELDS", proto::INT32},
                {"PAGE_NO",  proto::INT32},
                {"SPACE",    proto::INT32},
        };
        int64_t table_id = construct_table("INNODB_SYS_INDEXES", fields);
        _calls[table_id] = [table_id](RuntimeState *state, std::vector<ExprNode *> &conditions) ->
                std::vector<SmartRecord> {
            std::vector<SmartRecord> records;
            return records;
        };
    }

    void InformationSchema::init_innodb_sys_tables() {
        FieldVec fields{
                {"TABLE_ID",      proto::UINT64},
                {"NAME",          proto::STRING},
                {"FLAG",          proto::INT32},
                {"N_COLS",        proto::INT32},
                {"SPACE",         proto::INT32},
                {"FILE_FORMAT",   proto::STRING},
                {"ROW_FORMAT",    proto::STRING},
                {"ZIP_PAGE_SIZE", proto::UINT32},
        };
        int64_t table_id = construct_table("INNODB_SYS_TABLES", fields);
        _calls[table_id] = [table_id](RuntimeState *state, std::vector<ExprNode *> &conditions) ->
                std::vector<SmartRecord> {
            std::vector<SmartRecord> records;
            return records;
        };
    }

    void InformationSchema::init_innodb_sys_tablespaces() {
        FieldVec fields{
                {"SPACE",         proto::UINT32},
                {"NAME",          proto::STRING},
                {"FLAG",          proto::UINT32},
                {"FILE_FORMAT",   proto::STRING},
                {"ROW_FORMAT",    proto::STRING},
                {"PAGE_SIZE",     proto::UINT32},
                {"ZIP_PAGE_SIZE", proto::UINT32},
        };
        int64_t table_id = construct_table("INNODB_SYS_TABLESPACES", fields);
        _calls[table_id] = [table_id](RuntimeState *state, std::vector<ExprNode *> &conditions) ->
                std::vector<SmartRecord> {
            std::vector<SmartRecord> records;
            return records;
        };
    }

    void InformationSchema::init_innodb_sys_tablestats() {
        FieldVec fields{
                {"TABLE_ID",          proto::UINT64},
                {"NAME",              proto::STRING},
                {"STATS_INITIALIZED", proto::STRING},
                {"NUM_ROWS",          proto::UINT64},
                {"CLUST_INDEX_SIZE",  proto::UINT64},
                {"OTHER_INDEX_SIZE",  proto::UINT64},
                {"MODIFIED_COUNTER",  proto::UINT64},
                {"AUTOINC",           proto::UINT64},
                {"REF_COUNT",         proto::INT32},
        };
        int64_t table_id = construct_table("INNODB_SYS_TABLESTATS", fields);
        _calls[table_id] = [table_id](RuntimeState *state, std::vector<ExprNode *> &conditions) ->
                std::vector<SmartRecord> {
            std::vector<SmartRecord> records;
            return records;
        };
    }

    void InformationSchema::init_innodb_trx() {
        FieldVec fields{
                {"trx_id",                     proto::STRING},
                {"trx_state",                  proto::STRING},
                {"trx_started",                proto::DATETIME},
                {"trx_requested_lock_id",      proto::STRING},
                {"trx_wait_started",           proto::DATETIME},
                {"trx_weight",                 proto::UINT64},
                {"trx_mysql_thread_id",        proto::UINT64},
                {"trx_query",                  proto::STRING},
                {"trx_operation_state",        proto::STRING},
                {"trx_tables_in_use",          proto::UINT64},
                {"trx_tables_locked",          proto::UINT64},
                {"trx_lock_structs",           proto::UINT64},
                {"trx_lock_memory_bytes",      proto::UINT64},
                {"trx_rows_locked",            proto::UINT64},
                {"trx_rows_modified",          proto::UINT64},
                {"trx_concurrency_tickets",    proto::UINT64},
                {"trx_isolation_level",        proto::STRING},
                {"trx_unique_checks",          proto::INT32},
                {"trx_foreign_key_checks",     proto::INT32},
                {"trx_last_foreign_key_error", proto::STRING},
                {"trx_adaptive_hash_latched",  proto::INT32},
                {"trx_adaptive_hash_timeout",  proto::UINT64},
                {"trx_is_read_only",           proto::INT32},
                {"trx_autocommit_non_locking", proto::INT32},
        };
        int64_t table_id = construct_table("INNODB_TRX", fields);
        _calls[table_id] = [table_id](RuntimeState *state, std::vector<ExprNode *> &conditions) ->
                std::vector<SmartRecord> {
            std::vector<SmartRecord> records;
            return records;
        };
    }

    void InformationSchema::init_optimizer_trace() {
        FieldVec fields{
                {"QUERY",                             proto::STRING},
                {"TRACE",                             proto::STRING},
                {"MISSING_BYTES_BEYOND_MAX_MEM_SIZE", proto::INT32},
                {"INSUFFICIENT_PRIVILEGES",           proto::INT32},
        };
        int64_t table_id = construct_table("OPTIMIZER_TRACE", fields);
        _calls[table_id] = [table_id](RuntimeState *state, std::vector<ExprNode *> &conditions) ->
                std::vector<SmartRecord> {
            std::vector<SmartRecord> records;
            return records;
        };
    }

    void InformationSchema::init_parameters() {
        FieldVec fields{
                {"SPECIFIC_CATALOG",         proto::STRING},
                {"SPECIFIC_SCHEMA",          proto::STRING},
                {"SPECIFIC_NAME",            proto::STRING},
                {"ORDINAL_POSITION",         proto::INT32},
                {"PARAMETER_MODE",           proto::STRING},
                {"PARAMETER_NAME",           proto::STRING},
                {"DATA_TYPE",                proto::STRING},
                {"CHARACTER_MAXIMUM_LENGTH", proto::INT32},
                {"CHARACTER_OCTET_LENGTH",   proto::INT32},
                {"NUMERIC_PRECISION",        proto::UINT64},
                {"NUMERIC_SCALE",            proto::INT32},
                {"DATETIME_PRECISION",       proto::UINT64},
                {"CHARACTER_SET_NAME",       proto::STRING},
                {"COLLATION_NAME",           proto::STRING},
                {"DTD_IDENTIFIER",           proto::STRING},
                {"ROUTINE_TYPE",             proto::STRING},
        };
        int64_t table_id = construct_table("PARAMETERS", fields);
        _calls[table_id] = [table_id](RuntimeState *state, std::vector<ExprNode *> &conditions) ->
                std::vector<SmartRecord> {
            std::vector<SmartRecord> records;
            return records;
        };
    }

    void InformationSchema::init_partitions() {
        FieldVec fields{
                {"TABLE_CATALOG",                 proto::STRING},
                {"TABLE_SCHEMA",                  proto::STRING},
                {"TABLE_NAME",                    proto::STRING},
                {"PARTITION_NAME",                proto::STRING},
                {"SUBPARTITION_NAME",             proto::STRING},
                {"PARTITION_ORDINAL_POSITION",    proto::UINT64},
                {"SUBPARTITION_ORDINAL_POSITION", proto::UINT64},
                {"PARTITION_METHOD",              proto::STRING},
                {"SUBPARTITION_METHOD",           proto::STRING},
                {"PARTITION_EXPRESSION",          proto::STRING},
                {"SUBPARTITION_EXPRESSION",       proto::STRING},
                {"PARTITION_DESCRIPTION",         proto::STRING},
                {"TABLE_ROWS",                    proto::UINT64},
                {"AVG_ROW_LENGTH",                proto::UINT64},
                {"DATA_LENGTH",                   proto::UINT64},
                {"MAX_DATA_LENGTH",               proto::UINT64},
                {"INDEX_LENGTH",                  proto::UINT64},
                {"DATA_FREE",                     proto::UINT64},
                {"CREATE_TIME",                   proto::DATETIME},
                {"UPDATE_TIME",                   proto::DATETIME},
                {"CHECK_TIME",                    proto::DATETIME},
                {"CHECKSUM",                      proto::UINT64},
                {"PARTITION_COMMENT",             proto::STRING},
                {"NODEGROUP",                     proto::STRING},
                {"TABLESPACE_NAME",               proto::STRING},
        };
        int64_t table_id = construct_table("PARTITIONS", fields);
        _calls[table_id] = [table_id](RuntimeState *state, std::vector<ExprNode *> &conditions) ->
                std::vector<SmartRecord> {
            std::vector<SmartRecord> records;
            return records;
        };
    }

    void InformationSchema::init_plugins() {
        FieldVec fields{
                {"PLUGIN_NAME",            proto::STRING},
                {"PLUGIN_VERSION",         proto::STRING},
                {"PLUGIN_STATUS",          proto::STRING},
                {"PLUGIN_TYPE",            proto::STRING},
                {"PLUGIN_TYPE_VERSION",    proto::STRING},
                {"PLUGIN_LIBRARY",         proto::STRING},
                {"PLUGIN_LIBRARY_VERSION", proto::STRING},
                {"PLUGIN_AUTHOR",          proto::STRING},
                {"PLUGIN_DESCRIPTION",     proto::STRING},
                {"PLUGIN_LICENSE",         proto::STRING},
                {"LOAD_OPTION",            proto::STRING},
        };
        int64_t table_id = construct_table("PLUGINS", fields);
        _calls[table_id] = [table_id](RuntimeState *state, std::vector<ExprNode *> &conditions) ->
                std::vector<SmartRecord> {
            std::vector<SmartRecord> records;
            return records;
        };
    }

    void InformationSchema::init_processlist() {
        FieldVec fields{
                {"ID",      proto::UINT64},
                {"USER",    proto::STRING},
                {"HOST",    proto::STRING},
                {"DB",      proto::STRING},
                {"COMMAND", proto::STRING},
                {"TIME",    proto::INT32},
                {"STATE",   proto::STRING},
                {"INFO",    proto::STRING},
        };
        int64_t table_id = construct_table("PROCESSLIST", fields);
        _calls[table_id] = [table_id](RuntimeState *state, std::vector<ExprNode *> &conditions) ->
                std::vector<SmartRecord> {
            std::vector<SmartRecord> records;
            return records;
        };
    }

    void InformationSchema::init_profiling() {
        FieldVec fields{
                {"QUERY_ID",            proto::INT32},
                {"SEQ",                 proto::INT32},
                {"STATE",               proto::STRING},
                {"DURATION",            proto::DOUBLE},
                {"CPU_USER",            proto::DOUBLE},
                {"CPU_SYSTEM",          proto::DOUBLE},
                {"CONTEXT_VOLUNTARY",   proto::INT32},
                {"CONTEXT_INVOLUNTARY", proto::INT32},
                {"BLOCK_OPS_IN",        proto::INT32},
                {"BLOCK_OPS_OUT",       proto::INT32},
                {"MESSAGES_SENT",       proto::INT32},
                {"MESSAGES_RECEIVED",   proto::INT32},
                {"PAGE_FAULTS_MAJOR",   proto::INT32},
                {"PAGE_FAULTS_MINOR",   proto::INT32},
                {"SWAPS",               proto::INT32},
                {"SOURCE_FUNCTION",     proto::STRING},
                {"SOURCE_FILE",         proto::STRING},
                {"SOURCE_LINE",         proto::INT32},
        };
        int64_t table_id = construct_table("PROFILING", fields);
        _calls[table_id] = [table_id](RuntimeState *state, std::vector<ExprNode *> &conditions) ->
                std::vector<SmartRecord> {
            std::vector<SmartRecord> records;
            return records;
        };
    }

    void InformationSchema::init_schema_privileges() {
        FieldVec fields{
                {"GRANTEE",        proto::STRING},
                {"TABLE_CATALOG",  proto::STRING},
                {"TABLE_SCHEMA",   proto::STRING},
                {"PRIVILEGE_TYPE", proto::STRING},
                {"IS_GRANTABLE",   proto::STRING},
        };
        int64_t table_id = construct_table("SCHEMA_PRIVILEGES", fields);
        _calls[table_id] = [table_id](RuntimeState *state, std::vector<ExprNode *> &conditions) ->
                std::vector<SmartRecord> {
            std::vector<SmartRecord> records;
            return records;
        };
    }

    void InformationSchema::init_session_status() {
        FieldVec fields{
                {"VARIABLE_NAME",  proto::STRING},
                {"VARIABLE_VALUE", proto::STRING},
        };
        int64_t table_id = construct_table("SESSION_STATUS", fields);
        _calls[table_id] = [table_id](RuntimeState *state, std::vector<ExprNode *> &conditions) ->
                std::vector<SmartRecord> {
            std::vector<SmartRecord> records;
            return records;
        };
    }

    void InformationSchema::init_session_variables() {
        FieldVec fields{
                {"VARIABLE_NAME",  proto::STRING},
                {"VARIABLE_VALUE", proto::STRING},
        };
        int64_t table_id = construct_table("SESSION_VARIABLES", fields);
        _calls[table_id] = [table_id](RuntimeState *state, std::vector<ExprNode *> &conditions) ->
                std::vector<SmartRecord> {
            std::vector<SmartRecord> records;
            return records;
        };
    }

    void InformationSchema::init_table_constraints() {
        FieldVec fields{
                {"CONSTRAINT_CATALOG", proto::STRING},
                {"CONSTRAINT_SCHEMA",  proto::STRING},
                {"CONSTRAINT_NAME",    proto::STRING},
                {"TABLE_SCHEMA",       proto::STRING},
                {"TABLE_NAME",         proto::STRING},
                {"CONSTRAINT_TYPE",    proto::STRING},
        };
        int64_t table_id = construct_table("TABLE_CONSTRAINTS", fields);
        _calls[table_id] = [table_id](RuntimeState *state, std::vector<ExprNode *> &conditions) ->
                std::vector<SmartRecord> {
            std::vector<SmartRecord> records;
            return records;
        };
    }

    void InformationSchema::init_table_privileges() {
        FieldVec fields{
                {"GRANTEE",        proto::STRING},
                {"TABLE_CATALOG",  proto::STRING},
                {"TABLE_SCHEMA",   proto::STRING},
                {"TABLE_NAME",     proto::STRING},
                {"PRIVILEGE_TYPE", proto::STRING},
                {"IS_GRANTABLE",   proto::STRING},
        };
        int64_t table_id = construct_table("TABLE_PRIVILEGES", fields);
        _calls[table_id] = [table_id](RuntimeState *state, std::vector<ExprNode *> &conditions) ->
                std::vector<SmartRecord> {
            std::vector<SmartRecord> records;
            return records;
        };
    }

    void InformationSchema::init_tablespaces() {
        FieldVec fields{
                {"TABLESPACE_NAME",    proto::STRING},
                {"ENGINE",             proto::STRING},
                {"TABLESPACE_TYPE",    proto::STRING},
                {"LOGFILE_GROUP_NAME", proto::STRING},
                {"EXTENT_SIZE",        proto::UINT64},
                {"AUTOEXTEND_SIZE",    proto::UINT64},
                {"MAXIMUM_SIZE",       proto::UINT64},
                {"NODEGROUP_ID",       proto::UINT64},
                {"TABLESPACE_COMMENT", proto::STRING},
        };
        int64_t table_id = construct_table("TABLESPACES", fields);
        _calls[table_id] = [table_id](RuntimeState *state, std::vector<ExprNode *> &conditions) ->
                std::vector<SmartRecord> {
            std::vector<SmartRecord> records;
            return records;
        };
    }

    void InformationSchema::init_user_privileges() {
        FieldVec fields{
                {"GRANTEE",        proto::STRING},
                {"TABLE_CATALOG",  proto::STRING},
                {"PRIVILEGE_TYPE", proto::STRING},
                {"IS_GRANTABLE",   proto::STRING},
        };
        int64_t table_id = construct_table("USER_PRIVILEGES", fields);
        _calls[table_id] = [table_id](RuntimeState *state, std::vector<ExprNode *> &conditions) ->
                std::vector<SmartRecord> {
            std::vector<SmartRecord> records;
            return records;
        };
    }
} // namespace EA
