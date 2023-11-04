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


#include <ctype.h>
#include "elasticann/protocol/show_helper.h"
#include "elasticann/protocol/network_server.h"
#include "elasticann/common/store_interact.h"
#include "elasticann/logical_plan/query_context.h"
#include "re2/re2.h"
#include "turbo/strings/str_split.h"

namespace EA {

    void ShowHelper::init() {
        _calls[SQL_SHOW_ABNORMAL_REGIONS] = std::bind(&ShowHelper::_show_abnormal_regions,
                                                      this, std::placeholders::_1, std::placeholders::_2);
        _calls[SQL_SHOW_DATABASES] = std::bind(&ShowHelper::_show_databases,
                                               this, std::placeholders::_1, std::placeholders::_2);
        _calls[SQL_SHOW_TABLES] = std::bind(&ShowHelper::_show_tables,
                                            this, std::placeholders::_1, std::placeholders::_2);
        _calls[SQL_SHOW_CREATE_TABLE] = std::bind(&ShowHelper::_show_create_table,
                                                  this, std::placeholders::_1, std::placeholders::_2);
        _calls[SQL_SHOW_COLLATION] = std::bind(&ShowHelper::_show_collation,
                                               this, std::placeholders::_1, std::placeholders::_2);
        _calls[SQL_SHOW_SOCKET] = std::bind(&ShowHelper::_show_socket,
                                            this, std::placeholders::_1, std::placeholders::_2);
        _calls[SQL_SHOW_WARNINGS] = std::bind(&ShowHelper::_show_warnings,
                                              this, std::placeholders::_1, std::placeholders::_2);
        _calls[SQL_SHOW_PROCESSLIST] = std::bind(&ShowHelper::_show_processlist,
                                                 this, std::placeholders::_1, std::placeholders::_2);
        _calls[SQL_SHOW_COST] = std::bind(&ShowHelper::_show_cost,
                                          this, std::placeholders::_1, std::placeholders::_2);
        _calls[SQL_SHOW_FULL_COLUMNS] = std::bind(&ShowHelper::_show_full_columns,
                                                  this, std::placeholders::_1, std::placeholders::_2);
        _calls[SQL_SHOW_FULL_TABLES] = std::bind(&ShowHelper::_show_full_tables,
                                                 this, std::placeholders::_1, std::placeholders::_2);
        _calls[SQL_SHOW_SCHEMA_CONF] = std::bind(&ShowHelper::_show_schema_conf,
                                                 this, std::placeholders::_1, std::placeholders::_2);
        _calls[SQL_SHOW_TABLE_STATUS] = std::bind(&ShowHelper::_show_table_status,
                                                  this, std::placeholders::_1, std::placeholders::_2);
        _calls[SQL_SHOW_REGION] = std::bind(&ShowHelper::_show_region,
                                            this, std::placeholders::_1, std::placeholders::_2);
        _calls[SQL_SHOW_STORE_REGION] = std::bind(&ShowHelper::_show_store_region,
                                                  this, std::placeholders::_1, std::placeholders::_2);
        _calls[SQL_SHOW_VARIABLES] = std::bind(&ShowHelper::_show_variables,
                                               this, std::placeholders::_1, std::placeholders::_2);
        _calls[SQL_SHOW_STATUS] = std::bind(&ShowHelper::_show_variables,
                                            this, std::placeholders::_1, std::placeholders::_2);
        _calls[SQL_SHOW_VIRTUAL_INDEX] = std::bind(&ShowHelper::_show_virtual_index,
                                                   this, std::placeholders::_1, std::placeholders::_2);
        _calls[SQL_SHOW_NAMESPACE] = std::bind(&ShowHelper::_handle_client_query_template_dispatch,
                                               this, std::placeholders::_1, std::placeholders::_2);
        _calls[SQL_SHOW_META] = std::bind(&ShowHelper::_handle_client_query_template_dispatch,
                                          this, std::placeholders::_1, std::placeholders::_2);
        _calls[SQL_DISABLE_INDEXS] = std::bind(&ShowHelper::_handle_client_query_template_dispatch,
                                               this, std::placeholders::_1, std::placeholders::_2);
        _calls[SQL_SHOW_PRIVILEGE] = std::bind(&ShowHelper::_show_privilege,
                                               this, std::placeholders::_1, std::placeholders::_2);
        _calls[SQL_SHOW_USER] = std::bind(&ShowHelper::_show_user,
                                          this, std::placeholders::_1, std::placeholders::_2);
        _calls[SQL_SHOW_STORE_TXN] = std::bind(&ShowHelper::_show_store_txn,
                                               this, std::placeholders::_1, std::placeholders::_2);
        _calls[SQL_SHOW_DDL_WORK] = std::bind(&ShowHelper::_show_ddl_work,
                                              this, std::placeholders::_1, std::placeholders::_2);
        _calls[SQL_SHOW_GLOBAL_DDL_WORK] = std::bind(&ShowHelper::_show_global_ddl_work,
                                                     this, std::placeholders::_1, std::placeholders::_2);
        _calls[SQL_SHOW_DIFF_REGION_SIZE] = std::bind(&ShowHelper::_show_diff_region_size,
                                                      this, std::placeholders::_1, std::placeholders::_2);
        _calls[SQL_SHOW_NETWORK_SEGMENT] = std::bind(&ShowHelper::_show_network_segment,
                                                     this, std::placeholders::_1, std::placeholders::_2);
        _calls[SQL_SHOW_SWITCH] = std::bind(&ShowHelper::_show_switch,
                                            this, std::placeholders::_1, std::placeholders::_2);
        _calls[SQL_SHOW_ALL_TABLES] = std::bind(&ShowHelper::_show_all_tables,
                                                this, std::placeholders::_1, std::placeholders::_2);
        _calls[SQL_SHOW_BINLOGS_INFO] = std::bind(&ShowHelper::_show_binlogs_info, this, std::placeholders::_1,
                                                  std::placeholders::_2);
        _calls[SQL_SHOW_INSTANCE_PARAM] = std::bind(&ShowHelper::_show_instance_param,
                                                    this, std::placeholders::_1, std::placeholders::_2);
        _calls[SQL_SHOW_ENGINES] = std::bind(&ShowHelper::_show_engines,
                                             this, std::placeholders::_1, std::placeholders::_2);
        _calls[SQL_SHOW_CHARSET] = std::bind(&ShowHelper::_show_charset,
                                             this, std::placeholders::_1, std::placeholders::_2);
        _calls[SQL_SHOW_CHARACTER_SET] = std::bind(&ShowHelper::_show_charset,
                                                   this, std::placeholders::_1, std::placeholders::_2);
        _calls[SQL_SHOW_INDEX] = std::bind(&ShowHelper::_show_index,
                                           this, std::placeholders::_1, std::placeholders::_2);
        _calls[SQL_SHOW_INDEXES] = std::bind(&ShowHelper::_show_index,
                                             this, std::placeholders::_1, std::placeholders::_2);
        _calls[SQL_SHOW_KEYS] = std::bind(&ShowHelper::_show_index,
                                          this, std::placeholders::_1, std::placeholders::_2);
        _calls[SQL_SHOW_PARTITION_TABLE] = std::bind(&ShowHelper::_show_partition_table,
                                                     this, std::placeholders::_1, std::placeholders::_2);
        _calls[SQL_SHOW_ABNORMAL_SWITCH] = std::bind(&ShowHelper::_show_abnormal_switch,
                                                     this, std::placeholders::_1, std::placeholders::_2);
        _wrapper = MysqlWrapper::get_instance();
    }

    bool ShowHelper::execute(const SmartSocket &client) {
        if (client == nullptr || client->query_ctx == nullptr) {
            TLOG_ERROR("param invalid");
            //client->state = STATE_ERROR;
            return false;
        }

        std::vector<std::string> split_vec = turbo::StrSplit(client->query_ctx->sql, turbo::ByAnyChar(" \t\n\r"),
                                                             turbo::SkipEmpty());
        if (split_vec.size() < 2) {
            _wrapper->make_simple_ok_packet(client);
            client->state = STATE_READ_QUERY_RESULT;
            return true;
        }
        std::transform(split_vec[1].begin(), split_vec[1].end(), split_vec[1].begin(), ::tolower);
        std::string &key = split_vec[1];
        if (key == "full") {
            if (split_vec.size() > 2 && turbo::EqualsIgnoreCase(split_vec[2], "tables")) {
                key = SQL_SHOW_FULL_TABLES;
            } else if (split_vec.size() > 2 &&
                       (turbo::EqualsIgnoreCase(split_vec[2], "columns") ||
                        turbo::EqualsIgnoreCase(split_vec[2], "fields"))) {
                key = SQL_SHOW_FULL_COLUMNS;
            } else {
                _wrapper->make_simple_ok_packet(client);
                client->state = STATE_READ_QUERY_RESULT;
                return true;
            }
        }
        auto iter = _calls.find(key);
        if (iter == _calls.end() || iter->second == nullptr) {
            _wrapper->make_simple_ok_packet(client);
            client->state = STATE_READ_QUERY_RESULT;
            return true;
        }
        return iter->second(client, split_vec);
    }

    bool ShowHelper::_show_abnormal_regions(const SmartSocket &client, const std::vector<std::string> &split_vec_arg) {
        if (client == nullptr || client->query_ctx == nullptr) {
            TLOG_ERROR("param invalid");
            //client->state = STATE_ERROR;
            return false;
        }

        std::string resource_tag;
        auto split_vec = split_vec_arg;
        bool unhealthy = false;
        bool is_learner = false;
        if (split_vec.back() == "unhealthy") {
            unhealthy = true;
            split_vec.pop_back();
        } else if (split_vec.back() == "learner") {
            is_learner = true;
            split_vec.pop_back();
        }
        if (split_vec.size() == 4) {
            resource_tag = split_vec[3];
        } else if (split_vec.size() < 3) {
            client->state = STATE_ERROR;
            return false;
        }

        proto::QueryRequest req;
        req.set_op_type(proto::QUERY_REGION_PEER_STATUS);
        if (is_learner) {
            req.set_op_type(proto::QUERY_REGION_LEARNER_STATUS);
        }
        if (resource_tag != "") {
            req.set_resource_tag(resource_tag);
        }
        proto::QueryResponse res;
        MetaServerInteract::get_instance()->send_request("query", req, res);
        //TLOG_WARN("res:{}", res.ShortDebugString().c_str());
        std::vector<std::vector<std::string> > rows;
        rows.reserve(10);
        size_t max_size = 0;
        for (auto &region_info: res.region_status_infos()) {
            if (unhealthy && region_info.is_healthy()) {
                continue;
            }
            std::vector<std::string> row;
            row.reserve(5);
            row.emplace_back(region_info.table_name());
            row.emplace_back(std::to_string(region_info.table_id()));
            row.emplace_back(std::to_string(region_info.region_id()));
            if (region_info.is_healthy()) {
                row.emplace_back("healthy");
            } else {
                row.emplace_back("unhealthy");
            }
            for (auto &peer_info: region_info.peer_status_infos()) {
                row.emplace_back(peer_info.peer_id() + "@" + proto::PeerStatus_Name(peer_info.peer_status()));
            }
            if (max_size < row.size()) {
                max_size = row.size();
            }
            rows.emplace_back(row);
        }

        for (auto &row: rows) {
            if (row.size() < max_size) {
                for (size_t i = 0; i < max_size - row.size(); i++) {
                    row.emplace_back("nullptr");
                }
            }
        }

        std::vector<std::string> names = {"table_name", "table_id", "region_id", "region_status"};
        for (size_t i = 1; max_size > 4 && i <= max_size - 4; i++) {
            names.emplace_back("peer" + std::to_string(i));
        }

        std::vector<ResultField> fields;
        fields.reserve(4);
        for (auto &name: names) {
            ResultField field;
            field.name = name;
            field.type = MYSQL_TYPE_STRING;
            fields.emplace_back(field);
        }
        // Make mysql packet.
        if (_make_common_resultset_packet(client, fields, rows) != 0) {
            TLOG_ERROR("{}, Failed to make result packet.", *client);
            _wrapper->make_err_packet(client, ER_MAKE_RESULT_PACKET, "Failed to make result packet.");
            client->state = STATE_ERROR;
            return false;
        }
        client->state = STATE_READ_QUERY_RESULT;
        return true;
    }

    bool ShowHelper::_show_collation(const SmartSocket &client, const std::vector<std::string> &split_vec) {
        if (client == nullptr) {
            TLOG_ERROR("param invalid");
            //client->state = STATE_ERROR;
            return false;
        }

        // Make result info.
        std::vector<ResultField> fields;
        fields.reserve(6);
        do {
            ResultField field;
            field.name = "Collation";
            field.type = MYSQL_TYPE_VARCHAR;
            field.length = 1024;
            fields.emplace_back(field);
        } while (0);
        do {
            ResultField field;
            field.name = "Charset";
            field.type = MYSQL_TYPE_VARCHAR;
            field.length = 1024;
            fields.emplace_back(field);
        } while (0);
        do {
            ResultField field;
            field.name = "Id";
            field.type = MYSQL_TYPE_LONGLONG;
            fields.emplace_back(field);
        } while (0);
        do {
            ResultField field;
            field.name = "Default";
            field.type = MYSQL_TYPE_VARCHAR;
            field.length = 1024;
            fields.emplace_back(field);
        } while (0);
        do {
            ResultField field;
            field.name = "Compiled";
            field.type = MYSQL_TYPE_VARCHAR;
            field.length = 1024;
            fields.emplace_back(field);
        } while (0);
        do {
            ResultField field;
            field.name = "Sortlen";
            field.type = MYSQL_TYPE_VARCHAR;
            field.length = 1024;
            fields.emplace_back(field);
        } while (0);

        // Make rows.
        std::vector<std::vector<std::string> > rows;
        std::vector<std::string> row;
        row.emplace_back("gbk_chinese_ci");
        row.emplace_back("gbk");
        row.emplace_back("28");
        row.emplace_back("Yes");
        row.emplace_back("Yes");
        row.emplace_back("1");
        rows.emplace_back(row);
        std::vector<std::string> row1;
        row1.emplace_back("gbk_bin");
        row1.emplace_back("gbk");
        row1.emplace_back("87");
        row1.emplace_back("   ");
        row1.emplace_back("Yes");
        row1.emplace_back("1");
        rows.emplace_back(row1);
        rows.push_back({"utf8_general_ci", "utf8", "33", "Yes", "Yes", "1"});
        rows.push_back({"utf8_bin", "utf8", "83", " ", "Yes", "1"});
        rows.push_back({"binary", "binary", "63", " ", "Yes", "1"});

        // Make mysql packet.
        if (_make_common_resultset_packet(client, fields, rows) != 0) {
            TLOG_ERROR("{}, Failed to package mysql common result.", *client);
            _wrapper->make_err_packet(client, ER_MAKE_RESULT_PACKET, "Failed to make result packet.");
            client->state = STATE_ERROR;
            return false;
        }
        client->state = STATE_READ_QUERY_RESULT;
        return true;
    }

    bool ShowHelper::_show_databases(const SmartSocket &client, const std::vector<std::string> &split_vec) {
        if (client == nullptr || !client->user_info) {
            TLOG_ERROR("param invalid");
            //client->state = STATE_ERROR;
            return false;
        }

        // Make fields.
        std::vector<ResultField> fields;
        fields.reserve(1);
        do {
            ResultField field;
            field.name = "Database";
            field.type = MYSQL_TYPE_VARCHAR;
            field.length = 1024;
            fields.emplace_back(field);
        } while (0);

        // Make rows.
        std::vector<std::vector<std::string> > rows;
        rows.reserve(10);
        SchemaFactory *factory = SchemaFactory::get_instance();
        if (!factory) {
            TLOG_ERROR("param invalid");
            return false;
        }
        std::vector<std::string> dbs = factory->get_db_list(client->user_info->all_database);
        for (uint32_t cnt = 0; cnt < dbs.size(); ++cnt) {
            std::vector<std::string> row;
            row.emplace_back(dbs[cnt]);
            rows.emplace_back(row);
        }

        // Make mysql packet.
        if (_make_common_resultset_packet(client, fields, rows) != 0) {
            TLOG_ERROR("{}, Failed to make result packet.", *client);
            _wrapper->make_err_packet(client, ER_MAKE_RESULT_PACKET, "Failed to make result packet.");
            client->state = STATE_ERROR;
            return false;
        }
        client->state = STATE_READ_QUERY_RESULT;
        return true;
    }

    bool ShowHelper::_show_tables(const SmartSocket &client, const std::vector<std::string> &split_vec) {
        SchemaFactory *factory = SchemaFactory::get_instance();
        if (client == nullptr || client->query_ctx == nullptr || client->user_info == nullptr || factory == nullptr) {
            TLOG_ERROR("param invalid");
            //client->state = STATE_ERROR;
            return false;
        }

        std::string namespace_ = client->user_info->namespace_;
        std::string db = client->current_db;
        if (split_vec.size() == 4) {
            db = split_vec[3];
        }
        if (db == "") {
            TLOG_WARN("no database selected");
            _wrapper->make_err_packet(client, ER_NO_DB_ERROR, "No database selected");
            client->state = STATE_READ_QUERY_RESULT;
            return false;
        }
        if (db == "information_schema") {
            namespace_ = "INTERNAL";
        }

        // Make fields.
        std::vector<ResultField> fields;
        fields.reserve(1);
        do {
            ResultField field;
            field.name = "Tables_in_" + db;
            field.db = db;
            field.type = MYSQL_TYPE_VARCHAR;
            field.length = 1024;
            fields.emplace_back(field);
        } while (0);

        // Make rows.
        std::vector<std::vector<std::string> > rows;
        rows.reserve(10);
        std::vector<std::string> tables = factory->get_table_list(
                namespace_, db, client->user_info.get());
        for (uint32_t cnt = 0; cnt < tables.size(); ++cnt) {
            //TLOG_INFO("table:{}", tables[cnt].c_str());
            std::vector<std::string> row;
            row.emplace_back(tables[cnt]);
            rows.emplace_back(row);
        }

        // Make mysql packet.
        if (_make_common_resultset_packet(client, fields, rows) != 0) {
            TLOG_ERROR("{}, Failed to make result packet.", *client);
            _wrapper->make_err_packet(client, ER_MAKE_RESULT_PACKET, "{}", client->query_ctx->sql.c_str());
            client->state = STATE_ERROR;
            return false;
        }
        client->state = STATE_READ_QUERY_RESULT;
        return true;
    }

    bool ShowHelper::_show_create_table(const SmartSocket &client, const std::vector<std::string> &split_vec) {
        SchemaFactory *factory = SchemaFactory::get_instance();
        if (client == nullptr || client->query_ctx == nullptr || client->user_info == nullptr || factory == nullptr) {
            TLOG_ERROR("param invalid");
            //client->state = STATE_ERROR;
            return false;
        }
        static std::map<proto::PrimitiveType, std::string> type_map = {
                {proto::BOOL,      "boolean"},
                {proto::INT8,      "tinyint(4)"},
                {proto::UINT8,     "tinyint(4) unsigned"},
                {proto::INT16,     "smallint(6)"},
                {proto::UINT16,    "smallint(6) unsigned"},
                {proto::INT32,     "int(10)"},
                {proto::UINT32,    "int(10) unsigned"},
                {proto::INT64,     "bigint(20)"},
                {proto::UINT64,    "bigint(20) unsigned"},
                {proto::FLOAT,     "float"},
                {proto::DOUBLE,    "double"},
                {proto::STRING,    "varchar(1024)"},
                {proto::DATETIME,  "DATETIME"},
                {proto::TIME,      "TIME"},
                {proto::TIMESTAMP, "TIMESTAMP"},
                {proto::DATE,      "DATE"},
                {proto::HLL,       "HLL"},
                {proto::BITMAP,    "BITMAP"},
                {proto::TDIGEST,   "TDIGEST"},
        };
        static std::map<proto::IndexType, std::string> index_map = {
                {proto::I_PRIMARY,  "PRIMARY KEY"},
                {proto::I_UNIQ,     "UNIQUE KEY"},
                {proto::I_KEY,      "KEY"},
                {proto::I_FULLTEXT, "FULLTEXT KEY"},
        };
        static std::map<proto::Charset, std::string> charset_map = {
                {proto::UTF8, "utf8"},
                {proto::GBK,  "gbk"},
        };
        // Make fields.
        std::vector<ResultField> fields;
        fields.reserve(2);
        do {
            ResultField field;
            field.name = "Table";
            field.type = MYSQL_TYPE_VARCHAR;
            field.length = 1024;
            fields.emplace_back(field);
        } while (0);
        do {
            ResultField field;
            field.name = "Create Table";
            field.type = MYSQL_TYPE_VARCHAR;
            field.length = 10240;
            fields.emplace_back(field);
        } while (0);

        std::string db = client->current_db;
        std::string table;
        if (split_vec.size() == 4) {
            std::string db_table = split_vec[3];
            std::string::size_type position = db_table.find_first_of('.');
            if (position == std::string::npos) {
                // `table_name`
                table = remove_quote(db_table.c_str(), '`');
            } else {
                // `db_name`.`table_name`
                db = remove_quote(db_table.substr(0, position).c_str(), '`');
                table = remove_quote(db_table.substr(position + 1,
                                                     db_table.length() - position - 1).c_str(), '`');
            }
        } else {
            client->state = STATE_ERROR;
            return false;
        }

        std::string namespace_ = client->user_info->namespace_;
        if (db == "information_schema") {
            namespace_ = "INTERNAL";
        }
        std::string full_name = namespace_ + "." + db + "." + table;
        int64_t table_id = -1;
        if (factory->get_table_id(full_name, table_id) != 0) {
            client->state = STATE_ERROR;
            client->query_ctx->stat_info.error_code = ER_NO_SUCH_TABLE;
            client->query_ctx->stat_info.error_msg << "Table '" << db << "."
                                                   << table << "' not exist";
            return false;
        }
        // Make rows.
        std::vector<std::vector<std::string> > rows;
        std::vector<std::string> row;
        row.emplace_back(table);
        std::ostringstream oss;
        TableInfo info = factory->get_table_info(table_id);
        oss << "CREATE TABLE `" << table << "` (\n";
        for (auto &field: info.fields) {
            if (field.deleted) {
                continue;
            }
            oss << "  " << "`" << field.short_name << "` ";
            oss << type_map[field.type] << " ";
            oss << (field.can_null ? "nullptr " : "NOT nullptr ");
            if (!field.default_expr_value.is_null()) {
                oss << "DEFAULT ";
                if (field.default_value == "(current_timestamp())") {
                    oss << "CURRENT_TIMESTAMP ";
                } else {
                    oss << "'" << field.default_value << "' ";
                }
            }
            if (!field.on_update_value.empty()) {
                if (field.on_update_value == "(current_timestamp())") {
                    oss << "ON UPDATE " << "CURRENT_TIMESTAMP ";
                }
            }
            oss << (field.auto_inc ? "AUTO_INCREMENT " : "");
            if (!field.comment.empty()) {
                oss << "COMMENT '" << field.comment << "'";
            }
            oss << ",\n";
        }
        uint32_t index_idx = 0;
        for (auto &index_id: info.indices) {
            IndexInfo index_info = factory->get_index_info(index_id);
            if (index_info.index_hint_status == proto::IHS_DISABLE && index_info.state == proto::IS_DELETE_LOCAL) {
                if (++index_idx == info.indices.size()) { // trim ",\n" to "\n"
                    long curPos = oss.tellp();
                    oss.seekp(curPos - 2);
                    oss << "\n";
                }
                continue;
            }
            if (index_info.is_global) {
                oss << " " << index_map[index_info.type] << " GLOBAL ";
            } else if (index_info.type == proto::I_PRIMARY || index_info.type == proto::I_FULLTEXT) {
                oss << " " << index_map[index_info.type] << " ";
            } else {
                oss << "  " << index_map[index_info.type] << " LOCAL ";
            }
            if (index_info.index_hint_status == proto::IHS_VIRTUAL) {
                oss << "VIRTUAL ";
            }
            if (index_info.type != proto::I_PRIMARY) {
                std::vector<std::string> split_vec = turbo::StrSplit(index_info.name,
                                                                     turbo::ByChar('.'));
                oss << "`" << split_vec[split_vec.size() - 1] << "` ";
            }
            oss << "(";
            uint32_t field_idx = 0;
            for (auto &field: index_info.fields) {
                std::vector<std::string> split_vec = turbo::StrSplit(field.name,
                                                                     turbo::ByChar('.'));
                if (++field_idx < index_info.fields.size()) {
                    oss << "`" << split_vec[split_vec.size() - 1] << "`,";
                } else {
                    oss << "`" << split_vec[split_vec.size() - 1] << "`";
                }
            }
            oss << ") COMMENT '{\"index_state\":\"";
            oss << proto::IndexState_Name(index_info.state) << "\", ";
            if (index_info.type == proto::I_FULLTEXT) {
                oss << "\"segment_type\":\"" << proto::SegmentType_Name(index_info.segment_type) << "\", ";
                oss << "\"storage_type\":\"" << proto::StorageType_Name(index_info.storage_type) << "\", ";
            }
            oss << "\"hint_status\":\"" << proto::IndexHintStatus_Name(index_info.index_hint_status) << "\"}'";
            if (++index_idx < info.indices.size()) {
                oss << ",\n";
            } else {
                oss << "\n";
            }
        }
        static std::map<proto::Engine, std::string> engine_map = {
                {proto::ROCKSDB,            "Rocksdb"},
                {proto::REDIS,              "Redis"},
                {proto::ROCKSDB_CSTORE,     "Rocksdb_cstore"},
                {proto::BINLOG,             "Binlog"},
                {proto::INFORMATION_SCHEMA, "MEMORY"}
        };
        oss << ") ENGINE=" << engine_map[info.engine];
        oss << " DEFAULT CHARSET=" << charset_map[info.charset];
        oss << " AVG_ROW_LENGTH=" << info.byte_size_per_record;
        oss << " COMMENT='{\"resource_tag\":\"" << info.resource_tag << "\"";
        if (!info.comment.empty()) {
            oss << ", \"comment\":\"" << info.comment << "\"";
        }
        oss << ", \"replica_num\":" << info.replica_num;
        oss << ", \"region_split_lines\":" << info.region_split_lines;
        if (info.ttl_info.ttl_duration_s > 0) {
            oss << ", \"ttl_duration\":" << info.ttl_info.ttl_duration_s;
        }
        if (info.learner_resource_tags.size() > 0) {
            oss << ", \"learner_resource_tag\": [";
            for (size_t i = 0; i < info.learner_resource_tags.size(); i++) {
                oss << "\"" << info.learner_resource_tags[i] << "\"";
                if (i != info.learner_resource_tags.size() - 1) {
                    oss << ",";
                }
            }
            oss << "]";
        }
        if (info.dists.size() > 0) {
            oss << ", \"dists\": [";
            for (size_t i = 0; i < info.dists.size(); ++i) {
                oss << " { ";
                if (!info.dists[i].resource_tag.empty()) {
                    oss << "\"resource_tag\":\"" << info.dists[i].resource_tag << "\",";
                }
                if (!info.dists[i].logical_room.empty()) {
                    oss << "\"logical_room\":\"" << info.dists[i].logical_room << "\", ";
                }
                if (!info.dists[i].physical_room.empty()) {
                    oss << "\"physical_room\":\"" << info.dists[i].physical_room << "\", ";
                }
                oss << "\"count\":" << info.dists[i].count << "}";
                if (i != info.dists.size() - 1) {
                    oss << ",";
                }
            }
            oss << "]";
        }
        if (!info.main_logical_room.empty()) {
            oss << ", \"main_logical_room\": \"" << info.main_logical_room << "\"";
        }

        if (info.region_num > 0) {
            oss << ", \"region_num\":" << info.region_num;
        }
        oss << ", \"namespace\":\"" << info.namespace_ << "\"}'";
        if (info.partition_num > 1) {
            if (info.partition_ptr != nullptr) {
                oss << info.partition_ptr->to_str();
            } else {
                TLOG_WARN("{}, partition info is null", *client);
            }
        }
        row.emplace_back(oss.str());
        rows.emplace_back(row);
        // Make mysql packet.
        if (_make_common_resultset_packet(client, fields, rows) != 0) {
            TLOG_ERROR("{}, Failed to make result packet." ,*client);
            _wrapper->make_err_packet(client, ER_MAKE_RESULT_PACKET, "{}", client->query_ctx->sql.c_str());
            client->state = STATE_ERROR;
            return false;
        }
        client->state = STATE_READ_QUERY_RESULT;
        return true;
    }

    bool ShowHelper::_show_socket(const SmartSocket &client, const std::vector<std::string> &split_vec) {
        // Make fields.
        std::vector<ResultField> fields;
        fields.reserve(3);
        do {
            ResultField field;
            field.name = "ip";
            field.type = MYSQL_TYPE_VARCHAR;
            field.length = 1024;
            fields.emplace_back(field);
        } while (0);
        do {
            ResultField field;
            field.name = "count";
            field.type = MYSQL_TYPE_VARCHAR;
            field.length = 1024;
            fields.emplace_back(field);
        } while (0);
        do {
            ResultField field;
            field.name = "username";
            field.type = MYSQL_TYPE_VARCHAR;
            field.length = 1024;
            fields.emplace_back(field);
        } while (0);
        std::map<std::string, std::map<std::string, int>> ip_map;
        EpollInfo *epoll_info = NetworkServer::get_instance()->get_epoll_info();
        for (int32_t idx = 0; idx < CONFIG_MPL_EPOLL_MAX_SIZE; ++idx) {
            const SmartSocket &sock = epoll_info->get_fd_mapping(idx);
            if (sock == nullptr || sock->is_free || sock->fd == -1 || sock->ip == "") {
                continue;
            }
            ip_map[sock->ip][sock->username]++;
        }
        // Make rows.
        std::vector<std::vector<std::string> > rows;
        rows.reserve(10);
        for (auto &pair: ip_map) {
            for (auto &pair2: pair.second) {
                std::vector<std::string> row;
                row.emplace_back(pair.first);
                row.emplace_back(std::to_string(pair2.second));
                row.emplace_back(pair2.first);
                rows.emplace_back(row);
            }
        }
        std::sort(rows.begin(), rows.end(),
                  [](const std::vector<std::string> &left, const std::vector<std::string> &right) {
                      int l = atoi(left[1].c_str());
                      int r = atoi(right[1].c_str());
                      return l < r;
                  });
        // Make mysql packet.
        if (_make_common_resultset_packet(client, fields, rows) != 0) {
            TLOG_ERROR("{}, Failed to make result packet.", *client);
            _wrapper->make_err_packet(client, ER_MAKE_RESULT_PACKET, "Failed to make result packet.");
            client->state = STATE_ERROR;
            return false;
        }
        client->state = STATE_READ_QUERY_RESULT;
        return true;
    }

    bool ShowHelper::_show_processlist(const SmartSocket &client, const std::vector<std::string> &split_vec) {
        // Make fields.
        std::vector<ResultField> fields;
        fields.reserve(8);
        bool only_show_doing_sql = false;
        if (!split_vec.empty() && split_vec.back() == "sql") {
            only_show_doing_sql = true;
        }
        do {
            ResultField field;
            field.name = "Id";
            field.type = MYSQL_TYPE_VARCHAR;
            field.length = 1024;
            fields.emplace_back(field);
        } while (0);
        do {
            ResultField field;
            field.name = "User";
            field.type = MYSQL_TYPE_VARCHAR;
            field.length = 1024;
            fields.emplace_back(field);
        } while (0);
        do {
            ResultField field;
            field.name = "Host";
            field.type = MYSQL_TYPE_VARCHAR;
            field.length = 1024;
            fields.emplace_back(field);
        } while (0);
        do {
            ResultField field;
            field.name = "db";
            field.type = MYSQL_TYPE_VARCHAR;
            field.length = 1024;
            fields.emplace_back(field);
        } while (0);
        do {
            ResultField field;
            field.name = "Command";
            field.type = MYSQL_TYPE_VARCHAR;
            field.length = 1024;
            fields.emplace_back(field);
        } while (0);
        do {
            ResultField field;
            field.name = "Time";
            field.type = MYSQL_TYPE_VARCHAR;
            field.length = 1024;
            fields.emplace_back(field);
        } while (0);
        do {
            ResultField field;
            field.name = "State";
            field.type = MYSQL_TYPE_VARCHAR;
            field.length = 1024;
            fields.emplace_back(field);
        } while (0);
        do {
            ResultField field;
            field.name = "Info";
            field.type = MYSQL_TYPE_VARCHAR;
            field.length = 1024;
            fields.emplace_back(field);
        } while (0);
        // Make rows.
        std::vector<std::vector<std::string> > rows;
        rows.reserve(10);
        EpollInfo *epoll_info = NetworkServer::get_instance()->get_epoll_info();
        for (int32_t idx = 0; idx < CONFIG_MPL_EPOLL_MAX_SIZE; ++idx) {
            const SmartSocket &sock = epoll_info->get_fd_mapping(idx);
            if (sock == nullptr || sock->is_free || sock->fd == -1 || sock->ip == "") {
                if (sock != nullptr) {
                    TLOG_WARN("{}, processlist, free:{}", *sock, sock->is_free);
                }
                continue;
            }
            if (!sock->user_info || !sock->query_ctx) {
                TLOG_ERROR("param invalid");
                return false;
            }
            if (only_show_doing_sql && sock->query_ctx->sql.size() == 0) {
                continue;
            }
            TLOG_WARN("{}, processlist, free:{}", *sock, sock->is_free);
            std::vector<std::string> row;
            row.emplace_back(std::to_string(sock->conn_id));
            row.emplace_back(sock->user_info->username);
            row.emplace_back(sock->ip);
            row.emplace_back(sock->current_db);
            auto command = sock->query_ctx->mysql_cmd;
            if (command == COM_SLEEP) {
                row.emplace_back("Sleep");
            } else {
                row.emplace_back("Query");
            }
            row.emplace_back(std::to_string(time(nullptr) - sock->last_active));
            if (command == COM_SLEEP) {
                row.emplace_back(" ");
            } else {
                row.emplace_back("executing");
            }
            if (command == COM_SLEEP) {
                row.emplace_back("");
            } else {
                row.emplace_back(sock->query_ctx->sql);
            }
            rows.emplace_back(row);
        }
        // Make mysql packet.
        if (_make_common_resultset_packet(client, fields, rows) != 0) {
            TLOG_ERROR("{}, Failed to make result packet.", *client);
            _wrapper->make_err_packet(client, ER_MAKE_RESULT_PACKET, "Failed to make result packet.");
            client->state = STATE_ERROR;
            return false;
        }
        client->state = STATE_READ_QUERY_RESULT;
        return true;
    }

    bool ShowHelper::_show_warnings(const SmartSocket &client, const std::vector<std::string> &split_vec) {
        if (client == nullptr) {
            TLOG_ERROR("param invalid");
            //client->state = STATE_ERROR;
            return false;
        }

        // Make result info.
        std::vector<ResultField> fields;
        fields.reserve(3);
        do {
            ResultField field;
            field.name = "Level";
            field.type = MYSQL_TYPE_VARCHAR;
            field.length = 1024;
            fields.emplace_back(field);
        } while (0);
        do {
            ResultField field;
            field.name = "Code";
            field.type = MYSQL_TYPE_LONG;
            fields.emplace_back(field);
        } while (0);
        do {
            ResultField field;
            field.name = "Message";
            field.type = MYSQL_TYPE_VARCHAR;
            field.length = 1024;
            fields.emplace_back(field);
        } while (0);
        // Make rows.
        std::vector<std::vector<std::string> > rows;

        // Make mysql packet.
        if (_make_common_resultset_packet(client, fields, rows) != 0) {
            TLOG_ERROR("{}, Failed to package mysql common result.", *client);
            _wrapper->make_err_packet(client, ER_MAKE_RESULT_PACKET, "Failed to make result packet.");
            client->state = STATE_ERROR;
            return false;
        }
        client->state = STATE_READ_QUERY_RESULT;
        return true;
    }

    bool ShowHelper::_show_cost(const SmartSocket &client, const std::vector<std::string> &split_vec) {
        SchemaFactory *factory = SchemaFactory::get_instance();
        if (client == nullptr || client->query_ctx == nullptr || factory == nullptr) {
            TLOG_ERROR("param invalid");
            //client->state = STATE_ERROR;
            return false;
        }

        if (split_vec.size() != 3) {
            client->state = STATE_ERROR;
            return false;
        }

        std::vector<std::string> database_table;
        if (split_vec[2] == "switch") {
            factory->get_cost_switch_open(database_table);
        } else {
            factory->table_with_statistics_info(database_table);
        }
        TLOG_WARN("show cost");
        std::vector<std::vector<std::string> > rows;
        rows.reserve(10);
        for (auto &d_t_name: database_table) {
            TLOG_WARN("{}", d_t_name.c_str());
            std::vector<std::string> split_vec = turbo::StrSplit(d_t_name, turbo::ByChar('.'));
            if (split_vec.size() != 3) {
                TLOG_ERROR("databae table name:{}", d_t_name.c_str());
                continue;
            }
            rows.emplace_back(split_vec);
        }

        std::vector<std::string> names = {"name_space", "database_name", "table_name"};

        std::vector<ResultField> fields;
        fields.reserve(3);
        for (auto &name: names) {
            ResultField field;
            field.name = name;
            field.type = MYSQL_TYPE_STRING;
            fields.emplace_back(field);
        }
        // Make mysql packet.
        if (_make_common_resultset_packet(client, fields, rows) != 0) {
            TLOG_ERROR("{}, Failed to make result packet.", *client);
            _wrapper->make_err_packet(client, ER_MAKE_RESULT_PACKET, "Failed to make result packet.");
            client->state = STATE_ERROR;
            return false;
        }
        client->state = STATE_READ_QUERY_RESULT;
        return true;
    }

    bool ShowHelper::_show_full_tables(const SmartSocket &client, const std::vector<std::string> &split_vec) {
        bool is_like_pattern = false;
        std::string like_pattern;
        re2::RE2::Options option;
        std::unique_ptr<re2::RE2> regex_ptr;
        if (client == nullptr) {
            TLOG_ERROR("param invalid");
            //client->state = STATE_ERROR;
            return false;
        }
        SchemaFactory *factory = SchemaFactory::get_instance();
        if (factory == nullptr || client == nullptr || client->user_info == nullptr || client->query_ctx == nullptr) {
            TLOG_ERROR("param invalid");
            return false;
        }

        std::string namespace_ = client->user_info->namespace_;
        std::string current_db = client->current_db;


        if (split_vec.size() == 5) {
            current_db = remove_quote(split_vec[4].c_str(), '`');
        } else if (split_vec.size() == 3) {
        } else if (split_vec.size() == 7) {
            // TODO: where [LIKE 'pattern' | WHERE expr]
            is_like_pattern = true;
            std::string like_str;
            current_db = remove_quote(split_vec[4].c_str(), '`');
            like_str = remove_quote(split_vec[6].c_str(), '"');
            like_str = remove_quote(like_str.c_str(), '\'');
            for (auto ch: like_str) {
                if (ch == '%') {
                    like_pattern.append(".*");
                } else {
                    like_pattern.append(1, ch);
                }
            }
            option.set_encoding(RE2::Options::EncodingLatin1);
            option.set_case_sensitive(false);
            regex_ptr.reset(new re2::RE2(like_pattern, option));

        } else {
            client->state = STATE_ERROR;
            return false;
        }

        if (current_db == "") {
            TLOG_WARN("no database selected");
            _wrapper->make_err_packet(client, ER_NO_DB_ERROR, "No database selected");
            client->state = STATE_READ_QUERY_RESULT;
            return false;
        }
        if (current_db == "information_schema") {
            namespace_ = "INTERNAL";
        }

        // Make fields.
        std::vector<ResultField> fields;
        fields.reserve(2);
        do {
            ResultField field;
            field.name = "Tables_in_" + current_db;
            field.type = MYSQL_TYPE_VARCHAR;
            field.length = 1024;
            fields.emplace_back(field);
        } while (0);
        do {
            ResultField field;
            field.name = "Table_type";
            field.type = MYSQL_TYPE_VARCHAR;
            field.length = 1024;
            fields.emplace_back(field);
        } while (0);

        // Make rows.
        std::vector<std::vector<std::string> > rows;
        rows.reserve(10);
        std::vector<std::string> tables = factory->get_table_list(
                namespace_, current_db, client->user_info.get());
        //TLOG_INFO("db:{} table.size:{}", current_db.c_str(), tables.size());
        for (uint32_t cnt = 0; cnt < tables.size(); ++cnt) {
            //TLOG_INFO("table:{}", tables[cnt].c_str());
            if (is_like_pattern) {
                if (!RE2::FullMatch(tables[cnt], *regex_ptr)) {
                    TLOG_INFO("not match");
                    continue;
                }
            }
            std::vector<std::string> row;
            row.emplace_back(tables[cnt]);
            row.emplace_back("BASE TABLE");
            rows.emplace_back(row);
        }

        // Make mysql packet.
        if (_make_common_resultset_packet(client, fields, rows) != 0) {
            TLOG_ERROR("{}, Failed to make result packet.", *client);
            _wrapper->make_err_packet(client, ER_MAKE_RESULT_PACKET, "{}", client->query_ctx->sql.c_str());
            client->state = STATE_ERROR;
            return false;
        }
        client->state = STATE_READ_QUERY_RESULT;
        return true;
    }

    bool ShowHelper::_show_full_columns(const SmartSocket &client, const std::vector<std::string> &split_vec) {
        SchemaFactory *factory = SchemaFactory::get_instance();
        if (client == nullptr || client->user_info == nullptr || factory == nullptr) {
            TLOG_ERROR("param invalid");
            //client->state = STATE_ERROR;
            return false;
        }
        bool is_like_pattern = false;
        std::string like_pattern;
        re2::RE2::Options option;
        std::unique_ptr<re2::RE2> regex_ptr;

        // Make fields.
        std::vector<ResultField> fields;
        fields.reserve(9);
        do {
            ResultField field;
            field.name = "Field";
            field.type = MYSQL_TYPE_VARCHAR;
            field.length = 1024;
            fields.emplace_back(field);
        } while (0);
        do {
            ResultField field;
            field.name = "Type";
            field.type = MYSQL_TYPE_VARCHAR;
            field.length = 1024;
            fields.emplace_back(field);
        } while (0);
        do {
            ResultField field;
            field.name = "Collation";
            field.type = MYSQL_TYPE_VARCHAR;
            field.length = 1024;
            fields.emplace_back(field);
        } while (0);
        do {
            ResultField field;
            field.name = "Null";
            field.type = MYSQL_TYPE_VARCHAR;
            field.length = 1024;
            fields.emplace_back(field);
        } while (0);
        do {
            ResultField field;
            field.name = "Key";
            field.type = MYSQL_TYPE_VARCHAR;
            field.length = 1024;
            fields.emplace_back(field);
        } while (0);
        do {
            ResultField field;
            field.name = "default";
            field.type = MYSQL_TYPE_VARCHAR;
            field.length = 1024;
            fields.emplace_back(field);
        } while (0);
        do {
            ResultField field;
            field.name = "Extra";
            field.type = MYSQL_TYPE_VARCHAR;
            field.length = 1024;
            fields.emplace_back(field);
        } while (0);
        do {
            ResultField field;
            field.name = "Privileges";
            field.type = MYSQL_TYPE_VARCHAR;
            field.length = 1024;
            fields.emplace_back(field);
        } while (0);
        do {
            ResultField field;
            field.name = "Comment";
            field.type = MYSQL_TYPE_VARCHAR;
            field.length = 1024;
            fields.emplace_back(field);
        } while (0);

        std::string db = client->current_db;
        std::string table;
        if (split_vec.size() == 5) {
            std::string db_table = split_vec[4];
            std::string::size_type position = db_table.find_first_of('.');
            if (position == std::string::npos) {
                // `table_name`
                table = remove_quote(db_table.c_str(), '`');
            } else {
                // `db_name`.`table_name`
                db = remove_quote(db_table.substr(0, position).c_str(), '`');
                table = remove_quote(db_table.substr(position + 1,
                                                     db_table.length() - position - 1).c_str(), '`');
            }
        } else if (split_vec.size() == 7) {
            db = remove_quote(split_vec[6].c_str(), '`');
            table = remove_quote(split_vec[4].c_str(), '`');
        } else if (split_vec.size() == 9) {
            is_like_pattern = true;
            std::string like_str;
            db = remove_quote(split_vec[6].c_str(), '`');
            table = remove_quote(split_vec[4].c_str(), '`');
            like_str = remove_quote(split_vec[8].c_str(), '"');
            like_str = remove_quote(like_str.c_str(), '\'');
            for (auto ch: like_str) {
                if (ch == '%') {
                    like_pattern.append(".*");
                } else {
                    like_pattern.append(1, ch);
                }
            }
            option.set_encoding(RE2::Options::EncodingLatin1);
            option.set_case_sensitive(false);
            regex_ptr.reset(new re2::RE2(like_pattern, option));
        } else {
            client->state = STATE_ERROR;
            return false;
        }
        std::string namespace_ = client->user_info->namespace_;
        if (db == "information_schema") {
            namespace_ = "INTERNAL";
        }
        std::string full_name = namespace_ + "." + db + "." + table;
        int64_t table_id = -1;
        if (factory->get_table_id(full_name, table_id) != 0) {
            client->state = STATE_ERROR;
            return false;
        }
        TableInfo info = factory->get_table_info(table_id);
        std::map<int32_t, IndexInfo> field_index;
        for (auto &index_id: info.indices) {
            IndexInfo index_info = factory->get_index_info(index_id);
            for (auto &field: index_info.fields) {
                if (field_index.count(field.id) == 0) {
                    field_index[field.id] = index_info;
                }
            }
        }
        // Make rows.
        std::vector<std::vector<std::string> > rows;
        rows.reserve(10);
        for (auto &field: info.fields) {
            if (field.deleted) {
                continue;
            }
            std::vector<std::string> row;
            std::vector<std::string> split_vec = turbo::StrSplit(field.name, turbo::ByAnyChar(" \t\n\r."),
                                                                 turbo::SkipEmpty());
            if (is_like_pattern) {
                if (!RE2::FullMatch(split_vec[split_vec.size() - 1], *regex_ptr)) {
                    TLOG_INFO("not match");
                    continue;
                }
            }
            row.emplace_back(split_vec[split_vec.size() - 1]);
            row.emplace_back(PrimitiveType_Name(field.type));
            row.emplace_back("nullptr");
            row.emplace_back(field.can_null ? "YES" : "NO");
            if (field_index.count(field.id) == 0) {
                row.emplace_back(" ");
            } else {
                std::string index = IndexType_Name(field_index[field.id].type);
                if (field_index[field.id].type == proto::I_FULLTEXT) {
                    index += "(" + proto::SegmentType_Name(field_index[field.id].segment_type) + ")";
                }
                row.emplace_back(index);
            }
            row.emplace_back(field.default_value);
            if (info.auto_inc_field_id == field.id) {
                row.emplace_back("auto_increment");
            } else {
                row.emplace_back(" ");
            }
            row.emplace_back("select,insert,update,references");
            row.emplace_back(" ");
            rows.emplace_back(row);
        }

        // Make mysql packet.
        if (_make_common_resultset_packet(client, fields, rows) != 0) {
            TLOG_ERROR("{}, Failed to make result packet.", *client);
            _wrapper->make_err_packet(client, ER_MAKE_RESULT_PACKET, "Failed to make result packet.");
            client->state = STATE_ERROR;
            return false;
        }
        client->state = STATE_READ_QUERY_RESULT;
        return true;
    }

    bool ShowHelper::_show_table_status(const SmartSocket &client, const std::vector<std::string> &split_vec) {
        SchemaFactory *factory = SchemaFactory::get_instance();
        if (client == nullptr || client->user_info == nullptr || factory == nullptr) {
            TLOG_ERROR("param invalid");
            //client->state = STATE_ERROR;
            return false;
        }

        // Make fields.
        std::vector<ResultField> fields;
        fields.reserve(18);
        do {
            ResultField field;
            field.name = "Name";
            field.type = MYSQL_TYPE_VARCHAR;
            field.length = 1024;
            fields.emplace_back(field);
        } while (0);
        do {
            ResultField field;
            field.name = "Engine";
            field.type = MYSQL_TYPE_VARCHAR;
            field.length = 1024;
            fields.emplace_back(field);
        } while (0);
        do {
            ResultField field;
            field.name = "Version";
            field.type = MYSQL_TYPE_LONG;
            fields.emplace_back(field);
        } while (0);
        do {
            ResultField field;
            field.name = "Row_format";
            field.type = MYSQL_TYPE_VARCHAR;
            field.length = 1024;
            fields.emplace_back(field);
        } while (0);
        do {
            ResultField field;
            field.name = "Rows";
            field.type = MYSQL_TYPE_LONG;
            fields.emplace_back(field);
        } while (0);
        do {
            ResultField field;
            field.name = "Avg_row_length";
            field.type = MYSQL_TYPE_LONG;
            fields.emplace_back(field);
        } while (0);
        do {
            ResultField field;
            field.name = "Data_length";
            field.type = MYSQL_TYPE_LONG;
            fields.emplace_back(field);
        } while (0);
        do {
            ResultField field;
            field.name = "Max_data_length";
            field.type = MYSQL_TYPE_LONG;
            fields.emplace_back(field);
        } while (0);
        do {
            ResultField field;
            field.name = "Index_length";
            field.type = MYSQL_TYPE_LONG;
            fields.emplace_back(field);
        } while (0);
        do {
            ResultField field;
            field.name = "Data_free";
            field.type = MYSQL_TYPE_LONG;
            fields.emplace_back(field);
        } while (0);
        do {
            ResultField field;
            field.name = "Auto_increment";
            field.type = MYSQL_TYPE_LONG;
            fields.emplace_back(field);
        } while (0);
        do {
            ResultField field;
            field.name = "Create_time";
            field.type = MYSQL_TYPE_VARCHAR;
            field.length = 1024;
            fields.emplace_back(field);
        } while (0);
        do {
            ResultField field;
            field.name = "Update_time";
            field.type = MYSQL_TYPE_VARCHAR;
            field.length = 1024;
            fields.emplace_back(field);
        } while (0);
        do {
            ResultField field;
            field.name = "Check_time";
            field.type = MYSQL_TYPE_VARCHAR;
            field.length = 1024;
            fields.emplace_back(field);
        } while (0);
        do {
            ResultField field;
            field.name = "Collation";
            field.type = MYSQL_TYPE_VARCHAR;
            field.length = 1024;
            fields.emplace_back(field);
        } while (0);
        do {
            ResultField field;
            field.name = "Checksum";
            field.type = MYSQL_TYPE_VARCHAR;
            field.length = 1024;
            fields.emplace_back(field);
        } while (0);
        do {
            ResultField field;
            field.name = "Create_options";
            field.type = MYSQL_TYPE_VARCHAR;
            field.length = 1024;
            fields.emplace_back(field);
        } while (0);
        do {
            ResultField field;
            field.name = "Comment";
            field.type = MYSQL_TYPE_VARCHAR;
            field.length = 1024;
            fields.emplace_back(field);
        } while (0);

        std::string namespace_ = client->user_info->namespace_;
        std::string db = client->current_db;

        std::vector<std::vector<std::string> > rows;
        std::vector<std::vector<std::string> > assign_rows;
        rows.reserve(10);
        assign_rows.reserve(1);
        std::string assign_table;
        std::string table;
        /* not support yet: 'WHERE expr'
    SHOW TABLE STATUS
    [{FROM | IN} db_name]
    [LIKE 'pattern' | WHERE expr]
    */
        if (split_vec.size() == 3) {
        } else if (split_vec.size() == 5) {
            if (turbo::EqualsIgnoreCase(split_vec[3], "from") || turbo::EqualsIgnoreCase(split_vec[3], "in")) {
                db = remove_quote(split_vec[4].c_str(), '`');
            } else if (turbo::EqualsIgnoreCase(split_vec[3], "like")) {
                table = remove_quote(split_vec[4].c_str(), '\''); // handle like `pattern` as table_name
            }
        } else if (split_vec.size() == 7) {
            db = remove_quote(split_vec[4].c_str(), '`');
            table = remove_quote(split_vec[6].c_str(), '\'');
        } else {
            client->state = STATE_ERROR;
            return false;
        }
        if (db == "") {
            TLOG_WARN("no database selected");
            _wrapper->make_err_packet(client, ER_NO_DB_ERROR, "No database selected");
            client->state = STATE_READ_QUERY_RESULT;
            return false;
        }
        if (db == "information_schema") {
            namespace_ = "INTERNAL";
        }
        std::string key = namespace_ + "." + db;
        if (!table.empty()) {
            std::string full_name = namespace_ + "." + db + "." + table;
            int64_t table_id = -1;
            if (factory->get_table_id(full_name, table_id) != 0) {
                client->state = STATE_ERROR;
                return false;
            }
            assign_table = table;
        }

        // 查看cache是否有效
        {
            BAIDU_SCOPED_LOCK(_mutex);
            if (_table_info_cache_time.find(key) != _table_info_cache_time.end()
                && butil::gettimeofday_us() - _table_info_cache_time[key] < FLAGS_show_table_status_cache_time) {
                if (assign_table != "") {
                    // 指定了table，遍历找到table信息
                    for (auto &row: _table_info_cache[key]) {
                        if (row.size() > 0 && row[0] == assign_table) {
                            assign_rows.emplace_back(row);
                            break;
                        }
                    }
                }
                if (_make_common_resultset_packet(client,
                                                  fields,
                                                  assign_table == "" ? _table_info_cache[key] : assign_rows) != 0) {
                    TLOG_ERROR("{}, Failed to make result packet.", *client);
                    _wrapper->make_err_packet(client, ER_MAKE_RESULT_PACKET, "Failed to make result packet.");
                    client->state = STATE_ERROR;
                    return false;
                }
                client->state = STATE_READ_QUERY_RESULT;
                return true;
            }
        }

        proto::QueryRequest req;
        req.set_op_type(proto::QUERY_TABLE_FLATTEN);
        req.set_namespace_name(client->user_info->namespace_);
        req.set_database(db);
        proto::QueryResponse res;
        // 这个请求meta会对table_mutex加锁并copy所有table元数据，比较重
        MetaServerInteract::get_instance()->send_request("query", req, res);
        for (auto &table_info: res.flatten_tables()) {
            std::string create_time = "2018-08-09 15:01:40";
            if (!table_info.create_time().empty()) {
                create_time = table_info.create_time();
            }
            // Make rows.
            std::vector<std::string> row;
            row.emplace_back(table_info.table_name());
            row.emplace_back("Innodb");
            row.emplace_back(std::to_string(table_info.version()));
            row.emplace_back("Compact");
            row.emplace_back(std::to_string(table_info.row_count()));
            row.emplace_back(std::to_string(table_info.byte_size_per_record()));
            row.emplace_back("0");
            row.emplace_back("0");
            row.emplace_back("0");
            row.emplace_back("0");
            row.emplace_back("0");
            row.emplace_back(create_time);
            row.emplace_back("");
            row.emplace_back("");
            row.emplace_back("utf8_general_ci");
            row.emplace_back("");
            row.emplace_back("");
            row.emplace_back("");
            rows.emplace_back(row);
            if (assign_table != "" && table_info.table_name() == assign_table) {
                assign_rows.emplace_back(row);
            }
        }
        {
            // 更新缓存
            BAIDU_SCOPED_LOCK(_mutex);
            _table_info_cache_time[key] = butil::gettimeofday_us();
            _table_info_cache[key] = rows;
        }
        // Make mysql packet.
        if (_make_common_resultset_packet(client,
                                          fields,
                                          assign_table == "" ? rows : assign_rows) != 0) {
            TLOG_ERROR("{}, Failed to make result packet.", *client);
            _wrapper->make_err_packet(client, ER_MAKE_RESULT_PACKET, "Failed to make result packet.");
            client->state = STATE_ERROR;
            return false;
        }
        client->state = STATE_READ_QUERY_RESULT;
        return true;
    }

    bool ShowHelper::_show_schema_conf(const SmartSocket &client, const std::vector<std::string> &split_vec) {
        SchemaFactory *factory = SchemaFactory::get_instance();
        if (client == nullptr || client->query_ctx == nullptr || factory == nullptr) {
            TLOG_ERROR("param invalid");
            //client->state = STATE_ERROR;
            return false;
        }

        std::unordered_set<std::string> allowed_conf = {"need_merge",
                                                        "storage_compute_separate",
                                                        "select_index_by_cost",
                                                        "pk_prefix_balance",
                                                        "backup_table",
                                                        "in_fast_import",
                                                        "tail_split_num",
                                                        "tail_split_step"};
        // 前三个conf按照bool解析, pk_prefix_balance按照int32来解析
        if (split_vec.size() != 3 || allowed_conf.find(split_vec[2]) == allowed_conf.end()) {
            client->state = STATE_ERROR;
            return false;
        }

        std::vector<std::string> database_table;
        factory->get_schema_conf_open(split_vec[2], database_table);
        TLOG_WARN("show schema_conf: {}", split_vec[2].c_str());
        std::vector<std::vector<std::string> > rows;
        rows.reserve(10);
        for (auto &d_t_name: database_table) {
            TLOG_WARN("{}", d_t_name.c_str());
            std::vector<std::string> split_vec = turbo::StrSplit(d_t_name, turbo::ByChar('.'));
            if (split_vec.size() != 3 && split_vec.size() != 4) {
                TLOG_ERROR("database table name:{}", d_t_name.c_str());
                continue;
            }
            rows.emplace_back(split_vec);
        }

        std::vector<std::string> names = {"namespace", "database_name", "table_name"};
        if (split_vec[2] == "pk_prefix_balance"
            || split_vec[2] == "backup_table"
            || split_vec[2] == "tail_split_num"
            || split_vec[2] == "tail_split_step") {
            names.emplace_back("value");
        }

        std::vector<ResultField> fields;
        fields.reserve(3);
        for (auto &name: names) {
            ResultField field;
            field.name = name;
            field.type = MYSQL_TYPE_STRING;
            fields.emplace_back(field);
        }
        // Make mysql packet.
        if (_make_common_resultset_packet(client, fields, rows) != 0) {
            TLOG_ERROR("{}, Failed to make result packet.", *client);
            _wrapper->make_err_packet(client, ER_MAKE_RESULT_PACKET, "Failed to make result packet.");
            client->state = STATE_ERROR;
            return false;
        }
        client->state = STATE_READ_QUERY_RESULT;
        return true;
    }

    bool ShowHelper::_process_binlogs_info(const SmartSocket &client, std::unordered_map<int64_t,
            std::unordered_map<int64_t, std::vector<proto::StoreRes>>> &table_id_to_query_info) {
        std::vector<std::string> field_names = {"table_id", "current_partition_id", "region_id", "instance_ip",
                                                "table_name", "check_point_datetime", "max_oldest_datetime",
                                                "region_oldest_datetime", "binlog_cf_oldest_datetime",
                                                "data_cf_oldest_datetime"};
        std::vector<ResultField> result_fields;
        result_fields.reserve(3);
        for (auto &field_name: field_names) {
            ResultField field;
            field.name = field_name;
            field.type = MYSQL_TYPE_STRING;
            result_fields.emplace_back(field);
        }

        std::vector<std::vector<std::string>> result_rows;
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

        if (_make_common_resultset_packet(client, result_fields, result_rows) != 0) {
            TLOG_ERROR("{}, Failed to make result packet.", *client);
            _wrapper->make_err_packet(client, ER_MAKE_RESULT_PACKET, "Failed to make result packet.");
            client->state = STATE_ERROR;
            return false;
        }
        client->state = STATE_READ_QUERY_RESULT;
        return true;
    }

    bool ShowHelper::_process_partition_binlogs_info(const SmartSocket &client, std::unordered_map<int64_t,
            std::unordered_map<int64_t, std::vector<proto::StoreRes>>> &table_id_to_query_info) {
        std::vector<std::string> field_names = {"table_id", "partition_index", "check_point_datetime",
                                                "oldest_datetime", "table_name"};
        std::vector<ResultField> result_fields;
        result_fields.reserve(3);
        for (auto &field_name: field_names) {
            ResultField field;
            field.name = field_name;
            field.type = MYSQL_TYPE_STRING;
            result_fields.emplace_back(field);
        }

        std::vector<std::vector<std::string>> result_rows;
        for (const auto &region_id_peers_info: table_id_to_query_info) {
            int64_t table_id = region_id_peers_info.first;
            const std::string table_name = SchemaFactory::get_instance()->get_table_info(table_id).name;
            for (const auto &region_id_peer_info: region_id_peers_info.second) {
                int64_t region_id = region_id_peer_info.first;
                proto::RegionInfo region_info_tmp;
                SchemaFactory::get_instance()->get_region_info(table_id, region_id, region_info_tmp);
                int64_t current_partition_id = region_info_tmp.partition_id();
                const std::vector<proto::StoreRes> &pb_peer_info_vec = region_id_peer_info.second;
                int64_t max_check_point_ts = INT64_MIN;
                int64_t min_oldest_ts = INT64_MAX;
                for (const auto &binlog_peer_info: pb_peer_info_vec) {
                    int64_t check_point_ts = binlog_peer_info.binlog_info().check_point_ts();
                    int64_t oldest_ts = binlog_peer_info.binlog_info().oldest_ts();
                    max_check_point_ts = std::max(max_check_point_ts, check_point_ts);
                    min_oldest_ts = std::min(min_oldest_ts, oldest_ts);
                }
                std::vector<std::string> row;
                row.reserve(3);
                row.emplace_back(std::to_string(table_id));
                row.emplace_back(std::to_string(current_partition_id));
                row.emplace_back(ts_to_datetime_str(max_check_point_ts));
                row.emplace_back(ts_to_datetime_str(min_oldest_ts));
                row.emplace_back(table_name);
                result_rows.emplace_back(row);
            }
        }

        std::map<std::string, std::vector<std::string>> map_final_result;
        for (const auto &vec_result_row: result_rows) {
            const std::string &table_id_str = vec_result_row[0];
            const std::string &partition_index_str = vec_result_row[1];
            const std::string prefix_str = table_id_str + partition_index_str;
            if (map_final_result.count(prefix_str) > 0) {
                map_final_result[prefix_str][2] = std::max(vec_result_row[2], map_final_result[prefix_str][2]);
                map_final_result[prefix_str][3] = std::max(vec_result_row[3], map_final_result[prefix_str][3]);
            } else {
                map_final_result[prefix_str] = vec_result_row;
            }
        }

        std::vector<std::vector<std::string>> resutl_res_final;
        resutl_res_final.reserve(10);
        for (const auto &final_result: map_final_result) {
            resutl_res_final.emplace_back(final_result.second);
        }

        //泛型排序，让展示结果有序
        std::sort(resutl_res_final.begin(), resutl_res_final.end(),
                  [](const std::vector<std::string> &a, const std::vector<std::string> &b) {
                      if (a.size() < 3 || b.size() < 3) {
                          return false;
                      }
                      const std::string str_prefix_a = a[0] + a[1];
                      const std::string str_prefix_b = b[0] + b[1];
                      errno = 0;
                      int64_t value_prefix_a = strtoll(str_prefix_a.c_str(), nullptr, 10);
                      int64_t value_prefix_b = strtoll(str_prefix_b.c_str(), nullptr, 10);
                      return value_prefix_a <= value_prefix_b;
                  });

        if (_make_common_resultset_packet(client, result_fields, resutl_res_final) != 0) {
            TLOG_ERROR("{}, Failed to make result packet.", *client);
            _wrapper->make_err_packet(client, ER_MAKE_RESULT_PACKET, "Failed to make result packet.");
            client->state = STATE_ERROR;
            return false;
        }
        client->state = STATE_READ_QUERY_RESULT;
        return true;
    }

    bool ShowHelper::_show_binlogs_info(const SmartSocket &client, const std::vector<std::string> &split_params) {
        SchemaFactory *factory = SchemaFactory::get_instance();
        if (factory == nullptr) {
            TLOG_WARN("factory is null, need check");
            return false;
        }

        std::string input_table_name = "";
        int64_t input_partition_id = -1;
        bool is_detailed = false;
        if (split_params.size() < 3) {
            return false;
        } else if (split_params.size() == 3) {
            if (split_params[2] == "detailed") {
                TLOG_WARN("input parameter does not contain table_name");
                return false;
            }
            input_table_name = split_params[2];
        } else if (split_params.size() == 4) {
            if (split_params[2] == "detail") {
                input_table_name = split_params[3];
                is_detailed = true;
            } else {
                input_table_name = split_params[2];
                input_partition_id = strtoll(split_params[3].c_str(), nullptr, 10);
            }
        } else if (split_params.size() > 4) {
            TLOG_WARN("input parameters counts is above four");
            return false;
        }

        std::unordered_map<int64_t, std::unordered_map<int64_t, std::vector<proto::RegionInfo>>> table_id_partition_binlogs;
        std::unordered_map<int64_t, std::unordered_map<int64_t, std::vector<proto::StoreRes>>> table_id_to_query_info;
        factory->get_partition_binlog_regions(input_table_name, input_partition_id, table_id_partition_binlogs);
        _query_regions_concurrent(table_id_to_query_info, table_id_partition_binlogs);
        if (is_detailed) {
            return _process_binlogs_info(client, table_id_to_query_info);
        } else {
            return _process_partition_binlogs_info(client, table_id_to_query_info);
        }
    }

    void ShowHelper::_query_regions_concurrent(
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

    bool ShowHelper::_show_all_tables(const SmartSocket &client, const std::vector<std::string> &split_vec) {
        SchemaFactory *factory = SchemaFactory::get_instance();
        if (client == nullptr || client->query_ctx == nullptr || factory == nullptr) {
            TLOG_ERROR("param invalid");
            //client->state = STATE_ERROR;
            return false;
        }
        if (split_vec.size() != 3) {
            client->state = STATE_ERROR;
            return false;
        }

        std::vector<std::string> database_table;
        std::map<std::string, std::function<bool(const SmartTable &)>> type_func_map;
        type_func_map["binlog"] = [](const SmartTable &table) {
            return table != nullptr && table->binlog_id > 0;
        };
        type_func_map["ttl"] = [](const SmartTable &table) {
            return table != nullptr && table->ttl_info.ttl_duration_s > 0;
        };
        type_func_map["fulltext"] = [](const SmartTable &table) {
            return table != nullptr && table->has_fulltext;
        };
        type_func_map["cstore"] = [](const SmartTable &table) {
            return table != nullptr && table->engine == proto::ROCKSDB_CSTORE;
        };
        type_func_map["learner"] = [](const SmartTable &table) {
            return table != nullptr && !table->learner_resource_tags.empty();
        };
        type_func_map["type_timestamp"] = [](const SmartTable &table) {
            if (table != nullptr) {
                for (auto &f: table->fields) {
                    if (f.type == proto::TIMESTAMP) {
                        return true;
                    }
                }
            }
            return false;
        };

        if (type_func_map[split_vec[2]] != nullptr) {
            factory->get_table_by_filter(database_table, type_func_map[split_vec[2]]);
        } else {
            TLOG_WARN("not support type:{}", split_vec[2].c_str());
            return false;
        }
        std::vector<std::vector<std::string> > rows;
        rows.reserve(10);
        for (auto &d_t_name: database_table) {
            TLOG_WARN("{}", d_t_name.c_str());
            std::vector<std::string> split_vec = turbo::StrSplit(d_t_name, turbo::ByChar('.'));
            if (split_vec.size() != 6) {
                TLOG_ERROR("database table name:{}", d_t_name.c_str());
                continue;
            }
            rows.emplace_back(split_vec);
        }

        std::vector<std::string> names = {"namespace", "database_name", "table_name", "binlog_db", "binlog_table",
                                          "learner_tags"};

        std::vector<ResultField> fields;
        fields.reserve(4);
        for (auto &name: names) {
            ResultField field;
            field.name = name;
            field.type = MYSQL_TYPE_STRING;
            fields.emplace_back(field);
        }
        // Make mysql packet.
        if (_make_common_resultset_packet(client, fields, rows) != 0) {
            TLOG_ERROR("{}, Failed to make result packet.", *client);
            _wrapper->make_err_packet(client, ER_MAKE_RESULT_PACKET, "Failed to make result packet.");
            client->state = STATE_ERROR;
            return false;
        }
        client->state = STATE_READ_QUERY_RESULT;
        return true;
    }

    bool ShowHelper::_show_region(const SmartSocket &client, const std::vector<std::string> &split_vec) {
        SchemaFactory *factory = SchemaFactory::get_instance();
        if (factory == nullptr || client == nullptr) {
            TLOG_ERROR("param invalid");
            return false;
        }

        if (split_vec.size() < 4 || split_vec[2].size() == 0) {
            client->state = STATE_ERROR;
            return false;
        }
        int64_t table_id = -1;
        if (std::isdigit(split_vec[2][0])) {
            table_id = strtoll(split_vec[2].c_str(), nullptr, 10);
        } else {
            size_t found = split_vec[2].find(".");
            std::string full_name;
            if (found != std::string::npos) {
                full_name = client->user_info->namespace_ + "." + split_vec[2];
            } else {
                full_name = client->user_info->namespace_ + "." + client->current_db + "." + split_vec[2];
            }
            if (factory->get_table_id(full_name, table_id) != 0) {
                client->state = STATE_ERROR;
                return false;
            }
        }
        int64_t region_id = strtoll(split_vec[3].c_str(), nullptr, 10);
        TLOG_WARN("table_id:{}, region_id: {}", table_id, region_id);

        // Make fields.
        std::vector<ResultField> fields;
        fields.reserve(2);
        do {
            ResultField field;
            field.name = "region_id";
            field.type = MYSQL_TYPE_VARCHAR;
            field.length = 1024;
            fields.emplace_back(field);
        } while (0);
        do {
            ResultField field;
            field.name = "region_info";
            field.type = MYSQL_TYPE_VARCHAR;
            field.length = 1024;
            fields.emplace_back(field);
        } while (0);

        // Make rows.
        std::vector<std::vector<std::string> > rows;
        rows.reserve(10);

        proto::RegionInfo region_info;
        if (factory->get_region_info(table_id, region_id, region_info) == 0) {
            int64_t table_id = region_info.table_id();
            auto index_info = factory->get_index_info(table_id);
            if (region_info.start_key().size() > 0) {
                TableKey start_key(region_info.start_key());
                region_info.set_start_key(start_key.decode_start_key_string(index_info));
            } else {
                region_info.set_start_key("-∞");
            }
            if (region_info.end_key().size() > 0) {
                TableKey end_key(region_info.end_key());
                region_info.set_end_key(end_key.decode_start_key_string(index_info));
            } else {
                region_info.set_end_key("+∞");
            }
            std::vector<std::string> row;
            row.emplace_back(std::to_string(region_id));
            row.emplace_back(region_info.ShortDebugString().c_str());
            rows.emplace_back(row);
        } else {
            TLOG_WARN("region: {} does not exist", region_id);
        }

        // Make mysql packet.
        if (_make_common_resultset_packet(client, fields, rows) != 0) {
            TLOG_ERROR("{}, Failed to make result packet.", *client);
            _wrapper->make_err_packet(client, ER_MAKE_RESULT_PACKET, "Failed to make result packet.");
            client->state = STATE_ERROR;
            return false;
        }
        client->state = STATE_READ_QUERY_RESULT;
        return true;
    }

    bool ShowHelper::_show_store_region(const SmartSocket &client, const std::vector<std::string> &split_vec) {
        SchemaFactory *factory = SchemaFactory::get_instance();
        if (client == nullptr || client->query_ctx == nullptr || factory == nullptr) {
            TLOG_ERROR("param invalid");
            //client->state = STATE_ERROR;
            return false;
        }

        if (split_vec.size() != 5) {
            client->state = STATE_ERROR;
            return false;
        }
        std::string store_addr = split_vec[3];
        int64_t region_id = strtoll(split_vec[4].c_str(), nullptr, 10);
        TLOG_WARN("region_id: {}", region_id);

        // Make fields.
        std::vector<ResultField> fields;
        fields.reserve(2);
        do {
            ResultField field;
            field.name = "region_id";
            field.type = MYSQL_TYPE_VARCHAR;
            field.length = 1024;
            fields.emplace_back(field);
        } while (0);

        do {
            ResultField field;
            field.name = "region_info";
            field.type = MYSQL_TYPE_VARCHAR;
            field.length = 1024;
            fields.emplace_back(field);
        } while (0);

        // Make rows.
        std::vector<std::vector<std::string> > rows;
        rows.reserve(10);

        proto::RegionIds req;
        req.add_region_ids(region_id);
        proto::StoreRes res;
        StoreInteract interact(store_addr);
        interact.send_request("query_region", req, res);
        proto::RegionInfo region_info;
        if (res.regions_size() == 1) {
            region_info = res.regions(0);
            int64_t table_id = region_info.table_id();
            auto index_info = factory->get_index_info(table_id);
            if (region_info.start_key().size() > 0) {
                TableKey start_key(region_info.start_key());
                region_info.set_start_key(start_key.decode_start_key_string(index_info));
            } else {
                region_info.set_start_key("-∞");
            }
            if (region_info.end_key().size() > 0) {
                TableKey end_key(region_info.end_key());
                region_info.set_end_key(end_key.decode_start_key_string(index_info));
            } else {
                region_info.set_end_key("+∞");
            }
            std::vector<std::string> row;
            row.emplace_back(std::to_string(region_id));
            row.emplace_back(region_info.ShortDebugString().c_str());
            rows.emplace_back(row);
        } else {
            TLOG_WARN("region: {} does not exist", region_id);
        }

        // Make mysql packet.
        if (_make_common_resultset_packet(client, fields, rows) != 0) {
            TLOG_ERROR("{}, Failed to make result packet.", *client);
            _wrapper->make_err_packet(client, ER_MAKE_RESULT_PACKET, "Failed to make result packet.");
            client->state = STATE_ERROR;
            return false;
        }
        client->state = STATE_READ_QUERY_RESULT;
        return true;
    }

    bool ShowHelper::_show_virtual_index(const SmartSocket &client, const std::vector<std::string> &split_vec) {
        SchemaFactory *factory = SchemaFactory::get_instance();
        if (client == nullptr || client->query_ctx == nullptr || factory == nullptr) {
            TLOG_ERROR("param invalid");
            //client->state = STATE_ERROR;
            return false;
        }

        if (split_vec.size() != 3) {
            client->state = STATE_ERROR;
            return false;
        }

        std::vector<std::string> database_table;
        VirtualIndexMap sample = factory->get_virtual_index_info();
        std::vector<std::vector<std::string> > rows;
        rows.reserve(10);
        for (auto &iter: sample.index_id_sample_sqls_map) {
            std::string index_name = sample.index_id_name_map[iter.first];
            for (auto &sample_sql: iter.second) {
                std::vector<std::string> row;
                std::string database;
                std::string table;
                std::string sql;
                _parse_sample_sql(sample_sql, database, table, sql);
                uint64_t out[2];
                butil::MurmurHash3_x64_128(sample_sql.c_str(), sample_sql.size(), 0x1234, out);
                std::string sign = std::to_string(out[0]);
                row.emplace_back(database);
                row.emplace_back(table);
                row.emplace_back(index_name);
                row.emplace_back(sign);
                row.emplace_back(sql);
                rows.emplace_back(row);
            }
        }

        std::vector<std::string> names = {"database_name", "table_name", "virtual_index_name", "sign", "sql"};

        std::vector<ResultField> fields;
        fields.reserve(5);
        for (auto &name: names) {
            ResultField field;
            field.name = name;
            field.type = MYSQL_TYPE_STRING;
            fields.emplace_back(field);
        }
        // Make mysql packet.
        if (_make_common_resultset_packet(client, fields, rows) != 0) {
            TLOG_ERROR("{}, Failed to make result packet.", *client);
            _wrapper->make_err_packet(client, ER_MAKE_RESULT_PACKET, "Failed to make result packet.");
            client->state = STATE_ERROR;
            return false;
        }
        client->state = STATE_READ_QUERY_RESULT;
        return true;
    }

    bool ShowHelper::_show_variables(const SmartSocket &client, const std::vector<std::string> &split_vec) {
        // type == SQL_SHOW_NUM
        if (client == nullptr) {
            TLOG_ERROR("param invalid");
            //client->state = STATE_ERROR;
            return false;
        }

        // Make fields.
        std::vector<ResultField> fields;
        fields.reserve(2);
        do {
            ResultField field;
            field.name = "Variable_name";
            field.type = MYSQL_TYPE_VARCHAR;
            field.length = 1024;
            fields.emplace_back(field);
        } while (0);
        do {
            ResultField field;
            field.name = "Value";
            field.type = MYSQL_TYPE_VARCHAR;
            field.length = 1024;
            fields.emplace_back(field);
        } while (0);

        // Make rows.
        std::vector<std::vector<std::string> > rows;
        rows.reserve(10);
        do {
            std::vector<std::string> row;
            row.emplace_back("character_set_client");
            row.emplace_back(client->charset_name);
            rows.emplace_back(row);
        } while (0);
        do {
            std::vector<std::string> row;
            row.emplace_back("character_set_connection");
            row.emplace_back(client->charset_name);
            rows.emplace_back(row);
        } while (0);
        do {
            std::vector<std::string> row;
            row.emplace_back("character_set_results");
            row.emplace_back(client->charset_name);
            rows.emplace_back(row);
        } while (0);
        do {
            std::vector<std::string> row;
            row.emplace_back("character_set_server");
            row.emplace_back(client->charset_name);
            rows.emplace_back(row);
        } while (0);
        do {
            std::vector<std::string> row;
            row.emplace_back("init_connect");
            row.emplace_back(" ");
            rows.emplace_back(row);
        } while (0);
        do {
            std::vector<std::string> row;
            row.emplace_back("interactive_timeout");
            row.emplace_back("28800");
            rows.emplace_back(row);
        } while (0);
        do {
            std::vector<std::string> row;
            row.emplace_back("language");
            row.emplace_back("/home/mysql/mysql/share/mysql/english/");
            rows.emplace_back(row);
        } while (0);
        do {
            std::vector<std::string> row;
            row.emplace_back("lower_case_table_names");
            row.emplace_back("0");
            rows.emplace_back(row);
        } while (0);
        do {
            std::vector<std::string> row;
            row.emplace_back("max_allowed_packet");
            row.emplace_back("268435456");
            rows.emplace_back(row);
        } while (0);
        do {
            std::vector<std::string> row;
            row.emplace_back("net_buffer_length");
            row.emplace_back("16384");
            rows.emplace_back(row);
        } while (0);
        do {
            std::vector<std::string> row;
            row.emplace_back("net_write_timeout");
            row.emplace_back("60");
            rows.emplace_back(row);
        } while (0);
        do {
            std::vector<std::string> row;
            row.emplace_back("query_cache_size");
            row.emplace_back("335544320");
            rows.emplace_back(row);
        } while (0);
        do {
            std::vector<std::string> row;
            row.emplace_back("query_cache_type");
            row.emplace_back("OFF");
            rows.emplace_back(row);
        } while (0);
        do {
            std::vector<std::string> row;
            row.emplace_back("sql_mode");
            row.emplace_back(" ");
            rows.emplace_back(row);
        } while (0);
        do {
            std::vector<std::string> row;
            row.emplace_back("system_time_zone");
            row.emplace_back("CST");
            rows.emplace_back(row);
        } while (0);
        do {
            std::vector<std::string> row;
            row.emplace_back("time_zone");
            row.emplace_back("SYSTEM");
            rows.emplace_back(row);
        } while (0);
        do {
            std::vector<std::string> row;
            row.emplace_back("tx_isolation");
            row.emplace_back("REPEATABLE-READ");
            rows.emplace_back(row);
        } while (0);
        do {
            std::vector<std::string> row;
            row.emplace_back("wait_timeout");
            row.emplace_back("28800");
            rows.emplace_back(row);
        } while (0);
        do {
            std::vector<std::string> row;
            row.emplace_back("auto_increment_increment");
            row.emplace_back("1");
            rows.emplace_back(row);
        } while (0);

        // Make mysql packet.
        if (_make_common_resultset_packet(client, fields, rows) != 0) {
            TLOG_ERROR("{}, Failed to make result packet.", *client);
            _wrapper->make_err_packet(client, ER_MAKE_RESULT_PACKET, "Failed to make result packet.");
            client->state = STATE_ERROR;
            return false;
        }
        client->state = STATE_READ_QUERY_RESULT;
        return true;
    }

    bool ShowHelper::_show_user(const SmartSocket &client, const std::vector<std::string> &split_vec) {
        SchemaFactory *factory = SchemaFactory::get_instance();
        if (factory == nullptr || client == nullptr || split_vec.size() != 3) {
            TLOG_ERROR("param invalid");
            return false;
        }

        auto info = factory->get_user_info(split_vec[2]);
        if (info == nullptr) {
            TLOG_WARN("user name not exist [{}]", split_vec[2].c_str());
            _wrapper->make_err_packet(client, ER_NO_SUCH_USER, "No Such User");
            client->state = STATE_READ_QUERY_RESULT;
            return false;
        }

        // Make fields.
        std::vector<ResultField> fields;
        fields.reserve(5);
        std::vector<std::string> names = {"Username", "Password", "Namespace Name", "Version", "Auth IPs",
                                          "resource_tag", "ddl_permission", "use_read_index"};
        for (auto name: names) {
            ResultField field;
            field.name = name;
            field.type = MYSQL_TYPE_VARCHAR;
            field.length = 1024;
            fields.emplace_back(field);
        }

        // Make rows.
        std::vector<std::vector<std::string> > rows;
        rows.reserve(1);
        std::vector<std::string> row;
        row.emplace_back(info->username);
        row.emplace_back(info->password);
        row.emplace_back(info->namespace_);
        row.emplace_back(std::to_string(info->version));
        std::string ips;
        for (auto ip: info->auth_ip_set) {
            ips.append(ip);
        }
        row.emplace_back(ips);
        row.emplace_back(info->resource_tag);
        row.emplace_back(std::to_string(info->ddl_permission));
        row.emplace_back(std::to_string(info->use_read_index));
        rows.emplace_back(row);

        // Make mysql packet.
        if (_make_common_resultset_packet(client, fields, rows) != 0) {
            TLOG_ERROR("{}, Failed to make result packet.", *client);
            _wrapper->make_err_packet(client, ER_MAKE_RESULT_PACKET, "Failed to make result packet.");
            client->state = STATE_ERROR;
            return false;
        }
        client->state = STATE_READ_QUERY_RESULT;
        return true;
    }

    bool ShowHelper::_show_privilege(const SmartSocket &client, const std::vector<std::string> &split_vec) {
        SchemaFactory *factory = SchemaFactory::get_instance();
        if (factory == nullptr || client == nullptr || split_vec.size() != 3) {
            TLOG_ERROR("param invalid");
            return false;
        }

        const std::string &username = split_vec[2];
        auto info = factory->get_user_info(username);
        if (info == nullptr) {
            TLOG_WARN("user name not exist [{}]", username.c_str());
            _wrapper->make_err_packet(client, ER_NO_SUCH_USER, "No Such User");
            client->state = STATE_READ_QUERY_RESULT;
            return false;
        }

        // Make fields.
        std::vector<ResultField> fields;
        fields.reserve(5);
        std::vector<std::string> names = {"Database ID", "Database Name", "Table ID", "Table Name", "RW"};
        for (auto name: names) {
            ResultField field;
            field.name = name;
            field.type = MYSQL_TYPE_VARCHAR;
            field.length = 1024;
            fields.emplace_back(field);
        }

        proto::QueryRequest request;
        proto::QueryResponse response;
        request.set_op_type(proto::QUERY_USERPRIVILEG);
        request.set_user_name(username);
        MetaServerInteract::get_instance()->send_request("query", request, response);
        // Make rows.
        std::vector<std::vector<std::string> > rows;
        rows.reserve(10);
        for (auto user_privilege: response.user_privilege()) {
            for (auto db: user_privilege.privilege_database()) {
                std::vector<std::string> row;
                row.reserve(5);
                row.emplace_back(std::to_string(db.database_id()));
                row.emplace_back(db.database());
                row.emplace_back("*");
                row.emplace_back("*");
                row.emplace_back(db.database_rw() == proto::WRITE ? "write" : "read");
                rows.emplace_back(row);
            }
            for (auto table: user_privilege.privilege_table()) {
                std::vector<std::string> row;
                row.reserve(5);
                row.emplace_back(std::to_string(table.database_id()));
                row.emplace_back(table.database());
                row.emplace_back(std::to_string(table.table_id()));
                row.emplace_back(table.table_name());
                row.emplace_back(table.table_rw() == proto::WRITE ? "write" : "read");
                rows.emplace_back(row);
            }
        }

        // Make mysql packet.
        if (_make_common_resultset_packet(client, fields, rows) != 0) {
            TLOG_ERROR("{}, Failed to make result packet.", *client);
            _wrapper->make_err_packet(client, ER_MAKE_RESULT_PACKET, "Failed to make result packet.");
            client->state = STATE_ERROR;
            return false;
        }
        client->state = STATE_READ_QUERY_RESULT;
        return true;
    }

    bool ShowHelper::_show_store_txn(const SmartSocket &client, const std::vector<std::string> &split_vec) {
        SchemaFactory *factory = SchemaFactory::get_instance();
        if (client == nullptr || client->query_ctx == nullptr || factory == nullptr) {
            TLOG_ERROR("param invalid");
            //client->state = STATE_ERROR;
            return false;
        }

        if (split_vec.size() != 4) {
            client->state = STATE_ERROR;
            return false;
        }
        std::string store_addr = split_vec[2];
        int64_t region_id = strtoll(split_vec[3].c_str(), nullptr, 10);
        TLOG_WARN("region_id: {}", region_id);

        // Make fields.
        std::vector<ResultField> fields;
        std::vector<std::string> names = {"txn_id", "seq_id", "primary_region_id", "state"};
        std::unordered_map<proto::TxnState, std::string, std::hash<int>> state = {
                {proto::TXN_ROLLBACKED, "TXN_ROLLBACKED"},
                {proto::TXN_COMMITTED,  "TXN_COMMITTED"},
                {proto::TXN_PREPARED,   "TXN_PREPARED"},
                {proto::TXN_BEGINED,    "TXN_BEGINED"}
        };
        fields.reserve(4);
        for (auto name: names) {
            ResultField field;
            field.name = name;
            field.type = MYSQL_TYPE_VARCHAR;
            field.length = 1024;
            fields.emplace_back(field);
        }

        // Make rows.
        std::vector<std::vector<std::string> > rows;
        rows.reserve(10);

        proto::StoreReq req;
        req.set_region_id(region_id);
        req.set_op_type(proto::OP_TXN_QUERY_STATE);
        req.set_region_version(0);
        proto::StoreRes res;
        StoreInteract interact(store_addr);
        interact.send_request("query", req, res);
        TLOG_WARN("req:{} res:{}", req.ShortDebugString().c_str(), res.ShortDebugString().c_str());
        proto::RegionInfo region_info;
        for (auto txn_info: res.txn_infos()) {
            std::vector<std::string> row;
            row.emplace_back(std::to_string(txn_info.txn_id()));
            row.emplace_back(std::to_string(txn_info.seq_id()));
            row.emplace_back(std::to_string(txn_info.primary_region_id()));
            if (txn_info.has_txn_state() && state.find(txn_info.txn_state()) != state.end()) {
                row.emplace_back(state[txn_info.txn_state()]);
            } else {
                row.emplace_back("");
            }
            rows.emplace_back(row);
        }

        // Make mysql packet.
        if (_make_common_resultset_packet(client, fields, rows) != 0) {
            TLOG_ERROR("{}, Failed to make result packet.", *client);
            _wrapper->make_err_packet(client, ER_MAKE_RESULT_PACKET, "Failed to make result packet.");
            client->state = STATE_ERROR;
            return false;
        }
        client->state = STATE_READ_QUERY_RESULT;
        return true;
    }

    bool ShowHelper::_show_ddl_work(const SmartSocket &client, const std::vector<std::string> &split_vec) {
        std::unordered_map<proto::DdlWorkStatus, std::string, std::hash<int>> state = {
                {proto::DdlWorkIdle,    "DdlWorkIdle"},
                {proto::DdlWorkDoing,   "DdlWorkDoing"},
                {proto::DdlWorkDone,    "DdlWorkDone"},
                {proto::DdlWorkFail,    "DdlWorkFail"},
                {proto::DdlWorkDupUniq, "DdlWorkDupUniq"},
                {proto::DdlWorkError,   "DdlWorkError"}

        };
        std::unordered_map<proto::IndexState, std::string, std::hash<int>> index_state = {
                {proto::IS_PUBLIC,       "IS_PUBLIC"},
                {proto::IS_WRITE_LOCAL,  "IS_WRITE_LOCAL"},
                {proto::IS_WRITE_ONLY,   "IS_WRITE_ONLY"},
                {proto::IS_DELETE_ONLY,  "IS_DELETE_ONLY"},
                {proto::IS_DELETE_LOCAL, "IS_DELETE_LOCAL"},
                {proto::IS_NONE,         "IS_NONE"},
                {proto::IS_UNKNOWN,      "IS_UNKNOWN"},
        };

        SchemaFactory *factory = SchemaFactory::get_instance();
        if (client == nullptr || client->query_ctx == nullptr || factory == nullptr) {
            TLOG_ERROR("param invalid");
            return false;
        }

        bool show_region = false;
        bool show_column_ddl = false;
        if (split_vec.size() == 4 && turbo::EqualsIgnoreCase(split_vec[3], "region")) {
            show_region = true;
        } else if (split_vec.size() == 4 && turbo::EqualsIgnoreCase(split_vec[3], "column")) {
            show_column_ddl = true;
        } else if (split_vec.size() != 3) {
            TLOG_ERROR("param invalid");
            client->state = STATE_ERROR;
            return false;
        }
        const std::string &table_name = split_vec[2];
        int64_t table_id;
        std::string full_name = client->user_info->namespace_ + "." + client->current_db + "." + table_name;
        if (factory->get_table_id(full_name, table_id) != 0) {
            TLOG_ERROR("param invalid, no such table with table name: {}", full_name.c_str());
            client->state = STATE_ERROR;
            return false;
        }
        IndexInfo pri_info = factory->get_index_info(table_id);
        if (pri_info.id == -1) {
            TLOG_ERROR("param invalid, no such table with table name: {}", full_name.c_str());
            client->state = STATE_ERROR;
            return false;
        }
        // Make fields.
        std::vector<ResultField> fields;
        std::vector<std::string> names;
        names.reserve(10);
        fields.reserve(10);
        if (show_region) {
            names = {"index_id", "region_id", "status", "start_key", "end_key", "address"};
        } else if (show_column_ddl) {
            names = {"table", "status", "done/all", "cost_time", "opt sql"};
        } else {
            names = {"op_type", "index_state", "index_id", "begin_timestamp",
                     "end_timestamp", "rollback", "errcode", "deleted", "status",
                     "suspend", "update_timestamp", "global"};
        }
        for (auto name: names) {
            ResultField field;
            field.name = name;
            field.type = MYSQL_TYPE_VARCHAR;
            field.length = 1024;
            fields.emplace_back(field);
        }

        // Make rows.
        std::vector<std::vector<std::string> > rows;
        rows.reserve(10);
        proto::QueryRequest request;
        proto::QueryResponse response;
        request.set_op_type(proto::QUERY_DDLWORK);
        request.set_table_id(table_id);
        MetaServerInteract::get_instance()->send_request("query", request, response);
        TLOG_WARN("req:{} res:{}", request.ShortDebugString().c_str(), response.ShortDebugString().c_str());
        if (show_region) {
            int work_done_count = 0;
            int work_idle_count = 0;
            int work_doing_count = 0;
            int work_fail_count = 0;
            int work_error_count = 0;
            for (auto info: response.region_ddl_infos()) {
                std::vector<std::string> row;
                row.reserve(6);
                row.emplace_back(std::to_string(info.index_id()));
                row.emplace_back(std::to_string(info.region_id()));
                row.emplace_back(state[info.status()]);
                switch (info.status()) {
                    case proto::DdlWorkIdle: {
                        ++work_idle_count;
                        break;
                    }
                    case proto::DdlWorkDoing: {
                        ++work_doing_count;
                        break;
                    }
                    case proto::DdlWorkDone: {
                        ++work_done_count;
                        break;
                    }
                    case proto::DdlWorkFail: {
                        ++work_fail_count;
                        break;
                    }
                    case proto::DdlWorkDupUniq:
                    case proto::DdlWorkError: {
                        ++work_error_count;
                        break;
                    }
                    default:
                        break;
                }
                if (info.start_key().size() > 0) {
                    TableKey start_key(info.start_key());
                    row.emplace_back(start_key.decode_start_key_string(pri_info));
                } else {
                    row.emplace_back("-∞");
                }
                if (info.end_key().size() > 0) {
                    TableKey end_key(info.end_key());
                    row.emplace_back(end_key.decode_start_key_string(pri_info));
                } else {
                    row.emplace_back("+∞");
                }
                if (info.has_address()) {
                    row.emplace_back(info.address());
                } else {
                    row.emplace_back("");
                }
                rows.emplace_back(row);
            }
            if (!rows.empty()) {
                std::vector<std::string> row;
                row.reserve(6);
                row.emplace_back("Idle : " + std::to_string(work_idle_count));
                row.emplace_back("Doing : " + std::to_string(work_doing_count));
                row.emplace_back("Done : " + std::to_string(work_done_count));
                row.emplace_back("Fail : " + std::to_string(work_fail_count));
                row.emplace_back("Error : " + std::to_string(work_error_count));
                rows.emplace_back(row);
            }
        } else if (show_column_ddl) {
            int work_done_count = 0;
            int work_to_be_done_count = 0;
            for (auto info: response.region_ddl_infos()) {
                switch (info.status()) {
                    case proto::DdlWorkIdle:
                    case proto::DdlWorkDoing: {
                        ++work_to_be_done_count;
                        break;
                    }
                    case proto::DdlWorkDone:
                    case proto::DdlWorkFail:
                    case proto::DdlWorkDupUniq:
                    case proto::DdlWorkError: {
                        ++work_done_count;
                        break;
                    }
                    default:
                        break;
                }
            }
            for (auto ddl: response.ddlwork_infos()) {
                if (ddl.op_type() == proto::OP_MODIFY_FIELD) {
                    std::vector<std::string> row;
                    row.reserve(5);
                    row.emplace_back(client->current_db + "." + table_name);
                    row.emplace_back(ddl.has_status() ? state[ddl.status()] : "");
                    int64_t cost_s = butil::gettimeofday_s() - ddl.begin_timestamp();
                    if (ddl.job_state() == proto::IS_PUBLIC) {
                        cost_s = ddl.end_timestamp() - ddl.begin_timestamp();
                        row.emplace_back("   -  ");
                    } else {
                        row.emplace_back(std::to_string(work_done_count) + "/" +
                                         std::to_string(work_done_count + work_to_be_done_count));
                    }
                    int64_t hour = cost_s / (3600LL);
                    int64_t min = (cost_s - hour * 3600LL) / (60LL);
                    int64_t sec = cost_s - hour * 3600LL - min * 60LL;
                    std::string cost_time_str;
                    if (hour > 0) {
                        cost_time_str += std::to_string(hour);
                        cost_time_str += "h:";
                    }
                    if (min > 0) {
                        cost_time_str += std::to_string(min);
                        cost_time_str += "m:";
                    }
                    if (sec > 0) {
                        cost_time_str += std::to_string(sec);
                        cost_time_str += "s:";
                    }
                    if (cost_time_str.size() > 0) {
                        cost_time_str.pop_back();
                    }
                    row.emplace_back(cost_time_str);
                    row.emplace_back(ddl.opt_sql());
                    rows.emplace_back(row);
                }
            }
        } else {
            for (auto ddl: response.ddlwork_infos()) {
                if (ddl.op_type() == proto::OP_MODIFY_FIELD
                    && ddl.job_state() == proto::IS_PUBLIC) {
                    continue;
                }
                std::vector<std::string> row;
                row.emplace_back(std::to_string(ddl.op_type()));
                row.emplace_back(ddl.has_job_state() ? index_state[ddl.job_state()] : "");
                row.emplace_back(std::to_string(ddl.index_id()));
                row.emplace_back(std::to_string(ddl.begin_timestamp()));
                row.emplace_back(std::to_string(ddl.end_timestamp()));
                row.emplace_back(std::to_string(ddl.rollback()));
                row.emplace_back(std::to_string(ddl.errcode()));
                row.emplace_back(std::to_string(ddl.deleted()));
                row.emplace_back(ddl.has_status() ? state[ddl.status()] : "");
                row.emplace_back(std::to_string(ddl.suspend()));
                row.emplace_back(std::to_string(ddl.update_timestamp()));
                row.emplace_back(std::to_string(ddl.global()));
                rows.emplace_back(row);
            }
        }

        // Make mysql packet.
        if (_make_common_resultset_packet(client, fields, rows) != 0) {
            TLOG_ERROR("{}, Failed to make result packet.", *client);
            _wrapper->make_err_packet(client, ER_MAKE_RESULT_PACKET, "Failed to make result packet.");
            client->state = STATE_ERROR;
            return false;
        }
        client->state = STATE_READ_QUERY_RESULT;
        return true;
    }

    bool ShowHelper::_show_diff_region_size(const SmartSocket &client, const std::vector<std::string> &split_vec) {
        SchemaFactory *factory = SchemaFactory::get_instance();
        if (client == nullptr || client->query_ctx == nullptr || factory == nullptr) {
            TLOG_ERROR("param invalid");
            //client->state = STATE_ERROR;
            return false;
        }
        if (split_vec.size() < 3) {
            client->state = STATE_ERROR;
            return false;
        }
        bool ignore = false;
        double multiples = 1.5;
        if (split_vec.size() > 3) {
            multiples = strtod(split_vec[3].c_str(), nullptr);
        }
        if (split_vec.size() > 4) {
            ignore = true;
        }
        int64_t table_id = strtoll(split_vec[2].c_str(), nullptr, 10);
        TLOG_WARN("table_id: {}", table_id);

        // Make fields.
        std::vector<ResultField> fields;
        fields.reserve(2);
        do {
            ResultField field;
            field.name = "region_id";
            field.type = MYSQL_TYPE_VARCHAR;
            field.length = 1024;
            fields.emplace_back(field);
        } while (0);

        do {
            ResultField field;
            field.name = "lines";
            field.type = MYSQL_TYPE_VARCHAR;
            field.length = 1024;
            fields.emplace_back(field);
        } while (0);

        do {
            ResultField field;
            field.name = "peer:size";
            field.type = MYSQL_TYPE_VARCHAR;
            field.length = 1024;
            fields.emplace_back(field);
        } while (0);

        // Make rows.
        std::vector<std::vector<std::string> > rows;
        rows.reserve(10);

        std::map<std::string, proto::RegionInfo> region_infos;
        factory->get_all_region_by_table_id(table_id, &region_infos);
        std::map<std::string, std::vector<int64_t>> instance_regions;
        for (auto &pair: region_infos) {
            auto &info = pair.second;
            for (auto &ins: info.peers()) {
                instance_regions[ins].emplace_back(info.region_id());
            }
        }

        std::map<int64_t, std::vector<std::tuple<int64_t, std::string, int64_t>>> region_sizes;
        bthread::Mutex mutex;
        BthreadCond cond;
        bool err = false;
        for (auto &pair: instance_regions) {
            Bthread bth;
            cond.increase();
            bth.run([&cond, &mutex, &region_sizes, &err, pair, ignore]() {
                auto &store_addr = pair.first;
                proto::RegionIds req;
                for (auto &id: pair.second) {
                    req.add_region_ids(id);
                }
                proto::StoreRes res;
                int retry = 0;
                while (++retry <= 3) {
                    StoreInteract interact(store_addr);
                    interact.send_request("query_region", req, res);
                    if (res.errcode() == proto::SUCCESS) {
                        break;
                    }
                }
                if (res.errcode() != proto::SUCCESS) {
                    if (!ignore) {
                        cond.decrease_signal();
                        err = true;
                        return;
                    }
                }
                std::unique_lock<bthread::Mutex> lck(mutex);
                for (auto &info: res.regions()) {
                    region_sizes[info.region_id()].emplace_back(info.used_size(), store_addr, info.num_table_lines());
                }
                cond.decrease_signal();
            });
        }
        cond.wait();
        if (err) {
            TLOG_ERROR("error");
            client->state = STATE_ERROR;
            return false;
        }
        auto size_str = [](int64_t size) {
            std::ostringstream oss;
            if (size > 1024 * 1024) {
                oss << size / 1024 / 1024 << "m";
            } else if (size > 1024) {
                oss << size / 1024 << "k";
            } else {
                oss << size;
            }
            return oss.str();
        };
        for (auto &pair: region_sizes) {
            int64_t region_id = pair.first;
            int64_t first_size = 0;
            bool first = true;
            bool diff = false;
            for (auto pair2: pair.second) {
                if (first) {
                    first_size = std::get<0>(pair2);
                    first = false;
                } else {
                    if (first_size != 0) {
                        if (std::get<0>(pair2) * 1.0 / first_size > multiples ||
                            first_size * 1.0 / std::get<0>(pair2) > multiples) {
                            diff = true;
                        }
                    } else {
                        if (std::get<0>(pair2) != 0) {
                            diff = true;
                        }
                    }
                }
            }
            if (diff) {
                std::vector<std::string> row;
                row.reserve(3);
                row.emplace_back(std::to_string(region_id));
                std::ostringstream oss;
                int64_t lines = 0;
                for (auto pair2: pair.second) {
                    lines = std::get<2>(pair2);
                    oss << std::get<1>(pair2) << ":" << size_str(std::get<0>(pair2)) << ",";
                }
                row.emplace_back(std::to_string(lines));
                row.emplace_back(oss.str());
                rows.emplace_back(row);
            }
        }

        // Make mysql packet.
        if (_make_common_resultset_packet(client, fields, rows) != 0) {
            TLOG_ERROR("{}, Failed to make result packet.", *client);
            _wrapper->make_err_packet(client, ER_MAKE_RESULT_PACKET, "Failed to make result packet.");
            client->state = STATE_ERROR;
            return false;
        }
        client->state = STATE_READ_QUERY_RESULT;
        return true;
    }

    bool ShowHelper::_show_global_ddl_work(const SmartSocket &client, const std::vector<std::string> &split_vec) {
        SchemaFactory *factory = SchemaFactory::get_instance();
        if (client == nullptr || client->query_ctx == nullptr || factory == nullptr) {
            TLOG_ERROR("param invalid");
            return false;
        }

        if (split_vec.size() != 3) {
            client->state = STATE_ERROR;
            return false;
        }
        const std::string &table_name = split_vec[2];

        // Make fields.
        std::vector<ResultField> fields;
        std::vector<std::string> names = {"region_id", "start_key", "end_key", "status", "op_type", "index_id",
                                          "address", "retry_time", "update_timestamp", "partition"};
        std::unordered_map<proto::DdlWorkStatus, std::string, std::hash<int>> state = {
                {proto::DdlWorkIdle,    "DdlWorkIdle"},
                {proto::DdlWorkDoing,   "DdlWorkDoing"},
                {proto::DdlWorkDone,    "DdlWorkDone"},
                {proto::DdlWorkFail,    "DdlWorkFail"},
                {proto::DdlWorkDupUniq, "DdlWorkDupUniq"},
                {proto::DdlWorkError,   "DdlWorkError"}
        };
        fields.reserve(10);
        for (auto name: names) {
            ResultField field;
            field.name = name;
            field.type = MYSQL_TYPE_VARCHAR;
            field.length = 1024;
            fields.emplace_back(field);
        }

        int64_t table_id;
        std::string full_name = client->user_info->namespace_ + "." + client->current_db + "." + table_name;
        if (factory->get_table_id(full_name, table_id) != 0) {
            TLOG_ERROR("param invalid, no such table with table name: {}", full_name.c_str());
            client->state = STATE_ERROR;
            return false;
        }

        // Make rows.
        std::vector<std::vector<std::string> > rows;
        rows.reserve(10);
        proto::QueryRequest request;
        proto::QueryResponse response;
        request.set_op_type(proto::QUERY_INDEX_DDL_WORK);
        request.set_table_id(table_id);
        MetaServerInteract::get_instance()->send_request("query", request, response);
        //TLOG_WARN("req:{} res:{}", request.ShortDebugString().c_str(), response.ShortDebugString().c_str());

        auto index_info = factory->get_index_info(table_id);
        for (auto &ddl: response.region_ddl_infos()) {
            std::vector<std::string> row;
            row.emplace_back(std::to_string(ddl.region_id()));
            if (ddl.start_key().size() > 0) {
                TableKey start_key(ddl.start_key());
                row.emplace_back(start_key.decode_start_key_string(index_info));
            } else {
                row.emplace_back("-∞");
            }
            if (ddl.end_key().size() > 0) {
                TableKey end_key(ddl.end_key());
                row.emplace_back(end_key.decode_start_key_string(index_info));
            } else {
                row.emplace_back("+∞");
            }
            row.emplace_back(ddl.has_status() ? state[ddl.status()] : "");
            row.emplace_back(ddl.has_op_type() ? std::to_string(ddl.op_type()) : "");
            row.emplace_back(ddl.has_index_id() ? std::to_string(ddl.index_id()) : "");
            row.emplace_back(ddl.has_address() ? ddl.address() : "");
            row.emplace_back(ddl.has_retry_time() ? std::to_string(ddl.retry_time()) : "");
            row.emplace_back(ddl.has_update_timestamp() ? std::to_string(ddl.update_timestamp()) : "");
            row.emplace_back(ddl.has_partition() ? std::to_string(ddl.partition()) : "");
            rows.emplace_back(row);
        }

        // Make mysql packet.
        if (_make_common_resultset_packet(client, fields, rows) != 0) {
            TLOG_ERROR("{}, Failed to make result packet.", *client);
            _wrapper->make_err_packet(client, ER_MAKE_RESULT_PACKET, "Failed to make result packet.");
            client->state = STATE_ERROR;
            return false;
        }
        client->state = STATE_READ_QUERY_RESULT;
        return true;
    }

    bool ShowHelper::_show_partition_table(const SmartSocket &client, const std::vector<std::string> &split_vec) {
        SchemaFactory *factory = SchemaFactory::get_instance();
        if (client == nullptr || client->query_ctx == nullptr || factory == nullptr) {
            TLOG_ERROR("param invalid");
            return false;
        }

        // Make fields.
        std::vector<ResultField> fields;
        std::vector<std::string> names = {"db.table", "partition_type", "partition_field", "partition_expr"};
        fields.reserve(10);
        for (auto name: names) {
            ResultField field;
            field.name = name;
            field.type = MYSQL_TYPE_VARCHAR;
            field.length = 1024;
            fields.emplace_back(field);
        }
        std::string namespace_ = client->user_info->namespace_;
        // Make rows.
        std::vector<std::vector<std::string> > rows;
        rows.reserve(10);
        auto tb_vec = factory->get_table_list(namespace_, nullptr);
        for (auto &table_info: tb_vec) {
            std::vector<std::string> row;
            if (table_info != nullptr && table_info->partition_ptr != nullptr) {
                row.emplace_back(table_info->name);
                row.emplace_back(proto::PartitionType_Name(table_info->partition_info.type()));
                row.emplace_back(table_info->partition_info.field_info().field_name());
                if (table_info->partition_info.has_expr_string()) {
                    row.emplace_back(table_info->partition_info.expr_string());
                } else if (table_info->partition_info.has_range_partition_field()) {
                    row.emplace_back(table_info->partition_info.range_partition_field().ShortDebugString());
                } else if (table_info->partition_info.range_partition_values_size() > 0) {
                    row.emplace_back(table_info->partition_info.range_partition_values(0).ShortDebugString());
                } else {
                    row.emplace_back("---");
                }
                rows.emplace_back(row);
            }
        }

        // Make mysql packet.
        if (_make_common_resultset_packet(client, fields, rows) != 0) {
            TLOG_ERROR("{}, Failed to make result packet.", *client);
            _wrapper->make_err_packet(client, ER_MAKE_RESULT_PACKET, "Failed to make result packet.");
            client->state = STATE_ERROR;
            return false;
        }
        client->state = STATE_READ_QUERY_RESULT;
        return true;
    }

    bool ShowHelper::_show_network_segment(const SmartSocket &client, const std::vector<std::string> &split_vec) {
        if (client == nullptr || client->query_ctx == nullptr) {
            TLOG_ERROR("param invalid");
            //client->state = STATE_ERROR;
            return false;
        }

        std::string resource_tag;
        if (split_vec.size() == 3) {
            resource_tag = split_vec[2];
        } else if (split_vec.size() != 2) {
            client->state = STATE_ERROR;
            return false;
        }

        std::vector<ResultField> fields;
        fields.reserve(3);
        do {
            ResultField field;
            field.name = "resource tag";
            field.type = MYSQL_TYPE_VARCHAR;
            field.length = 1024;
            fields.emplace_back(field);
        } while (0);
        do {
            ResultField field;
            field.name = "network segment";
            field.type = MYSQL_TYPE_VARCHAR;
            field.length = 1024;
            fields.emplace_back(field);
        } while (0);
        do {
            ResultField field;
            field.name = "instance address";
            field.type = MYSQL_TYPE_VARCHAR;
            field.length = 1024;
            fields.emplace_back(field);
        } while (0);

        // Make rows.
        std::vector<std::vector<std::string> > rows;
        rows.reserve(10);
        proto::QueryRequest request;
        proto::QueryResponse response;
        request.set_op_type(proto::QUERY_NETWORK_SEGMENT);
        request.set_resource_tag(resource_tag);
        MetaServerInteract::get_instance()->send_request("query", request, response);
        TLOG_WARN("req:{} res:{}", request.ShortDebugString().c_str(), response.ShortDebugString().c_str());

        for (auto &info: response.instance_infos()) {
            std::vector<std::string> row = {info.resource_tag(), info.network_segment(), info.address()};
            rows.emplace_back(row);
        }
        // Make mysql packet.
        if (_make_common_resultset_packet(client, fields, rows) != 0) {
            TLOG_ERROR("{}, Failed to make result packet.", *client);
            _wrapper->make_err_packet(client, ER_MAKE_RESULT_PACKET, "Failed to make result packet.");
            client->state = STATE_ERROR;
            return false;
        }
        client->state = STATE_READ_QUERY_RESULT;
        return true;
    }

    bool ShowHelper::_show_switch(const SmartSocket &client, const std::vector<std::string> &split_vec) {
        if (client == nullptr || client->query_ctx == nullptr) {
            TLOG_ERROR("param invalid");
            //client->state = STATE_ERROR;
            return false;
        }

        std::string resource_tag;
        if (split_vec.size() == 3) {
            resource_tag = split_vec[2];
        } else if (split_vec.size() != 2) {
            client->state = STATE_ERROR;
            return false;
        }

        std::vector<ResultField> fields;
        fields.reserve(4);
        std::vector<std::string> names = {"resource tag", "peer load balance", "migrate balance",
                                          "network segment balance"};
        for (auto &name: names) {
            ResultField field;
            field.name = name;
            field.type = MYSQL_TYPE_VARCHAR;
            field.length = 1024;
            fields.emplace_back(field);
        }

        // Make rows.
        std::vector<std::vector<std::string> > rows;
        rows.reserve(10);
        proto::QueryRequest request;
        proto::QueryResponse response;
        request.set_op_type(proto::QUERY_RESOURCE_TAG_SWITCH);
        request.set_resource_tag(resource_tag);
        MetaServerInteract::get_instance()->send_request("query", request, response);
        TLOG_WARN("req:{} res:{}", request.ShortDebugString().c_str(), response.ShortDebugString().c_str());
        for (auto &info: response.resource_tag_infos()) {
            std::vector<std::string> row = {info.resource_tag(),
                                            info.peer_load_balance() ? "true" : "false",
                                            info.migrate() ? "true" : "false",
                                            info.network_segment_balance() ? "true" : "false"};
            rows.emplace_back(row);
        }
        // Make mysql packet.
        if (_make_common_resultset_packet(client, fields, rows) != 0) {
            TLOG_ERROR("{}, Failed to make result packet.", *client);
            _wrapper->make_err_packet(client, ER_MAKE_RESULT_PACKET, "Failed to make result packet.");
            client->state = STATE_ERROR;
            return false;
        }
        client->state = STATE_READ_QUERY_RESULT;
        return true;
    }

    bool ShowHelper::_show_instance_param(const SmartSocket &client, const std::vector<std::string> &split_vec) {
        if (client == nullptr || client->query_ctx == nullptr) {
            TLOG_ERROR("param invalid");
            //client->state = STATE_ERROR;
            return false;
        }

        std::string resource_tag_or_instance = "";
        if (split_vec.size() == 3) {
            resource_tag_or_instance = split_vec[2];
        } else if (split_vec.size() != 2) {
            client->state = STATE_ERROR;
            return false;
        }
        std::vector<ResultField> fields;
        fields.reserve(4);
        std::vector<std::string> names = {"resource tag or instance", "key", "value", "is meta"};
        for (auto &name: names) {
            ResultField field;
            field.name = name;
            field.type = MYSQL_TYPE_VARCHAR;
            field.length = 1024;
            fields.emplace_back(field);
        }

        // Make rows.
        std::vector<std::vector<std::string> > rows;
        rows.reserve(10);
        proto::QueryRequest request;
        proto::QueryResponse response;
        request.set_op_type(proto::QUERY_INSTANCE_PARAM);
        request.set_resource_tag(resource_tag_or_instance);
        MetaServerInteract::get_instance()->send_request("query", request, response);
        TLOG_WARN("req:{} res:{}", request.ShortDebugString().c_str(), response.ShortDebugString().c_str());
        for (auto &info: response.instance_params()) {
            for (auto &kv: info.params()) {
                std::vector<std::string> row = {info.resource_tag_or_address(),
                                                kv.key(),
                                                kv.value(),
                                                kv.is_meta_param() ? "true" : "false"};
                rows.emplace_back(row);
            }
        }
        // Make mysql packet.
        if (_make_common_resultset_packet(client, fields, rows) != 0) {
            TLOG_ERROR("{}, Failed to make result packet.", *client);
            _wrapper->make_err_packet(client, ER_MAKE_RESULT_PACKET, "Failed to make result packet.");
            client->state = STATE_ERROR;
            return false;
        }
        client->state = STATE_READ_QUERY_RESULT;
        return true;
    }

    bool ShowHelper::_show_engines(const SmartSocket &client, const std::vector<std::string> &split_vec) {
        if (client == nullptr) {
            TLOG_ERROR("param invalid");
            //client->state = STATE_ERROR;
            return false;
        }

        // Make fields.
        std::vector<ResultField> fields;
        fields.emplace_back(make_result_field("Engine", MYSQL_TYPE_VARCHAR, 1024));
        fields.emplace_back(make_result_field("Support", MYSQL_TYPE_VARCHAR, 1024));
        fields.emplace_back(make_result_field("Comment", MYSQL_TYPE_VARCHAR, 1024));
        fields.emplace_back(make_result_field("Transactions", MYSQL_TYPE_VARCHAR, 1024));
        fields.emplace_back(make_result_field("XA", MYSQL_TYPE_VARCHAR, 1024));
        fields.emplace_back(make_result_field("Savepoints", MYSQL_TYPE_VARCHAR, 1024));

        // Make rows.
        std::vector<std::vector<std::string> > rows;
        rows.reserve(10);
        do {
            std::vector<std::string> row;
            rows.push_back({"InnoDB", "DEFAULT", "Supports transactions, row-level locking, and foreign keys",
                            "YES", "YES", "YES"});
        } while (0);

        // Make mysql packet.
        if (_make_common_resultset_packet(client, fields, rows) != 0) {
            TLOG_ERROR("{}, Failed to make result packet.", *client);
            _wrapper->make_err_packet(client, ER_MAKE_RESULT_PACKET, "Failed to make result packet.");
            client->state = STATE_ERROR;
            return false;
        }
        client->state = STATE_READ_QUERY_RESULT;
        return true;
    }

    bool ShowHelper::_show_charset(const SmartSocket &client, const std::vector<std::string> &split_vec) {
        if (client == nullptr) {
            TLOG_ERROR("param invalid");
            //client->state = STATE_ERROR;
            return false;
        }

        // Make fields.
        std::vector<ResultField> fields;
        fields.emplace_back(make_result_field("Charset", MYSQL_TYPE_VARCHAR, 1024));
        fields.emplace_back(make_result_field("Description", MYSQL_TYPE_VARCHAR, 1024));
        fields.emplace_back(make_result_field("Default collation", MYSQL_TYPE_VARCHAR, 1024));
        fields.emplace_back(make_result_field("Maxlen", MYSQL_TYPE_VARCHAR, 1024));

        // Make rows.
        std::vector<std::vector<std::string> > rows;
        rows.reserve(10);
        std::vector<std::string> row;
        rows.push_back({"utf8", "UTF-8 Unicode", "utf8_general_ci", "3"});
        rows.push_back({"gbk", "GBK Simplified Chinese", "gbk_chinese_ci", "2"});

        // Make mysql packet.
        if (_make_common_resultset_packet(client, fields, rows) != 0) {
            TLOG_ERROR("{}, Failed to make result packet.", *client);
            _wrapper->make_err_packet(client, ER_MAKE_RESULT_PACKET, "Failed to make result packet.");
            client->state = STATE_ERROR;
            return false;
        }
        client->state = STATE_READ_QUERY_RESULT;
        return true;
    }

    bool ShowHelper::_show_index(const SmartSocket &client, const std::vector<std::string> &split_vec) {
        // not-support yet: [WHERE expr]
        SchemaFactory *factory = SchemaFactory::get_instance();
        if (client == nullptr || client->user_info == nullptr || factory == nullptr) {
            TLOG_ERROR("param invalid");
            //client->state = STATE_ERROR;
            return false;
        }
        // Make fields.
        std::vector<ResultField> fields;
        fields.reserve(15);
        fields.emplace_back(make_result_field("Table", MYSQL_TYPE_VARCHAR, 1024));
        fields.emplace_back(make_result_field("Non_unique", MYSQL_TYPE_LONG, 10));
        fields.emplace_back(make_result_field("Key_name", MYSQL_TYPE_VARCHAR, 1024));
        fields.emplace_back(make_result_field("Seq_in_index", MYSQL_TYPE_LONG, 10));
        fields.emplace_back(make_result_field("Column_name", MYSQL_TYPE_VARCHAR, 1024));
        fields.emplace_back(make_result_field("Collation", MYSQL_TYPE_VARCHAR, 1024));
        fields.emplace_back(make_result_field("Cardinality", MYSQL_TYPE_LONG, 10));
        fields.emplace_back(make_result_field("Sub_part", MYSQL_TYPE_VARCHAR, 1024));
        fields.emplace_back(make_result_field("Packed", MYSQL_TYPE_VARCHAR, 1024));
        fields.emplace_back(make_result_field("Null", MYSQL_TYPE_VARCHAR, 1024));
        fields.emplace_back(make_result_field("Index_type", MYSQL_TYPE_VARCHAR, 1024));
        fields.emplace_back(make_result_field("Comment", MYSQL_TYPE_VARCHAR, 1024));
        fields.emplace_back(make_result_field("Index_comment", MYSQL_TYPE_VARCHAR, 1024));

        std::string db = client->current_db;
        std::string table;
        if (split_vec.size() == 4) {
            std::string db_table = split_vec[3];
            std::string::size_type position = db_table.find_first_of('.');
            if (position == std::string::npos) {
                // `table_name`
                table = remove_quote(db_table.c_str(), '`');
            } else {
                // `db_name`.`table_name`
                db = remove_quote(db_table.substr(0, position).c_str(), '`');
                table = remove_quote(db_table.substr(position + 1,
                                                     db_table.length() - position - 1).c_str(), '`');
            }
        } else if (split_vec.size() == 6) {
            db = remove_quote(split_vec[5].c_str(), '`');
            table = remove_quote(split_vec[3].c_str(), '`');
        } else {
            client->state = STATE_ERROR;
            return false;
        }
        std::string namespace_ = client->user_info->namespace_;
        if (db == "information_schema") {
            namespace_ = "INTERNAL";
        }
        std::string full_name = namespace_ + "." + db + "." + table;
        int64_t table_id = -1;
        if (factory->get_table_id(full_name, table_id) != 0) {
            client->state = STATE_ERROR;
            return false;
        }
        TableInfo info = factory->get_table_info(table_id);
        std::map<int32_t, IndexInfo> field_index;
        for (auto &index_id: info.indices) {
            IndexInfo index_info = factory->get_index_info(index_id);
        }
        // Make rows.
        std::vector<std::vector<std::string> > rows;
        rows.reserve(info.indices.size());
        uint32_t index_idx = 0;
        for (auto &index_id: info.indices) {
            IndexInfo index_info = factory->get_index_info(index_id);
            if (index_info.index_hint_status == proto::IHS_DISABLE && index_info.state == proto::IS_DELETE_LOCAL) {
                continue;
            }
            bool non_unique = index_info.type != proto::I_PRIMARY && index_info.type != proto::I_UNIQ;
            std::string key_name = "PRIMARY";
            if (index_info.type != proto::I_PRIMARY) {
                std::vector<std::string> split_vec = turbo::StrSplit(index_info.name, turbo::ByChar('.'));
                key_name = split_vec[split_vec.size() - 1];
            }
            for (size_t i = 0; i < index_info.fields.size(); ++i) {
                rows.push_back({table, std::to_string(non_unique), key_name, std::to_string(i),
                                index_info.fields[i].short_name, "A", "2", "nullptr", "nullptr", "", "BTREE", "", ""});
            }
        }
        client->state = STATE_READ_QUERY_RESULT;
        return true;
    }

    bool ShowHelper::_show_abnormal_switch(const SmartSocket &client, const std::vector<std::string> &split_vec) {
        SchemaFactory *factory = SchemaFactory::get_instance();
        if (client == nullptr || client->query_ctx == nullptr || factory == nullptr) {
            TLOG_ERROR("param invalid");
            //client->state = STATE_ERROR;
            return false;
        }
        std::vector<ResultField> fields;
        fields.reserve(3);
        std::vector<std::string> names = {"switch", "table or resource_tag", "value"};
        for (auto &name: names) {
            ResultField field;
            field.name = name;
            field.type = MYSQL_TYPE_VARCHAR;
            field.length = 1024;
            fields.emplace_back(field);
        }
        std::vector<std::vector<std::string> > rows;
        rows.reserve(10);

        // fast_importer > 24h
        {
            proto::QueryRequest request;
            proto::QueryResponse response;
            request.set_op_type(proto::QUERY_FAST_IMPORTER_TABLES);
            MetaServerInteract::get_instance()->send_request("query", request, response);
            for (auto &tb: response.flatten_tables()) {
                if (tb.fast_importer_ts() > 24 * 60 * 60 * 1000 * 1000ULL) {
                    std::string full_name = tb.namespace_name() + "." + tb.table_name();
                    std::vector<std::string> row = {"fast_importer", full_name, std::to_string(tb.fast_importer_ts())};
                    rows.emplace_back(row);
                }
            }
        }

        // load_balance, migrate is close
        {
            proto::QueryRequest request;
            proto::QueryResponse response;
            request.set_op_type(proto::QUERY_RESOURCE_TAG_SWITCH);
            request.set_resource_tag("");
            MetaServerInteract::get_instance()->send_request("query", request, response);
            for (auto &info: response.resource_tag_infos()) {
                if (!info.peer_load_balance()) {
                    std::vector<std::string> row = {"peer_load_balance", info.resource_tag(), "false"};
                    rows.emplace_back(row);
                }
                if (!info.migrate()) {
                    std::vector<std::string> row = {"migrate", info.resource_tag(), "false"};
                    rows.emplace_back(row);
                }
            }
        }

        // table split_lines > 1000,0000
        {
            std::unordered_map<int64_t, int64_t> table_id_split_lines_map;
            factory->get_all_table_split_lines(table_id_split_lines_map, 10000000);
            for (auto &abnormal_split_line: table_id_split_lines_map) {
                int64_t table_id = abnormal_split_line.first;
                std::string table_name = std::to_string(table_id);
                auto table_info = factory->get_table_info(table_id);
                if (table_info.name != "") {
                    table_name = table_info.name;
                }
                std::vector<std::string> row = {"split_line",
                                                table_name,
                                                std::to_string(abnormal_split_line.second)};
                rows.emplace_back(row);
            }
        }

        // Make mysql packet.
        if (_make_common_resultset_packet(client, fields, rows) != 0) {
            TLOG_ERROR("{}, Failed to make result packet.", *client);
            _wrapper->make_err_packet(client, ER_MAKE_RESULT_PACKET, "Failed to make result packet.");
            client->state = STATE_ERROR;
            return false;
        }
        client->state = STATE_READ_QUERY_RESULT;
        return true;
    }

    bool ShowHelper::_handle_client_query_template_dispatch(const SmartSocket &client,
                                                            const std::vector<std::string> &split_vec) {
        if (turbo::EqualsIgnoreCase(split_vec[1], "meta")) {
            return _handle_client_query_template(client, "Meta",
                                                 MYSQL_TYPE_VARCHAR, {FLAGS_meta_server_bns});
        } else if (turbo::EqualsIgnoreCase(split_vec[1], "namespace")) {
            return _handle_client_query_template(client, "Namespace", MYSQL_TYPE_VARCHAR,
                                                 {client->user_info->namespace_});
        } else if (turbo::EqualsIgnoreCase(split_vec[1], "disable")) {
            std::vector<std::string> indexs;
            indexs.reserve(10);
            if (SchemaFactory::get_instance()->get_disable_indexs(indexs) != 0) {
                TLOG_WARN("get disable index error.");
            }
            return _handle_client_query_template(client, "Disable Indexs", MYSQL_TYPE_VARCHAR,
                                                 indexs);
        }
        return false;
    }

    bool ShowHelper::_handle_client_query_template(const SmartSocket &client,
                                                   const std::string &field_name, int32_t data_type,
                                                   const std::vector<std::string> &values) {
        if (client == nullptr) {
            TLOG_ERROR("param invalid");
            //client->state = STATE_ERROR;
            return false;
        }

        // Make fields.
        std::vector<ResultField> fields;
        fields.reserve(1);
        do {
            ResultField field;
            field.name = field_name.c_str();
            field.type = data_type;
            field.length = 1024;
            fields.emplace_back(field);
        } while (0);

        // make rows
        std::vector<std::vector<std::string> > rows;
        rows.reserve(10);
        for (const auto &value: values) {
            std::vector<std::string> row;
            row.reserve(1);
            row.emplace_back(value);
            rows.emplace_back(row);
        }

        // Make mysql packet.
        if (_make_common_resultset_packet(client, fields, rows) != 0) {
            TLOG_ERROR("{}, Failed to make result packet.", *client);
            _wrapper->make_err_packet(client, ER_MAKE_RESULT_PACKET, "Failed to make result packet.");
            client->state = STATE_ERROR;
            return false;
        }
        client->state = STATE_READ_QUERY_RESULT;
        return true;
    }

    int ShowHelper::_make_common_resultset_packet(const SmartSocket &sock,
                                                  std::vector<ResultField> &fields,
                                                  const std::vector<std::vector<std::string>> &rows) {
        if (!sock || !sock->send_buf) {
            TLOG_ERROR("sock == nullptr.");
            return RET_ERROR;
        }
        if (fields.size() == 0) {
            TLOG_ERROR("Field size is 0.");
            return RET_ERROR;
        }

        //Result Set Header Packet
        int start_pos = sock->send_buf->_size;
        if (!sock->send_buf->byte_array_append_len((const uint8_t *) "\x01\x00\x00\x01", 4)) {
            TLOG_ERROR("byte_array_append_len failed.");
            return RET_ERROR;
        }
        if (!sock->send_buf->byte_array_append_length_coded_binary(fields.size())) {
            TLOG_ERROR("byte_array_append_len failed. len:[{}]", fields.size());
            return RET_ERROR;
        }
        int packet_body_len = sock->send_buf->_size - start_pos - 4;
        sock->send_buf->_data[start_pos] = packet_body_len & 0xFF;
        sock->send_buf->_data[start_pos + 1] = (packet_body_len >> 8) & 0xFF;
        sock->send_buf->_data[start_pos + 2] = (packet_body_len >> 16) & 0xFF;
        sock->send_buf->_data[start_pos + 3] = (++sock->packet_id) & 0xFF;
        // Make field packets
        for (uint32_t cnt = 0; cnt < fields.size(); ++cnt) {
            fields[cnt].catalog = "baikal";
            if (sock->query_ctx != nullptr) {
                fields[cnt].db = sock->query_ctx->cur_db;
            }
            fields[cnt].table.clear();
            fields[cnt].org_table.clear();
            fields[cnt].org_name = fields[cnt].name;
            _wrapper->make_field_packet(sock->send_buf, &fields[cnt], ++sock->packet_id);
        }

        // Make EOF packet
        _wrapper->make_eof_packet(sock->send_buf, ++sock->packet_id);

        // Make row packets
        for (uint32_t cnt = 0; cnt < rows.size(); ++cnt) {
            // Make row data packet
            if (!_wrapper->make_row_packet(sock->send_buf, rows[cnt], ++sock->packet_id)) {
                TLOG_ERROR("make_row_packet failed");
                return RET_ERROR;
            }
        }
        // Make EOF packet
        _wrapper->make_eof_packet(sock->send_buf, ++sock->packet_id);
        return 0;
    }

    void
    ShowHelper::_parse_sample_sql(std::string sample_sql, std::string &database, std::string &table, std::string &sql) {
        // Remove comments.
        re2::RE2::Options option;
        option.set_encoding(RE2::Options::EncodingLatin1);
        option.set_case_sensitive(false);
        option.set_perl_classes(true);

        re2::RE2 reg("family_table_tag_optype_plat=\\[(.*)\t(.*)\t.*\t.*\t.*sql=\\[(.*)\\]", option);

        if (!RE2::Extract(sample_sql, reg, "\\1", &database)) {
            TLOG_WARN("extract commit error.");
        }
        if (!RE2::Extract(sample_sql, reg, "\\2", &table)) {
            TLOG_WARN("extract commit error.");
        }
        if (!RE2::Extract(sample_sql, reg, "\\3", &sql)) {
            TLOG_WARN("extract commit error.");
        }

        TLOG_WARN("sample_sql: {}, database: {}, table: {}, sql: {}", sample_sql.c_str(), database.c_str(),
                   table.c_str(), sql.c_str());
    }
}
