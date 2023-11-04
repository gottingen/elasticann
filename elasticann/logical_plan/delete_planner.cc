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


#include "elasticann/logical_plan/delete_planner.h"
#include "elasticann/rpc/meta_server_interact.h"
#include <gflags/gflags.h>
#include "elasticann/session/network_socket.h"

namespace EA {

    int DeletePlanner::plan() {
        if (_ctx->stmt_type == parser::NT_TRUNCATE) {
            if (_ctx->client_conn->txn_id != 0) {
                if (_ctx->stat_info.error_code == ER_ERROR_FIRST) {
                    _ctx->stat_info.error_code = ER_NOT_ALLOWED_COMMAND;
                    _ctx->stat_info.error_msg.str("not allowed truncate in transaction");
                }
                TLOG_ERROR("not allowed truncate table in txn connection txn_id:{}",
                         _ctx->client_conn->txn_id);
                return -1;
            }
            _truncate_stmt = (parser::TruncateStmt *) (_ctx->stmt);
            for (int i = 0; i < _truncate_stmt->partition_names.size(); ++i) {
                std::string lower_name = _truncate_stmt->partition_names[i].value;
                std::transform(lower_name.begin(), lower_name.end(), lower_name.begin(), ::tolower);
                _partition_names.emplace_back(lower_name);
            }
            if (0 != parse_db_tables(_truncate_stmt->table_name)) {
                TLOG_WARN("get truncate_table plan failed");
                return -1;
            }
            create_packet_node(proto::OP_TRUNCATE_TABLE);
            if (0 != create_truncate_node()) {
                TLOG_WARN("get truncate_table plan failed");
                return -1;
            }
            if (0 != reset_auto_incr_id()) {
                return -1;
            }
            return 0;
        }
        _delete_stmt = (parser::DeleteStmt *) (_ctx->stmt);
        if (!_delete_stmt) {
            return -1;
        }
        if (_delete_stmt->delete_table_list.size() != 0) {
            TLOG_WARN("unsupport multi table delete");
            return -1;
        }
        if (_delete_stmt->from_table->node_type != parser::NT_TABLE) {
            TLOG_WARN("unsupport multi table delete");
            return -1;
        }
        for (int i = 0; i < _delete_stmt->partition_names.size(); ++i) {
            std::string lower_name = _delete_stmt->partition_names[i].value;
            std::transform(lower_name.begin(), lower_name.end(), lower_name.begin(), ::tolower);
            _partition_names.emplace_back(lower_name);
        }
        if (0 != parse_db_tables(_delete_stmt->from_table, &_join_root)) {
            return -1;
        }
        // delete from xxx; => truncate table xxx;
        if (FLAGS_delete_all_to_truncate &&
            _delete_stmt->where == nullptr &&
            _delete_stmt->limit == nullptr) {
            create_packet_node(proto::OP_TRUNCATE_TABLE);
            if (0 != create_truncate_node()) {
                TLOG_WARN("get truncate_table plan failed");
                return -1;
            }
            if (0 != reset_auto_incr_id()) {
                return -1;
            }
            return 0;
        }

        if (0 != parse_where()) {
            return -1;
        }
        if (0 != parse_orderby()) {
            return -1;
        }
        if (0 != parse_limit()) {
            return -1;
        }
        create_packet_node(proto::OP_DELETE);
        proto::PlanNode *delete_node = _ctx->add_plan_node();
        if (0 != create_delete_node(delete_node)) {
            return -1;
        }
        if (0 != create_sort_node()) {
            return -1;
        }
        if (0 != create_filter_node(_where_filters, proto::WHERE_FILTER_NODE)) {
            return -1;
        }
        create_scan_tuple_descs();
        create_order_by_tuple_desc();
        if (0 != create_scan_nodes()) {
            return -1;
        }
        ScanTupleInfo &info = _plan_table_ctx->table_tuple_mapping[try_to_lower(_current_tables[0])];
        int64_t table_id = info.table_id;
        _ctx->prepared_table_id = table_id;
        set_dml_txn_state(table_id);
        // 局部索引binlog处理标记
        if (_ctx->open_binlog && !_factory->has_global_index(table_id)) {
            delete_node->set_local_index_binlog(true);
        }
        return 0;
    }

    int DeletePlanner::create_delete_node(proto::PlanNode *delete_node) {
        if (_current_tables.size() != 1 ||
            _plan_table_ctx->table_tuple_mapping.count(try_to_lower(_current_tables[0])) == 0) {
            TLOG_WARN("invalid sql format: {}", _ctx->sql.c_str());
            return -1;
        }
        if (_apply_root != nullptr) {
            TLOG_WARN("not support correlation subquery sql format: {}", _ctx->sql.c_str());
            return -1;
        }
        ScanTupleInfo &info = _plan_table_ctx->table_tuple_mapping[try_to_lower(_current_tables[0])];
        int64_t table_id = info.table_id;
        auto table_info_ptr = _factory->get_table_info_ptr(table_id);
        if (table_info_ptr == nullptr) {
            TLOG_WARN("table_id not found: {}", table_id);
            return 0;
        }

        delete_node->set_node_type(proto::DELETE_NODE);
        delete_node->set_limit(-1);
        delete_node->set_is_explain(_ctx->is_explain);
        delete_node->set_num_children(1); //TODO
        proto::DerivePlanNode *derive = delete_node->mutable_derive_node();
        proto::DeleteNode *_delete = derive->mutable_delete_node();
        _delete->set_table_id(table_id);

        auto pk = _factory->get_index_info_ptr(table_id);
        if (pk == nullptr) {
            TLOG_WARN("no pk found with id: {}", table_id);
            return -1;
        }
        for (auto &field: pk->fields) {
            auto &slot = get_scan_ref_slot(_current_tables[0], table_id, field.id, field.type);
            _delete->add_primary_slots()->CopyFrom(slot);
        }
        return 0;
    }

    int DeletePlanner::create_truncate_node() {
        if (_plan_table_ctx->table_tuple_mapping.size() != 1) {
            TLOG_WARN("invalid sql format: {}", _ctx->sql.c_str());
            return -1;
        }
        auto iter = _plan_table_ctx->table_tuple_mapping.begin();

        proto::PlanNode *truncate_node = _ctx->add_plan_node();
        truncate_node->set_node_type(proto::TRUNCATE_NODE);
        truncate_node->set_limit(-1);
        truncate_node->set_num_children(0); //TODO

        proto::DerivePlanNode *derive = truncate_node->mutable_derive_node();
        proto::TruncateNode *_truncate = derive->mutable_truncate_node();
        _truncate->set_table_id(iter->second.table_id);
        return 0;
    }

    int DeletePlanner::reset_auto_incr_id() {
        auto iter = _plan_table_ctx->table_tuple_mapping.begin();
        int64_t table_id = iter->second.table_id;
        auto table_info_ptr = _factory->get_table_info_ptr(table_id);
        if (table_info_ptr == nullptr || table_info_ptr->auto_inc_field_id == -1) {
            return 0;
        }

        proto::MetaManagerRequest request;
        request.set_op_type(proto::OP_UPDATE_FOR_AUTO_INCREMENT);
        auto auto_increment_ptr = request.mutable_auto_increment();
        auto_increment_ptr->set_table_id(table_id);
        auto_increment_ptr->set_force(true);
        auto_increment_ptr->set_start_id(0);

        proto::MetaManagerResponse response;
        if (MetaServerInteract::get_instance()->send_request("meta_manager", request, response) != 0) {
            if (response.errcode() != proto::SUCCESS && _ctx->stat_info.error_code == ER_ERROR_FIRST) {
                _ctx->stat_info.error_code = ER_TABLE_CANT_HANDLE_AUTO_INCREMENT;
                _ctx->stat_info.error_msg.str("reset auto increment failed");
            }
            TLOG_WARN("send_request fail");
            return -1;
        }
        return 0;
    }

    int DeletePlanner::parse_where() {
        if (_delete_stmt->where == nullptr) {
            TLOG_WARN("delete sql [{}] does not contain where conjunct", _ctx->sql.c_str());
            if (FLAGS_open_non_where_sql_forbid) {
                _ctx->stat_info.error_code = ER_SQL_REFUSE;
                _ctx->stat_info.error_msg << "delete sql no where conditions";
                return -1;
            }
            return 0;
        }
        if (0 != flatten_filter(_delete_stmt->where, _where_filters, CreateExprOptions())) {
            TLOG_WARN("flatten_filter failed");
            return -1;
        }
        return 0;
    }

    int DeletePlanner::parse_orderby() {
        if (_delete_stmt != nullptr && _delete_stmt->order != nullptr) {
            TLOG_WARN("delete does not support orderby");
            return -1;
        }
        return 0;
    }

    int DeletePlanner::parse_limit() {
        if (_delete_stmt->limit != nullptr) {
            _ctx->stat_info.error_code = ER_SYNTAX_ERROR;
            _ctx->stat_info.error_msg << "syntax error! delete does not support limit";
            return -1;
        }
        // parser::LimitClause* limit = _delete_stmt->limit;
        // if (limit->offset != nullptr && 0 != create_expr_tree(limit->offset, _limit_offset)) {
        //     TLOG_WARN("create limit offset expr failed");
        //     return -1;
        // }
        // if (limit->count != nullptr && 0 != create_expr_tree(limit->count, _limit_count)) {
        //     TLOG_WARN("create limit offset expr failed");
        //     return -1;
        // }
        return 0;
    }

} //namespace EA
