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


#include "elasticann/exec/exec_node.h"
#include "elasticann/exec/agg_node.h"
#include "elasticann/exec/filter_node.h"
#include "elasticann/exec/insert_node.h"
#include "elasticann/exec/update_node.h"
#include "elasticann/exec/delete_node.h"
#include "elasticann/exec/join_node.h"
#include "elasticann/exec/scan_node.h"
#include "elasticann/exec/dual_scan_node.h"
#include "elasticann/exec/rocksdb_scan_node.h"
#include "elasticann/exec/sort_node.h"
#include "elasticann/exec/packet_node.h"
#include "elasticann/exec/limit_node.h"
#include "elasticann/exec/truncate_node.h"
#include "elasticann/exec/kill_node.h"
#include "elasticann/exec/transaction_node.h"
#include "elasticann/exec/begin_manager_node.h"
#include "elasticann/exec/commit_manager_node.h"
#include "elasticann/exec/rollback_manager_node.h"
#include "elasticann/exec/lock_primary_node.h"
#include "elasticann/exec/lock_secondary_node.h"
#include "elasticann/exec/full_export_node.h"
#include "elasticann/exec/union_node.h"
#include "elasticann/exec/apply_node.h"
#include "elasticann/exec/load_node.h"
#include "elasticann/runtime/runtime_state.h"

namespace EA {
    int ExecNode::init(const proto::PlanNode &node) {
        _pb_node = node;
        _limit = node.limit();
        _num_rows_returned = 0;
        _node_type = node.node_type();
        _is_explain = node.is_explain();
        return 0;
    }

    int ExecNode::expr_optimize(QueryContext *ctx) {
        int ret = 0;
        for (auto c: _children) {
            int ret2 = c->expr_optimize(ctx);
            if (ret2 < 0) {
                ret = ret2;
            }
        }
        return ret;
    }

    int ExecNode::common_expr_optimize(std::vector<ExprNode *> *exprs) {
        int ret = 0;
        for (auto &expr: *exprs) {
            //类型推导
            ret = expr->expr_optimize();
            if (ret < 0) {
                TLOG_WARN("type_inferer fail");
                return ret;
            }
            if (expr->is_row_expr()) {
                continue;
            }
            if (!expr->is_constant()) {
                continue;
            }
            if (expr->is_literal()) {
                continue;
            }
            // place holder被替换会导致下一次exec参数对不上
            // TODO 后续得考虑普通查询计划复用，表达式如何对上
            if (expr->has_place_holder()) {
                continue;
            }
            ret = expr->open();
            if (ret < 0) {
                return ret;
            }
            ExprValue value = expr->get_value(nullptr);
            expr->close();
            delete expr;
            expr = new Literal(value);
        }
        return ret;
    }

    int ExecNode::predicate_pushdown(std::vector<ExprNode *> &input_exprs) {
        if (_children.size() > 0) {
            _children[0]->predicate_pushdown(input_exprs);
        }
        if (input_exprs.size() > 0) {
            add_filter_node(input_exprs);
        }
        input_exprs.clear();
        return 0;
    }

    void ExecNode::remove_additional_predicate(std::vector<ExprNode *> &input_exprs) {
        for (auto c: _children) {
            c->remove_additional_predicate(input_exprs);
        }
    }

    void ExecNode::add_filter_node(const std::vector<ExprNode *> &input_exprs) {
        proto::PlanNode pb_plan_node;
        pb_plan_node.set_node_type(proto::TABLE_FILTER_NODE);
        pb_plan_node.set_num_children(1);
        pb_plan_node.set_is_explain(_is_explain);
        pb_plan_node.set_limit(-1);
        auto filter_node = new FilterNode();
        filter_node->init(pb_plan_node);
        _parent->replace_child(this, filter_node);
        filter_node->add_child(this);
        for (auto &expr: input_exprs) {
            filter_node->add_conjunct(expr);
        }
    }

    void ExecNode::get_node(const proto::PlanNodeType node_type, std::vector<ExecNode *> &exec_nodes) {
        if (_node_type == node_type) {
            exec_nodes.emplace_back(this);
        }
        for (auto c: _children) {
            c->get_node(node_type, exec_nodes);
        }
    }

    void ExecNode::join_get_scan_nodes(const proto::PlanNodeType node_type, std::vector<ExecNode *> &exec_nodes) {
        if (_node_type == node_type) {
            exec_nodes.emplace_back(this);
        }
        for (auto c: _children) {
            if (c->node_type() == proto::JOIN_NODE || c->node_type() == proto::APPLY_NODE) {
                continue;
            }
            c->join_get_scan_nodes(node_type, exec_nodes);
        }
    }

    ExecNode *ExecNode::get_node(const proto::PlanNodeType node_type) {
        if (_node_type == node_type) {
            return this;
        } else {
            for (auto c: _children) {
                ExecNode *node = c->get_node(node_type);
                if (node != nullptr) {
                    return node;
                }
            }
            return nullptr;
        }
    }

    bool ExecNode::need_seperate() {
        switch (_node_type) {
            case proto::INSERT_NODE:
            case proto::UPDATE_NODE:
            case proto::DELETE_NODE:
            case proto::TRUNCATE_NODE:
            case proto::KILL_NODE:
            case proto::TRANSACTION_NODE:
            case proto::BEGIN_MANAGER_NODE:
            case proto::COMMIT_MANAGER_NODE:
            case proto::UNION_NODE:
            case proto::ROLLBACK_MANAGER_NODE:
                return true;
            case proto::SCAN_NODE:
                //TLOG_INFO("engine:{}", static_cast<ScanNode*>(this)->engine());
                if (static_cast<ScanNode *>(this)->engine() == proto::ROCKSDB) {
                    return true;
                }
                if (static_cast<ScanNode *>(this)->engine() == proto::BINLOG) {
                    return true;
                }
                if (static_cast<ScanNode *>(this)->engine() == proto::ROCKSDB_CSTORE) {
                    return true;
                }
                break;
            default:
                break;
        }
        for (auto c: _children) {
            if (c->need_seperate()) {
                return true;
            }
        }
        return false;
    }

    void ExecNode::create_trace() {
        if (_trace != nullptr) {
            for (auto c: _children) {
                if (c->get_trace() == nullptr) {
                    proto::TraceNode *trace_node = _trace->add_child_nodes();
                    trace_node->set_node_type(c->node_type());
                    c->set_trace(trace_node);
                }
                c->create_trace();
            }
        }
    }

    int ExecNode::open(RuntimeState *state) {
        int num_affected_rows = 0;
        for (auto c: _children) {
            int ret = 0;
            ret = c->open(state);
            if (ret < 0) {
                return ret;
            }
            num_affected_rows += ret;
        }
        return num_affected_rows;
    }

    void ExecNode::transfer_pb(int64_t region_id, proto::PlanNode *pb_node) {
        _pb_node.set_node_type(_node_type);
        _pb_node.set_limit(_limit);
        _pb_node.set_num_children(_children.size());
        pb_node->CopyFrom(_pb_node);
    }

    void ExecNode::create_pb_plan(int64_t region_id, proto::Plan *plan, ExecNode *root) {
        proto::PlanNode *pb_node = plan->add_nodes();
        root->transfer_pb(region_id, pb_node);
        for (size_t i = 0; i < root->children_size(); i++) {
            create_pb_plan(region_id, plan, root->children(i));
        }
    }

    int ExecNode::create_tree(const proto::Plan &plan, ExecNode **root) {
        int ret = 0;
        int idx = 0;
        if (plan.nodes_size() == 0) {
            *root = nullptr;
            return 0;
        }
        ret = ExecNode::create_tree(plan, &idx, nullptr, root);
        if (ret < 0) {
            return -1;
        }
        return 0;
    }

    int ExecNode::create_tree(const proto::Plan &plan, int *idx, ExecNode *parent,
                              ExecNode **root) {
        if (*idx >= plan.nodes_size()) {
            TLOG_ERROR("idx {} >= size {}", *idx, plan.nodes_size());
            return -1;
        }
        int num_children = plan.nodes(*idx).num_children();
        ExecNode *exec_node = nullptr;

        int ret = 0;
        ret = create_exec_node(plan.nodes(*idx), &exec_node);
        if (ret < 0) {
            TLOG_ERROR("create_exec_node fail:{}", plan.nodes(*idx).DebugString().c_str());
            return ret;
        }

        if (parent != nullptr) {
            parent->add_child(exec_node);
        } else if (root != nullptr) {
            *root = exec_node;
        } else {
            TLOG_ERROR("parent is null");
            return -1;
        }
        for (int i = 0; i < num_children; i++) {
            ++(*idx);
            ret = create_tree(plan, idx, exec_node, nullptr);
            if (ret < 0) {
                TLOG_ERROR("sub create_tree fail, idx:{}", *idx);
                return -1;
            }
        }
        return 0;
    }

    int ExecNode::create_exec_node(const proto::PlanNode &node, ExecNode **exec_node) {
        switch (node.node_type()) {
            case proto::SCAN_NODE:
                *exec_node = ScanNode::create_scan_node(node);
                if (*exec_node == nullptr) {
                    return -1;
                }
                return (*exec_node)->init(node);
            case proto::SORT_NODE:
                *exec_node = new SortNode;
                return (*exec_node)->init(node);
            case proto::AGG_NODE:
            case proto::MERGE_AGG_NODE:
                *exec_node = new AggNode;
                return (*exec_node)->init(node);
            case proto::TABLE_FILTER_NODE:
            case proto::WHERE_FILTER_NODE:
            case proto::HAVING_FILTER_NODE:
                *exec_node = new FilterNode;
                return (*exec_node)->init(node);
            case proto::UPDATE_NODE:
                *exec_node = new UpdateNode;
                return (*exec_node)->init(node);
            case proto::INSERT_NODE:
                *exec_node = new InsertNode;
                return (*exec_node)->init(node);
            case proto::DELETE_NODE:
                *exec_node = new DeleteNode;
                return (*exec_node)->init(node);
            case proto::PACKET_NODE:
                *exec_node = new PacketNode;
                return (*exec_node)->init(node);
            case proto::LIMIT_NODE:
                *exec_node = new LimitNode;
                return (*exec_node)->init(node);
            case proto::TRUNCATE_NODE:
                *exec_node = new TruncateNode;
                return (*exec_node)->init(node);
            case proto::KILL_NODE:
                *exec_node = new KillNode;
                return (*exec_node)->init(node);
            case proto::TRANSACTION_NODE:
                *exec_node = new TransactionNode;
                return (*exec_node)->init(node);
            case proto::BEGIN_MANAGER_NODE:
                *exec_node = new BeginManagerNode;
                return (*exec_node)->init(node);
            case proto::COMMIT_MANAGER_NODE:
                *exec_node = new CommitManagerNode;
                return (*exec_node)->init(node);
            case proto::ROLLBACK_MANAGER_NODE:
                *exec_node = new RollbackManagerNode;
                return (*exec_node)->init(node);
            case proto::JOIN_NODE:
                *exec_node = new JoinNode;
                return (*exec_node)->init(node);
            case proto::LOCK_PRIMARY_NODE:
                *exec_node = new LockPrimaryNode;
                return (*exec_node)->init(node);
            case proto::LOCK_SECONDARY_NODE:
                *exec_node = new LockSecondaryNode;
                return (*exec_node)->init(node);
            case proto::FULL_EXPORT_NODE:
                *exec_node = new FullExportNode;
                return (*exec_node)->init(node);
            case proto::DUAL_SCAN_NODE:
                *exec_node = new DualScanNode;
                return (*exec_node)->init(node);
            case proto::UNION_NODE:
                *exec_node = new UnionNode;
                return (*exec_node)->init(node);
            case proto::APPLY_NODE:
                *exec_node = new ApplyNode;
                return (*exec_node)->init(node);
            case proto::LOAD_NODE:
                *exec_node = new LoadNode;
                return (*exec_node)->init(node);
            default:
                TLOG_ERROR("create_exec_node failed: {}", node.DebugString().c_str());
                return -1;
        }
        return -1;
    }

    //>0代表放到cache里，==0代表不需要放到cache里
    int ExecNode::push_cmd_to_cache(RuntimeState *state,
                                    proto::OpType op_type,
                                    ExecNode *store_request,
                                    int seq_id) {
        TLOG_DEBUG("txn_id: {} op_type: {}, seq_id: {}, exec_node:{}",
                 state->txn_id, proto::OpType_Name(op_type).c_str(), seq_id, turbo::Ptr(store_request));
        if (state->txn_id == 0) {
            return 0;
        }
        auto client = state->client_conn();
        // cache dml cmd in baikaldb before sending to store
        if (op_type != proto::OP_INSERT
            && op_type != proto::OP_DELETE
            && op_type != proto::OP_UPDATE
            && op_type != proto::OP_BEGIN) {
            return 0;
        }
        if (client->cache_plans.count(seq_id)) {
            TLOG_WARN("seq_id duplicate seq_id:{}", seq_id);
        }
        CachePlan &plan_item = client->cache_plans[seq_id];
        plan_item.op_type = op_type;
        plan_item.sql_id = seq_id;
        plan_item.root = store_request;
        store_request->set_parent(nullptr);
        plan_item.tuple_descs = state->tuple_descs();
        return 1;
    }

    int ExecNode::push_cmd_to_cache(RuntimeState *state,
                                    proto::OpType op_type,
                                    ExecNode *store_request) {
        return push_cmd_to_cache(state, op_type, store_request,
                                 state->client_conn()->seq_id);
    }

}
