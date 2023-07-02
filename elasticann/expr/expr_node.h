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

#include <vector>
#include <unordered_set>
#include <unordered_map>
#include "elasticann/common/expr_value.h"
#include "elasticann/mem_row/mem_row.h"
#include "elasticann/proto/expr.pb.h"

namespace EA {
const int NOT_BOOL_ERRCODE = -100;
class ExprNode {
public:
    ExprNode() {}
    virtual ~ExprNode() {
        for (auto& e : _children) {
            SAFE_DELETE(e);
        }
    }

    virtual int init(const proto::ExprNode& node) {
        _node_type = node.node_type();
        _col_type = node.col_type();
        _col_flag = node.col_flag();
        if (node.has_charset()) {
            _charset = node.charset();
        }
        return 0;
    }
    virtual void children_swap() {}

    bool is_literal() {
        switch (_node_type) {
            case proto::NULL_LITERAL:
            case proto::BOOL_LITERAL:
            case proto::INT_LITERAL:
            case proto::DOUBLE_LITERAL:
            case proto::STRING_LITERAL:
            case proto::HLL_LITERAL:
            case proto::BITMAP_LITERAL:
            case proto::DATE_LITERAL:
            case proto::DATETIME_LITERAL:
            case proto::TIME_LITERAL:
            case proto::TIMESTAMP_LITERAL:
            case proto::PLACE_HOLDER_LITERAL:
                return true;
            default:
                return false;
        }
        return false;
    }
    bool has_place_holder() {
        if (is_place_holder()) {
            return true;
        }
        for (auto c : _children) {
            if (c->has_place_holder()) {
                return true;
            }
        }
        return false;
    }
    bool has_agg() {
        if (_node_type == proto::AGG_EXPR) {
            return true;
        }
        for (auto c : _children) {
            if (c->has_agg()) {
                return true;
            }
        }
        return false;
    }
    virtual bool is_place_holder() {
        return false;
    }
    bool is_slot_ref() {
        return _node_type == proto::SLOT_REF;
    }
    bool is_constant() const {
        return _is_constant;
    }
    bool has_null() const {
        return _has_null;
    }
    virtual ExprNode* get_last_insert_id() {
        for (auto c : _children) {
            if (c->get_last_insert_id() != nullptr) {
                return c;
            }
        }
        return nullptr;
    }
    bool is_row_expr() {
        return _node_type == proto::ROW_EXPR;
    }
    bool is_function_eq();
    bool is_children_all_eq();
    bool is_children_all_and();

    int expr_optimize() {
        const_pre_calc();
        return type_inferer();
    }
    //类型推导，只在baikal执行
    virtual int type_inferer() {
        for (auto c : _children) {
            int ret = 0;
            ret = c->type_inferer();
            if (ret < 0) {
                return ret;
            }
        }
        return 0;
    }
    //常量表达式预计算,eg. id * 2 + 2 * 4 => id * 2 + 8
    //TODO 考虑做各种左右变化,eg. id + 2 - 4 => id - 2; id * 2 + 4 > 4 / 2 => id > -1
    void const_pre_calc();

    // optimize or node to in node
    static void  or_node_optimize(ExprNode** expr_node);
    bool has_same_children();
    bool is_vaild_or_optimize_tree(int32_t level, std::unordered_set<int32_t>* tuple_set);
    static int change_or_node_to_in(ExprNode** expr_node);
    int serialize_tree(uint64_t& serialize_slot_id);
    void set_is_constant(bool flag) {
        _is_constant =  flag;
    }

    //参数校验，创建些运行时资源，比如in的map
    virtual int open() {
        for (auto e : _children) {
            int ret = 0;
            ret = e->open();
            if (ret < 0) {
                return ret;
            }
        }
        return 0;
    } 
    virtual ExprValue get_value(MemRow* row) { //对每行计算表达式
        return ExprValue::Null();
    }
    virtual ExprValue get_value(const ExprValue& value) {
        return ExprValue::Null();
    }
    //释放open创建的资源
    virtual void close() {
        for (auto e : _children) {
            e->close();
        }
    }

    virtual int64_t used_size() {
        int64_t size = sizeof(*this);
        for (auto c : _children) {
            size += c->used_size();
        }
        return size;
    }

    virtual void find_place_holder(std::map<int, ExprNode*>& placeholders) {
        for (size_t idx = 0; idx < _children.size(); ++idx) {
            _children[idx]->find_place_holder(placeholders);
        }
    }

    virtual void replace_slot_ref_to_literal(const std::set<int64_t>& sign_set,
                    std::map<int64_t, std::vector<ExprNode*>>& literal_maps);

    ExprNode* get_slot_ref(int32_t tuple_id, int32_t slot_id);
    ExprNode* get_parent(ExprNode* child);
    void add_child(ExprNode* expr_node) {
        _children.push_back(expr_node);
    }
    bool contains_null_function();
    bool contains_special_operator(proto::ExprNodeType expr_node_type) {
        bool contain = false;
        recursive_contains_special_operator(expr_node_type, &contain);
        return contain;
    }
    void recursive_contains_special_operator(proto::ExprNodeType expr_node_type, bool* contain) {
        if (_node_type == expr_node_type) {
            *contain = true;
            return;
        }
        for (auto child : _children) {
            child->recursive_contains_special_operator(expr_node_type, contain);
        }
    }

    void replace_child(size_t idx, ExprNode* expr) {
        delete _children[idx];
        _children[idx] = expr;
    }

    void del_child(size_t idx) {
        _children.erase(_children.begin() + idx);
    }
    // 与儿子断开连接
    void clear_children() {
        _children.clear();
    }
    size_t children_size() {
        return _children.size();
    }
    ExprNode* children(size_t idx) {
        return _children[idx];
    }
    ExprNode** mutable_children(size_t idx) {
        return &_children[idx];
    }
    proto::ExprNodeType node_type() {
        return _node_type;
    }
    proto::PrimitiveType col_type() {
        return _col_type;
    }
    void set_col_type(proto::PrimitiveType col_type) {
        _col_type = col_type;
    }
    uint32_t col_flag() {
        return _col_flag;
    }

    void set_charset(proto::Charset charset) {
        _charset = charset;
    }
    proto::Charset charset() {
        return _charset;
    }
    void set_col_flag(uint32_t col_flag) {
        _col_flag = col_flag;
    }

    void flatten_or_expr(std::vector<ExprNode*>* or_exprs) {
        if (node_type() != proto::OR_PREDICATE) {
            or_exprs->push_back(this);
            return;
        }
        for (auto c : _children) {
            c->flatten_or_expr(or_exprs);
        }
    }

    virtual void transfer_pb(proto::ExprNode* pb_node);
    static void create_pb_expr(proto::Expr* expr, ExprNode* root);
    static int create_tree(const proto::Expr& expr, ExprNode** root);
    static void destroy_tree(ExprNode* root) {
        delete root;
    }
    static void get_pb_expr(const proto::Expr& from, int* idx, proto::Expr* to);

    void get_all_tuple_ids(std::unordered_set<int32_t>& tuple_ids);
    void get_all_slot_ids(std::unordered_set<int32_t>& slot_ids);
    void get_all_field_ids(std::unordered_set<int32_t>& field_ids);
    int32_t tuple_id() const {
        return _tuple_id;
    }

    void set_slot_col_type(int32_t tuple_id, int32_t slot_id, proto::PrimitiveType col_type);

    proto::PrimitiveType get_slot_col_type(int32_t slot_id);

    int32_t slot_id() const {
        return _slot_id;
    }

    void disable_replace_agg_to_slot() {
        _replace_agg_to_slot = false;
    }

    void print_expr_info();
    static bvar::Adder<int64_t>  _s_non_boolean_sql_cnts;
protected:
    proto::ExprNodeType _node_type;
    proto::PrimitiveType _col_type = proto::INVALID_TYPE;
    std::vector<ExprNode*> _children;
    uint32_t _col_flag = 0;
    proto::Charset _charset = proto::CS_UNKNOWN;
    bool    _is_constant = true;
    bool    _has_null = false;
    bool    _replace_agg_to_slot = true;
    int32_t _tuple_id = -1;
    int32_t _slot_id = -1;
    bool is_logical_and_or_not();
public:
    static int create_expr_node(const proto::ExprNode& node, ExprNode** expr_node);
private:
    static int create_tree(const proto::Expr& expr, int* idx, ExprNode* parent, ExprNode** root);
};
}

