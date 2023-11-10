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


#pragma once

#include "elasticann/expr/expr_node.h"
//#include "sql_parser.h"

namespace EA {
    class Literal : public ExprNode {
    public:
        Literal() : _value(proto::NULL_TYPE) {
            _is_constant = true;
        }

        Literal(ExprValue value) : _value(value) {
            _is_constant = true;
            _has_null = value.is_null();
            value_to_node_type();
        }

        virtual ~Literal() {
        }

        void init(const ExprValue &value) {
            _value = value;
            _is_constant = true;
            _has_null = value.is_null();
            value_to_node_type();
        }

        virtual int init(const proto::ExprNode &node) {
            int ret = 0;
            ret = ExprNode::init(node);
            if (ret < 0) {
                return ret;
            }
            switch (node.node_type()) {
                case proto::NULL_LITERAL: {
                    _value.type = proto::NULL_TYPE;
                    _has_null = true;
                    break;
                }
                case proto::INT_LITERAL: {
                    _value.type = proto::INT64;
                    _value._u.int64_val = node.derive_node().int_val();
                    break;
                }
                case proto::BOOL_LITERAL: {
                    _value.type = proto::BOOL;
                    _value._u.bool_val = node.derive_node().bool_val();
                    break;
                }
                case proto::DOUBLE_LITERAL: {
                    _value.type = proto::DOUBLE;
                    _value._u.double_val = node.derive_node().double_val();
                    break;
                }
                case proto::STRING_LITERAL: {
                    _value.type = proto::STRING;
                    _value.str_val = node.derive_node().string_val();
                    break;
                }
                case proto::HEX_LITERAL: {
                    _value.type = proto::HEX;
                    _value.str_val = node.derive_node().string_val();
                    break;
                }
                case proto::HLL_LITERAL: {
                    _value.type = proto::HLL;
                    _value.str_val = node.derive_node().string_val();
                    break;
                }
                case proto::BITMAP_LITERAL: {
                    _value.type = proto::BITMAP;
                    _value.str_val = node.derive_node().string_val();
                    _value.cast_to(proto::BITMAP);
                    break;
                }
                case proto::TDIGEST_LITERAL: {
                    _value.type = proto::TDIGEST;
                    _value.str_val = node.derive_node().string_val();
                    break;
                }
                case proto::DATETIME_LITERAL: {
                    _value.type = proto::DATETIME;
                    _value._u.uint64_val = node.derive_node().int_val();
                    break;
                }
                case proto::TIME_LITERAL: {
                    _value.type = proto::TIME;
                    _value._u.int32_val = node.derive_node().int_val();
                    break;
                }
                case proto::TIMESTAMP_LITERAL: {
                    _value.type = proto::TIMESTAMP;
                    _value._u.uint32_val = node.derive_node().int_val();
                    break;
                }
                case proto::DATE_LITERAL: {
                    _value.type = proto::DATE;
                    _value._u.uint32_val = node.derive_node().int_val();
                    break;
                }
                case proto::PLACE_HOLDER_LITERAL: {
                    _value.type = proto::NULL_TYPE;
                    _is_place_holder = true;
                    _place_holder_id = node.derive_node().int_val(); // place_holder id
                    break;
                }
                default:
                    return -1;
            }
            return 0;
        }

        int64_t used_size() override {
            return sizeof(*this) + _value.size();
        }

        virtual bool is_place_holder() {
            return _is_place_holder;
        }

        virtual void find_place_holder(std::map<int, ExprNode *> &placeholders) {
            if (_is_place_holder) {
                placeholders.insert({_place_holder_id, this});
            }
        }

        virtual void transfer_pb(proto::ExprNode *pb_node) {
            ExprNode::transfer_pb(pb_node);
            switch (node_type()) {
                case proto::NULL_LITERAL:
                    break;
                case proto::BOOL_LITERAL:
                    pb_node->mutable_derive_node()->set_bool_val(_value.get_numberic<bool>());
                    break;
                case proto::INT_LITERAL:
                    pb_node->mutable_derive_node()->set_int_val(_value.get_numberic<int64_t>());
                    break;
                case proto::DOUBLE_LITERAL:
                    pb_node->mutable_derive_node()->set_double_val(_value.get_numberic<double>());
                    break;
                case proto::STRING_LITERAL:
                case proto::HEX_LITERAL:
                case proto::HLL_LITERAL:
                case proto::BITMAP_LITERAL:
                    pb_node->mutable_derive_node()->set_string_val(_value.get_string());
                    break;
                case proto::DATETIME_LITERAL:
                case proto::DATE_LITERAL:
                case proto::TIME_LITERAL:
                case proto::TIMESTAMP_LITERAL:
                    pb_node->mutable_derive_node()->set_int_val(_value.get_numberic<int64_t>());
                    break;
                case proto::PLACE_HOLDER_LITERAL:
                    pb_node->mutable_derive_node()->set_int_val(_place_holder_id);
                    TLOG_ERROR("place holder need not transfer pb, {}", _place_holder_id);
                    break;
                default:
                    break;
            }
        }

        // only the following castings are allowed:
        //  STRING_LITERA => TIMESTAMP_LITERAL
        //  STRING_LITERA => DATETIME_LITERAL
        //  STRING_LITERA => DATE_LITERAL
        void cast_to_type(proto::ExprNodeType literal_type) {
            if (literal_type == proto::TIMESTAMP_LITERAL) {
                _value.cast_to(proto::TIMESTAMP);
            } else if (literal_type == proto::DATE_LITERAL) {
                _value.cast_to(proto::DATE);
            } else if (literal_type == proto::DATETIME_LITERAL) {
                _value.cast_to(proto::DATETIME);
            } else if (literal_type == proto::TIME_LITERAL) {
                _value.cast_to(proto::TIME);
            }
            _node_type = literal_type;
            _col_type = _value.type;
        }

        void cast_to_col_type(proto::PrimitiveType type) {
            if (is_datetime_specic(type) && _value.is_numberic()) {
                _value.cast_to(proto::STRING);
            }
            _value.cast_to(type);
            value_to_node_type();
        }

        virtual ExprValue get_value(MemRow *row) {
            return _value.cast_to(_col_type);
        }

        virtual ExprValue get_value(const ExprValue &value) {
            return _value.cast_to(_col_type);
        }

    private:
        void value_to_node_type() {
            _col_type = _value.type;
            if (_value.is_timestamp()) {
                _node_type = proto::TIMESTAMP_LITERAL;
            } else if (_value.is_date()) {
                _node_type = proto::DATE_LITERAL;
            } else if (_value.is_datetime()) {
                _node_type = proto::DATETIME_LITERAL;
            } else if (_value.is_time()) {
                _node_type = proto::TIME_LITERAL;
            } else if (_value.is_int()) {
                _node_type = proto::INT_LITERAL;
            } else if (_value.is_string()) {
                _node_type = proto::STRING_LITERAL;
            } else if (_value.is_bool()) {
                _node_type = proto::BOOL_LITERAL;
            } else if (_value.is_double()) {
                _node_type = proto::DOUBLE_LITERAL;
            } else if (_value.is_hll()) {
                _node_type = proto::HLL_LITERAL;
            } else if (_value.is_bitmap()) {
                _node_type = proto::BITMAP_LITERAL;
            } else {
                _node_type = proto::NULL_LITERAL;
            }
        }

    private:
        ExprValue _value;
        int _place_holder_id = 0;
        bool _is_place_holder = false;
    };
}

