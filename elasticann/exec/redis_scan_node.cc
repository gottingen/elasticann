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


#include <map>
#include "elasticann/exec/redis_scan_node.h"
#include "elasticann/exec/filter_node.h"
#include "elasticann/exec/join_node.h"
#include "elasticann/common/schema_factory.h"
#include "elasticann/expr/scalar_fn_call.h"
#include "elasticann/expr/slot_ref.h"
#include "elasticann/runtime/runtime_state.h"

namespace EA {
    DEFINE_string(redis_addr, "127.0.0.1:6379", "redis addr");

    int RedisScanNode::init(const proto::PlanNode &node) {
        int ret = 0;
        ret = ScanNode::init(node);
        if (ret < 0) {
            TLOG_WARN("ExecNode::init fail, ret:{}", ret);
            return ret;
        }
        brpc::ChannelOptions option;
        option.protocol = brpc::PROTOCOL_REDIS;
        option.max_retry = 1;
        option.connect_timeout_ms = 3000;
        option.timeout_ms = -1;
        if (_redis_channel.Init(FLAGS_redis_addr.c_str(), &option) != 0) {
            TLOG_WARN("fail to connect redis, {}", FLAGS_redis_addr.c_str());
            return -1;
        }
        return 0;
    }

    int RedisScanNode::open(RuntimeState *state) {
        int ret = 0;
        ret = ScanNode::open(state);
        if (ret < 0) {
            TLOG_WARN("{}, ScanNode::open fail:{}", *state, ret);
            return ret;
        }
        // 简单kv只有primary
        // 如果数据源支持各种索引，可以在这边做处理
        proto::PossibleIndex pos_index;
        pos_index.ParseFromString(_pb_node.derive_node().scan_node().indexes(0));
        _index_id = pos_index.index_id();
        SchemaFactory *factory = SchemaFactory::get_instance();
        auto index_info_ptr = factory->get_index_info_ptr(_index_id);
        if (index_info_ptr == nullptr) {
            TLOG_WARN("{}, no index_info found for index id: {}", *state,
                      _index_id);
            return -1;
        }
        if (index_info_ptr->type != proto::I_PRIMARY) {
            TLOG_WARN("{}, index id: {} not primary: {}", *state,
                      _index_id, index_info_ptr->type);
            return -1;
        }
        for (auto &range: pos_index.ranges()) {
            // 空指针容易出错
            SmartRecord left_record = factory->new_record(_table_id);
            SmartRecord right_record = factory->new_record(_table_id);
            left_record->decode(range.left_pb_record());
            right_record->decode(range.right_pb_record());
            int left_field_cnt = range.left_field_cnt();
            int right_field_cnt = range.right_field_cnt();
            //bool left_open = range.left_open();
            //bool right_open = range.right_open();
            if (range.left_pb_record() != range.right_pb_record()) {
                TLOG_WARN("redis only support enqueue");
                return -1;
            }
            if (left_field_cnt != 1 || left_field_cnt != right_field_cnt) {
                TLOG_WARN("redis only support enqueue");
                return -1;
            }
            _primary_records.push_back(left_record);
        }
        return 0;
    }

    int RedisScanNode::get_by_key(SmartRecord record) {
        auto field_key = record->get_field_by_tag(1);
        // record的交互接口只关注ExprValue结构，这是一种混合类型，可以方便转化
        std::string key = record->get_value(field_key).get_string();
        brpc::RedisRequest request;
        brpc::RedisResponse response;
        brpc::Controller cntl;
        request.AddCommand("GET {}", key.c_str());
        _redis_channel.CallMethod(nullptr, &cntl, &request, &response, nullptr);
        if (cntl.Failed()) {
            TLOG_WARN("Fail to access redis-server, {}", cntl.ErrorText().c_str());
            return -1;
        }
        if (response.reply(0).is_nil() || response.reply(0).is_error()) {
            // key not exist
            return -1;
        }
        //TLOG_INFO("type {}", response.reply(0).type());
        ExprValue value(proto::STRING);
        value.str_val = response.reply(0).c_str();
        auto field_value = record->get_field_by_tag(2);
        record->set_value(field_value, value);
        return 0;
    }

    int RedisScanNode::get_next(RuntimeState *state, RowBatch *batch, bool *eos) {
        int ret = 0;
        SmartRecord record;
        while (1) {
            if (reached_limit()) {
                *eos = true;
                return 0;
            }
            if (batch->is_full()) {
                return 0;
            }
            if (_idx >= _primary_records.size()) {
                *eos = true;
                return 0;
            } else {
                record = _primary_records[_idx++];
            }
            ret = get_by_key(record);
            if (ret < 0) {
                TLOG_WARN("record get value fail, {}", record->debug_string().c_str());
                continue;
            }
            std::unique_ptr<MemRow> row = state->mem_row_desc()->fetch_mem_row();
            for (auto slot: _tuple_desc->slots()) {
                auto field = record->get_field_by_tag(slot.field_id());
                row->set_value(slot.tuple_id(), slot.slot_id(),
                               record->get_value(field));
            }
            batch->move_row(std::move(row));
            ++_num_rows_returned;
        }
    }

    void RedisScanNode::close(RuntimeState *state) {
        ScanNode::close(state);
        _idx = 0;
    }

}

