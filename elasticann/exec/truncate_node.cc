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


#include "elasticann/runtime/runtime_state.h"
#include "elasticann/exec/truncate_node.h"

namespace EA {
    int TruncateNode::init(const proto::PlanNode &node) {
        int ret = 0;
        ret = ExecNode::init(node);
        if (ret < 0) {
            TLOG_WARN("ExecNode::init fail, ret:{}", ret);
            return ret;
        }
        _table_id = node.derive_node().truncate_node().table_id();
        _table_info = SchemaFactory::get_instance()->get_table_info_ptr(_table_id);

        if (_table_info == nullptr) {
            TLOG_WARN("table info not found _table_id:{}", _table_id);
            return -1;
        }
        return 0;
    }

    int TruncateNode::open(RuntimeState *state) {
        int ret = 0;
        ret = ExecNode::open(state);
        if (ret < 0) {
            TLOG_WARN("{}, ExecNode::open fail:{}", *state, ret);
            return ret;
        }
        RocksWrapper *_db = RocksWrapper::get_instance();
        if (_db == nullptr) {
            TLOG_WARN("{}, get rocksdb instance failed", *state);
            return -1;
        }
        rocksdb::ColumnFamilyHandle *_data_cf = _db->get_data_handle();
        if (_data_cf == nullptr) {
            TLOG_WARN("{}, get rocksdb data column family failed", *state);
            return -1;
        }
        _region_id = state->region_id();

        MutTableKey region_start;
        MutTableKey region_end;
        region_start.append_i64(_region_id);
        region_end.append_i64(_region_id).append_u64(UINT64_MAX);

        rocksdb::WriteOptions write_options;
        //write_options.disableWAL = true;

        rocksdb::Slice begin(region_start.data());
        rocksdb::Slice end(region_end.data());
        TimeCost cost;
        auto res = _db->remove_range(write_options, _data_cf, begin, end, true);
        if (!res.ok()) {
            TLOG_WARN("{}, truncate table failed: table:{}, region:{}, code={}, msg={}",*state,
                             _table_id, _region_id, res.code(), res.ToString().c_str());
            return -1;
        }
        TLOG_WARN("{}, truncate table:{}, region:{}, cost:{}",*state,
                         _table_id, _region_id, cost.get_time());
        /*
        res = _db->compact_range(rocksdb::CompactRangeOptions(), _data_cf, &begin, &end);
        if (!res.ok()) {
            TLOG_WARN("{}, compact after truncated failed: table:{}, region:{}, code={}, msg={}", *state,
                _table_id, _region_id, res.code(), res.ToString().c_str());
            return -1;
        }
        */
        return 0;
    }

    void TruncateNode::transfer_pb(int64_t region_id, proto::PlanNode *pb_node) {
        ExecNode::transfer_pb(region_id, pb_node);
        auto truncate_node = pb_node->mutable_derive_node()->mutable_truncate_node();
        truncate_node->set_table_id(_table_id);
    }

}
