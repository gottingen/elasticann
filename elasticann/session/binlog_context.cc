// Copyright (c) 2020-present Baidu, Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "elasticann/session/binlog_context.h"
#include "elasticann/common/meta_server_interact.h"

namespace EA {

int64_t TsoFetcher::get_tso() {
    proto::TsoRequest request;
    request.set_op_type(proto::OP_GEN_TSO);
    request.set_count(1);
    proto::TsoResponse response;
    int retry_time = 0;
    int ret = 0;
    for (;;) {
        retry_time++;
        ret = MetaServerInteract::get_tso_instance()->send_request("tso_service", request, response);
        if (ret < 0) {
            if (response.errcode() == proto::RETRY_LATER && retry_time < 5) {
                bthread_usleep(tso::update_timestamp_interval_ms * 1000LL);
                continue;  
            } else {
                TLOG_ERROR("get tso failed, response:{}", response.ShortDebugString().c_str());
                return ret;
            }
        }
        break;
    }
    //TLOG_WARN("response:{}", response.ShortDebugString().c_str());
    auto&  tso = response.start_timestamp();
    int64_t timestamp = (tso.physical() << tso::logical_bits) + tso.logical();

    return timestamp;
}

int BinlogContext::get_binlog_regions(uint64_t log_id) {
    if (_table_info == nullptr || _partition_record == nullptr) {
        TLOG_WARN("wrong state log_id:{}", log_id);
        return -1;
    }
    if (!_table_info->is_linked) {
        TLOG_WARN("table {} not link to binlog table log_id:{}", _table_info->id, log_id);
        return -1;      
    }
    int ret = 0;
    if (_table_info->link_field.empty()) {
        ret = _factory->get_binlog_regions(_table_info->id, _binlog_region);
    } else {
        FieldInfo& link_filed = _table_info->link_field[0];
        auto field_desc = _partition_record->get_field_by_idx(link_filed.pb_idx);
        ExprValue value = _partition_record->get_value(field_desc);
        _partition_key = value.get_numberic<uint64_t>();
        ret = _factory->get_binlog_regions(_table_info->id, _binlog_region, value);
    }

    if (ret < 0) {
        TLOG_WARN("get binlog region failed table_id: {} log_id:{}",
            _table_info->id, log_id);
        return -1;
    }
    return 0;
}

} // namespace EA
