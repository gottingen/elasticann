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


#include "elasticann/raft/log_entry_reader.h"
#include "elasticann/raft/my_raft_log_storage.h"
#include "elasticann/common/common.h"
#include "elasticann/common/table_key.h"
#include "elasticann/common/mut_table_key.h"
#include "elasticann/proto/store.interface.pb.h"

namespace EA {
    int LogEntryReader::read_log_entry(int64_t region_id, int64_t log_index, std::string &log_entry) {
        MutTableKey log_data_key;
        log_data_key.append_i64(region_id).append_u8(MyRaftLogStorage::LOG_DATA_IDENTIFY).append_i64(log_index);
        std::string log_value;
        rocksdb::ReadOptions options;
        auto status = _rocksdb->get(options, _log_cf, rocksdb::Slice(log_data_key.data()), &log_value);
        if (!status.ok()) {
            TLOG_ERROR("read log entry fail, region_id: {}, log_index: {}", region_id, log_index);
            return -1;
        }
        rocksdb::Slice slice(log_value);
        LogHead head(slice);
        if (head.type != braft::ENTRY_TYPE_DATA) {
            TLOG_ERROR("log entry is not data, log_index:{}, region_id: {}", log_index, region_id);
            return -1;
        }
        slice.remove_prefix(MyRaftLogStorage::LOG_HEAD_SIZE);
        log_entry.assign(slice.data(), slice.size());
        return 0;
    }

    int LogEntryReader::read_log_entry(int64_t region_id, int64_t start_log_index, int64_t end_log_index,
                                       std::set<uint64_t> &txn_ids, std::map<int64_t, std::string> &log_entrys) {
        if (txn_ids.empty()) {
            return 0;
        }
        if (start_log_index > end_log_index) {
            TLOG_ERROR("region_id:{}, start_log_index:{}, end_log_index:{}", region_id, start_log_index,
                     end_log_index);
            return -1;
        }
        TimeCost cost;
        std::string log_entry;
        MutTableKey log_data_key;
        MutTableKey prefix;
        MutTableKey end_key;
        log_data_key.append_i64(region_id).append_u8(MyRaftLogStorage::LOG_DATA_IDENTIFY).append_i64(start_log_index);
        prefix.append_i64(region_id).append_u8(MyRaftLogStorage::LOG_DATA_IDENTIFY);
        end_key.append_i64(region_id).append_u8(MyRaftLogStorage::LOG_DATA_IDENTIFY).append_i64(end_log_index + 1);
        std::string log_value;
        rocksdb::ReadOptions options;
        rocksdb::Slice upper_bound_slice = end_key.data();
        options.iterate_upper_bound = &upper_bound_slice;
        options.prefix_same_as_start = true;
        options.total_order_seek = false;
        options.fill_cache = false;
        std::unique_ptr<rocksdb::Iterator> iter(_rocksdb->new_iterator(options, _log_cf));
        iter->Seek(log_data_key.data());
        for (; iter->Valid(); iter->Next()) {
            if (!iter->key().starts_with(prefix.data())) {
                TLOG_WARN("read end info, region_id: {}, key:{}", region_id, iter->key().ToString(true).c_str());
                break;
            }
            int64_t log_index = TableKey(iter->key()).extract_i64(sizeof(int64_t) + 1);
            if (log_index > end_log_index) {
                TLOG_WARN("region_id:{}, log_index:{}, end_log_index:{}", region_id, log_index, end_log_index);
                break;
            }
            rocksdb::Slice value_slice(iter->value());
            LogHead head(value_slice);
            value_slice.remove_prefix(MyRaftLogStorage::LOG_HEAD_SIZE);
            if (head.type != braft::ENTRY_TYPE_DATA) {
                TLOG_WARN("log entry is not data, region_id: {} head.type: {}", region_id, head.type);
                continue;
            }
            proto::StoreReq store_req;
            if (!store_req.ParseFromArray(value_slice.data(), value_slice.size())) {
                TLOG_ERROR("Fail to parse request fail, region_id: {}", region_id);
                return -1;
            }

            if (!is_txn_op_type(store_req.op_type())) {
                //TLOG_WARN("log entry is not txn, region_id: {} head.type: {}", region_id, store_req.op_type());
                continue;
            }

            if (store_req.txn_infos_size() > 0) {
                uint64_t txn_id = store_req.txn_infos(0).txn_id();
                if (txn_ids.count(txn_id) == 1) {
                    log_entrys[log_index] = value_slice.ToString();
                    TLOG_WARN("read txn log entry region_id:{}, log_index:{}, txn_id:{}", region_id, log_index,
                               txn_id);
                }
            }
        }
        TLOG_WARN("read txn log entry region_id:{}, time_cost:{}", region_id, cost.get_time());
        return 0;
    }

    int LogEntryReader::read_txn_last_log_entry(int64_t region_id, int64_t start_log_index, int64_t end_log_index,
                                                std::set<uint64_t> &txn_ids,
                                                std::map<uint64_t, std::string> &log_entrys) {
        if (txn_ids.empty()) {
            return 0;
        }
        TimeCost cost;
        std::string log_entry;
        MutTableKey log_data_key;
        MutTableKey prefix;
        MutTableKey end_key;
        log_data_key.append_i64(region_id).append_u8(MyRaftLogStorage::LOG_DATA_IDENTIFY).append_i64(start_log_index);
        prefix.append_i64(region_id).append_u8(MyRaftLogStorage::LOG_DATA_IDENTIFY);
        end_key.append_i64(region_id).append_u8(MyRaftLogStorage::LOG_DATA_IDENTIFY).append_i64(end_log_index + 1);
        std::string log_value;
        rocksdb::ReadOptions options;
        rocksdb::Slice upper_bound_slice = end_key.data();
        options.iterate_upper_bound = &upper_bound_slice;
        options.prefix_same_as_start = true;
        options.total_order_seek = false;
        options.fill_cache = false;
        std::map<int64_t, uint64_t> log_index_txn_map;
        std::unique_ptr<rocksdb::Iterator> iter(_rocksdb->new_iterator(options, _log_cf));
        iter->Seek(log_data_key.data());
        for (; iter->Valid(); iter->Next()) {
            if (!iter->key().starts_with(prefix.data())) {
                TLOG_WARN("read end info, region_id: {}, key:{}", region_id, iter->key().ToString(true).c_str());
                break;
            }
            int64_t log_index = TableKey(iter->key()).extract_i64(sizeof(int64_t) + 1);
            if (log_index > end_log_index) {
                TLOG_WARN("region_id:{}, log_index:{}, end_log_index:{}", region_id, log_index, end_log_index);
                break;
            }
            rocksdb::Slice value_slice(iter->value());
            LogHead head(value_slice);
            value_slice.remove_prefix(MyRaftLogStorage::LOG_HEAD_SIZE);
            if (head.type != braft::ENTRY_TYPE_DATA) {
                TLOG_WARN("log entry is not data, region_id: {} head.type: {}", region_id, head.type);
                continue;
            }
            proto::StoreReq store_req;
            if (!store_req.ParseFromArray(value_slice.data(), value_slice.size())) {
                TLOG_ERROR("Fail to parse request fail, region_id: {}", region_id);
                return -1;
            }

            if (!is_txn_op_type(store_req.op_type())) {
                //TLOG_WARN("log entry is not txn, region_id: {} head.type: {}", region_id, store_req.op_type());
                continue;
            }

            if (store_req.txn_infos_size() > 0) {
                uint64_t txn_id = store_req.txn_infos(0).txn_id();
                if (txn_ids.count(txn_id) == 1) {
                    log_entrys[txn_id] = value_slice.ToString();
                    TLOG_WARN("read txn log entry region_id:{}, log_index:{}, txn_id:{}", region_id, log_index,
                               txn_id);
                }
            }
        }
        TLOG_WARN("read txn log entry region_id:{}, time_cost:{}", region_id, cost.get_time());
        return 0;
    }
}


/* vim: set expandtab ts=4 sw=4 sts=4 tw=100: */
