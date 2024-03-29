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


#include "elasticann/exec/dml_manager_node.h"
#include "elasticann/exec/insert_manager_node.h"
#include "elasticann/session/network_socket.h"

namespace EA {
    int DmlManagerNode::open(RuntimeState *state) {
        int ret = 0;
        auto client_conn = state->client_conn();
        if (client_conn == nullptr) {
            TLOG_WARN("connection is nullptr: {}", state->txn_id);
            return -1;
        }
        client_conn->seq_id++;
        if (_return_empty) {
            return 0;
        }
        ExecNode *dml_node = _children[0];
        ret = _fetcher_store.run(state, _region_infos, dml_node, client_conn->seq_id, client_conn->seq_id, _op_type);
        if (ret < 0) {
            TLOG_WARN("fetcher store fail, txn_id: {} seq_id: {} need_rollback_seq[{}]",
                      state->txn_id, client_conn->seq_id, client_conn->seq_id);
            client_conn->need_rollback_seq.insert(client_conn->seq_id);
            return -1;
        }
        ret = push_cmd_to_cache(state, _op_type, dml_node);
        if (ret > 0) {
            _children.clear();
        }
        return _fetcher_store.affected_rows.load();
    }

    int DmlManagerNode::get_region_infos(RuntimeState *state,
                                         DMLNode *dml_node,
                                         const std::vector<SmartRecord> &insert_scan_records,
                                         const std::vector<SmartRecord> &delete_scan_records,
                                         std::map<int64_t, proto::RegionInfo> &region_infos) {
        int64_t global_index_id = dml_node->global_index_id();
        auto index_info_ptr = SchemaFactory::get_instance()->get_index_info_ptr(global_index_id);
        if (index_info_ptr == nullptr) {
            TLOG_WARN("invalid index info: {}", global_index_id);
            return -1;
        }
        if (index_info_ptr->index_hint_status == proto::IHS_VIRTUAL) {
            TLOG_WARN("index info: {} is virtual, skip", global_index_id);
            return 0;
        }
        int ret = _factory->get_region_by_key(*index_info_ptr,
                                              insert_scan_records,
                                              delete_scan_records,
                                              dml_node->insert_records_by_region(),
                                              dml_node->delete_records_by_region(),
                                              region_infos);
        if (ret < 0) {
            TLOG_WARN("get_region_by_key fail : {}", ret);
            return ret;
        }
        if (region_infos.size() == 0) {
            TLOG_WARN("region_infos.size = 0");
            return -1;
        }
        return 0;
    }

    int DmlManagerNode::send_request(RuntimeState *state,
                                     DMLNode *dml_node,
                                     const std::vector<SmartRecord> &insert_scan_records,
                                     const std::vector<SmartRecord> &delete_scan_records) {
        auto client_conn = state->client_conn();
        if (client_conn == nullptr) {
            TLOG_WARN("connection is nullptr: {}", state->txn_id);
            return -1;
        }
        int ret = 0;
        ret = get_region_infos(state, dml_node, insert_scan_records, delete_scan_records, _region_infos);
        if (ret < 0) {
            TLOG_WARN("get region info fail : {}", ret);
            return ret;
        }
        //每条dml语句都需要单独占一个seq_id
        client_conn->seq_id++;
        //对每个索引表进行lock_delete操作
        ret = _fetcher_store.run(state, _region_infos, dml_node, client_conn->seq_id, client_conn->seq_id, _op_type);
        if (ret < 0) {
            std::string seq_id_str = "[";
            for (auto seq_id: _seq_ids) {
                client_conn->need_rollback_seq.insert(seq_id);
                seq_id_str += std::to_string(seq_id) + ",";
            }
            client_conn->need_rollback_seq.insert(client_conn->seq_id);
            seq_id_str += std::to_string(client_conn->seq_id);
            seq_id_str += "]";
            TLOG_WARN("fetcher store fail, txn_id: {} log_id:{} seq_id: {} need_rollback_seq:{}", state->txn_id,
                      state->log_id(), client_conn->seq_id, seq_id_str.c_str());
            return -1;
        }
        push_cmd_to_cache(state, _op_type, dml_node);
        _seq_ids.push_back(client_conn->seq_id);
        _region_infos.clear();
        return _fetcher_store.affected_rows.load();
    }

    int DmlManagerNode::send_request_light(RuntimeState *state,
                                           DMLNode *dml_node,
                                           FetcherStore &fetcher_store,
                                           int seq_id,
                                           const std::vector<SmartRecord> &insert_scan_records,
                                           const std::vector<SmartRecord> &delete_scan_records) {
        int ret = 0;
        std::map<int64_t, proto::RegionInfo> region_infos;
        ret = get_region_infos(state, dml_node, insert_scan_records, delete_scan_records, region_infos);
        if (ret < 0) {
            TLOG_WARN("get region info fail : {}", ret);
            return ret;
        }
        //对每个索引表进行lock_delete操作
        ret = fetcher_store.run(state, region_infos, dml_node, seq_id, seq_id, _op_type);
        if (ret < 0) {
            TLOG_WARN("fetcher store fail, txn_id: {} seq_id: {}", state->txn_id, seq_id);
            return -1;
        }
        return fetcher_store.affected_rows.load();
    }

    int DmlManagerNode::send_request_concurrency(RuntimeState *state, size_t start_child) {
        auto client_conn = state->client_conn();
        if (client_conn == nullptr) {
            TLOG_WARN("connection is nullptr: {}", state->txn_id);
            return -1;
        }
        int error = 0;
        int prev_seq_id = client_conn->seq_id;
        ConcurrencyBthread send_bth(_affected_index_num + 1, &BTHREAD_ATTR_SMALL);
        auto it = _children.begin() + start_child;
        std::map<int, ExecNode *> cache_map;
        while (it != _children.end()) {
            DMLNode *exec_node = static_cast<DMLNode *>(*it);
            client_conn->seq_id++;
            int start_seq_id = client_conn->seq_id;
            cache_map[start_seq_id] = exec_node;
            auto send_func = [this, state, exec_node, start_seq_id, &error]() {
                int ret = 0;
                FetcherStore fetcher_store;
                ret = send_request_light(state, exec_node, fetcher_store, start_seq_id,
                                         _insert_scan_records, _del_scan_records);
                if (ret < 0) {
                    TLOG_WARN("exec node failed, log_id:{} index_id:{} ret:{} ",
                              state->log_id(), exec_node->global_index_id(), ret);
                    error = ret;
                }
            };
            send_bth.run(send_func);
            ++it;
        }
        send_bth.join();
        if (error >= 0) {
            // 并发都执行成功，将exec_node加入cache
            for (auto pair: cache_map) {
                push_cmd_to_cache(state, _op_type, pair.second, pair.first);
            }
            _children.erase(_children.begin() + start_child, _children.end());
        } else {
            // 有执行失败，回滚
            std::string seq_id_str = "[";
            for (int seq_id = prev_seq_id + 1; seq_id <= client_conn->seq_id; seq_id++) {
                client_conn->need_rollback_seq.insert(seq_id);
                seq_id_str += std::to_string(seq_id) + ",";
            }
            for (auto seq_id: _seq_ids) {
                client_conn->need_rollback_seq.insert(seq_id);
                seq_id_str += std::to_string(seq_id) + ",";
            }
            seq_id_str += "]";
            TLOG_WARN("exec node failed error:{} log_id:{} need_rollback_seq:{}", error,
                      state->log_id(), seq_id_str.c_str());
            return -1;
        }
        return 0;
    }
} // namespace EA
