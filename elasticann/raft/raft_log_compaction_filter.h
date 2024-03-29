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

#include <rocksdb/compaction_filter.h>
#include <bthread/mutex.h>
#include "elasticann/raft/my_raft_log_storage.h"
#include "elasticann/common/key_encoder.h"
#include "elasticann/raft/split_index_getter.h"

namespace EA {
    class RaftLogCompactionFilter : public rocksdb::CompactionFilter {
    public:
        static RaftLogCompactionFilter *get_instance() {
            static RaftLogCompactionFilter _instance;
            return &_instance;
        }

        ~RaftLogCompactionFilter() {
            bthread_mutex_destroy(&_mutex);
        }

        const char *Name() const override {
            return "RaftLogCompactionFilter";
        }

        // The compaction process invokes this method for kv that is being compacted.
        // A return value of false indicates that the kv should be preserved
        // a return value of true indicates that this key-value should be removed from the
        // output of the compaction.
        bool Filter(int /*level*/,
                    const rocksdb::Slice &key,
                    const rocksdb::Slice & /*existing_value*/,
                    std::string * /*new_value*/,
                    bool * /*value_changed*/) const override {
            if (key.size() != MyRaftLogStorage::LOG_DATA_KEY_SIZE) {
                return false;
            }
            uint64_t region_id_tmp = *(uint64_t *) key.data();
            int64_t region_id = KeyEncoder::decode_i64(KeyEncoder::to_endian_u64(region_id_tmp));
            uint64_t index_tmp = *(uint64_t *) (key.data() + sizeof(int64_t) + 1);
            int64_t index = KeyEncoder::decode_i64(KeyEncoder::to_endian_u64(index_tmp));
            //TLOG_INFO("filter parse region_id: {}, index: {}", region_id, index);
            BAIDU_SCOPED_LOCK(_mutex);
            auto iter = _first_index_map.find(region_id);
            if (iter == _first_index_map.end()) {
                //暂时安全考虑先返回false, 后续再考虑是不是直接删除
                return false;
            }
            //由于做分裂禁止删除的log_index
            int64_t split_log_index = SplitIndexGetter::get_instance()->get_split_index(region_id);

            // index < fisrt_log_index, return true
            return index < ((iter->second < split_log_index) ? iter->second : split_log_index);
        }

        int update_first_index_map(int64_t region_id, int64_t index) {
            TLOG_INFO("update compaction fileter, region_id: {}, index: {}",
                       region_id, index);
            BAIDU_SCOPED_LOCK(_mutex);
            _first_index_map[region_id] = index;
            return 0;
        }

        int remove_region_id(int64_t region_id) {
            TLOG_INFO("remove compaction fileter, region_id: {}", region_id);
            BAIDU_SCOPED_LOCK(_mutex);
            _first_index_map.erase(region_id);
            return 0;
        }

        void print_map() {
            for (auto &iter: _first_index_map) {
                TLOG_INFO("region_id:{}, first_index:{}",
                           iter.first, iter.second);
            }
        }

    private:
        RaftLogCompactionFilter() {
            bthread_mutex_init(&_mutex, nullptr);
        }

        // key:region_id, value: first_log_index
        std::unordered_map<int64_t, int64_t> _first_index_map;
        mutable bthread_mutex_t _mutex;
    };
}//namespace
