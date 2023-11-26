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
#include "elasticann/base/key_encoder.h"
#include "elasticann/base/double_buffer.h"

namespace EA {

    class SplitCompactionFilter : public rocksdb::CompactionFilter {
        struct FilterRegionInfo {
            FilterRegionInfo(bool use_ttl, const std::string &end_key, int64_t online_ttl_base_expire_time_us) :
                    use_ttl(use_ttl), end_key(end_key),
                    online_ttl_base_expire_time_us(online_ttl_base_expire_time_us) {}

            bool use_ttl = false;
            std::string end_key;
            int64_t online_ttl_base_expire_time_us = 0;
        };

        typedef butil::FlatMap<int64_t, FilterRegionInfo *> KeyMap;
        typedef DoubleBuffer<KeyMap> DoubleBufKey;
        typedef butil::FlatSet<int64_t> BinlogSet;
        typedef DoubleBuffer<BinlogSet> DoubleBufBinlog;
    public:
        static SplitCompactionFilter *get_instance() {
            static SplitCompactionFilter _instance;
            return &_instance;
        }

        virtual ~SplitCompactionFilter() {
        }

        const char *Name() const override {
            return "SplitCompactionFilter";
        }

        // The compaction process invokes this method for kv that is being compacted.
        // A return value of false indicates that the kv should be preserved
        // a return value of true indicates that this key-value should be removed from the
        // output of the compaction.
        bool Filter(int level,
                    const rocksdb::Slice &key,
                    const rocksdb::Slice &value,
                    std::string * /*new_value*/,
                    bool * /*value_changed*/) const override {
            //只对最后2层做filter
            return false;
        }

        void set_filter_region_info(int64_t region_id, const std::string &end_key,
                                    bool use_ttl, int64_t online_ttl_base_expire_time_us) {
            FilterRegionInfo *old = get_filter_region_info(region_id);
            // 已存在不更新
            if (old != nullptr && old->end_key == end_key) {
                return;
            }
            auto call = [region_id, end_key, use_ttl, online_ttl_base_expire_time_us](KeyMap &key_map) {
                FilterRegionInfo *new_info = new FilterRegionInfo(use_ttl, end_key, online_ttl_base_expire_time_us);
                key_map[region_id] = new_info;
            };
            _range_key_map.modify(call);
        }

        FilterRegionInfo *get_filter_region_info(int64_t region_id) const {
            auto iter = _range_key_map.read()->seek(region_id);
            if (iter != nullptr) {
                return *iter;
            }
            return nullptr;
        }

        void set_binlog_region(int64_t region_id) {
            auto call = [this, region_id](BinlogSet &region_id_set) {
                region_id_set.insert(region_id);
            };
            _binlog_region_id_set.modify(call);
        }

        bool is_binlog_region(int64_t region_id) const {
            auto iter = _binlog_region_id_set.read()->seek(region_id);
            if (iter != nullptr) {
                return true;
            }
            return false;
        }

    private:
        SplitCompactionFilter() {
            _range_key_map.read_background()->init(12301);
            _range_key_map.read()->init(12301);
            _binlog_region_id_set.read_background()->init(12301);
            _binlog_region_id_set.read()->init(12301);
        }

        // region_id => end_key
        mutable DoubleBufKey _range_key_map;
        mutable DoubleBufBinlog _binlog_region_id_set;
    };
}//namespace

