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

#include <atomic>
#include "elasticann/common/common.h"
#include "eaproto/db/store.interface.pb.h"

namespace EA {
    class Region;

    class RegionControl {
        typedef std::shared_ptr<Region> SmartRegion;
    public:
        static int remove_data(int64_t drop_region_id);

        static void compact_data(int64_t region_id);

        static void compact_data_in_queue(int64_t region_id);

        static int remove_log_entry(int64_t drop_region_id);

        static int remove_meta(int64_t drop_region_id);

        static int remove_snapshot_path(int64_t drop_region_id);

        static int clear_all_infos_for_region(int64_t drop_region_id);

        static int ingest_data_sst(const std::string &data_sst_file, int64_t region_id, bool move_files);

        static int ingest_meta_sst(const std::string &meta_sst_file, int64_t region_id);

        RegionControl(Region *region, int64_t region_id) : _region(region), _region_id(region_id) {}

        virtual ~RegionControl() = default;

        int sync_do_snapshot();

        void raft_control(google::protobuf::RpcController *controller,
                          const proto::RaftControlRequest *request,
                          proto::RaftControlResponse *response,
                          google::protobuf::Closure *done);

        void add_peer(const proto::AddPeer &add_peer, SmartRegion region, ExecutionQueue &queue);

        void add_peer(const proto::AddPeer *request,
                      proto::StoreRes *response,
                      google::protobuf::Closure *done);

        int transfer_leader(const proto::TransLeaderRequest &trans_leader_request,
                            SmartRegion region, ExecutionQueue &queue);

        int init_region_to_store(const std::string instance_address,
                                 const proto::InitRegion &init_region_request,
                                 proto::StoreRes *store_response);

        void store_status(const proto::RegionStatus &status) {
            _status.store(status);
        }

        proto::RegionStatus get_status() const {
            return _status.load();
        }

        // doing -> idle
        void reset_region_status() {
            BAIDU_SCOPED_LOCK(_mutex);
            if (_doing_cnt > 0) {
                _doing_cnt--;
            } else {
                proto::RegionStatus expected_status = proto::DOING;
                if (!_status.compare_exchange_strong(expected_status, proto::IDLE)) {
                    TLOG_WARN("region status is not doing, region_id: {}", _region_id);
                }
            }
            TLOG_WARN("region {} doing cnt: {}", _region_id, _doing_cnt);
        }

        // idle -> doing
        int make_region_status_doing() {
            BAIDU_SCOPED_LOCK(_mutex);
            if (_doing_cnt > 0) {
                TLOG_WARN("region doing is not 0, region_id: {}, doing cnt: {}", _region_id, _doing_cnt);
                return -1;
            }
            proto::RegionStatus expected_status = proto::IDLE;
            if (!_status.compare_exchange_strong(expected_status, proto::DOING)) {
                TLOG_WARN("region status is not idle, region_id: {}", _region_id);
                return -1;
            }
            return 0;
        }

        // 目前只有分裂add peer会调用
        int add_doing_cnt() {
            BAIDU_SCOPED_LOCK(_mutex);
            if (_status.load() == proto::DOING) {
                // doing状态下，doing cnt++
                // 分裂同步add peer一定是这种情况
                // 分裂异步add peer可能是这种情况
                _doing_cnt++;
            } else {
                // idle -> doing
                // 分裂异步add peer可能是这种情况
                if (_doing_cnt > 0) {
                    TLOG_ERROR("region doing is not 0, region_id: {}, doing cnt: {}", _region_id, _doing_cnt);
                    return -1;
                }
                proto::RegionStatus expected_status = proto::IDLE;
                if (!_status.compare_exchange_strong(expected_status, proto::DOING)) {
                    TLOG_WARN("region status is not idle when add peer, region_id: {}", _region_id);
                    return -1;
                }
            }
            TLOG_WARN("region {} doing cnt: {}", _region_id, _doing_cnt);
            return 0;
        }

    private:
        void construct_init_region_request(proto::InitRegion &init_region_request);

        int legal_for_add_peer(const proto::AddPeer &add_peer, proto::StoreRes *response);

        void node_add_peer(const proto::AddPeer &add_peer,
                           const std::string &new_instance,
                           proto::StoreRes *response,
                           google::protobuf::Closure *done);

    private:
        Region *_region = nullptr;
        int64_t _region_id = 0;
        //region status,用在split、merge、ddl、ttl、add_peer、remove_peer时
        //保证操作串行
        //只有leader有状态 ddl?
        std::atomic<proto::RegionStatus> _status;
        bthread::Mutex _mutex;
        int _doing_cnt = 0;
    };
} // end of namespace
