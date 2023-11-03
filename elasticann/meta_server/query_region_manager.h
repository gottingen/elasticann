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

#include "elasticann/meta_server/region_manager.h"

namespace EA {
    class QueryRegionManager {
    public:
        ~QueryRegionManager() {}

        static QueryRegionManager *get_instance() {
            static QueryRegionManager instance;
            return &instance;
        }

        void get_flatten_region(const proto::QueryRequest *request, proto::QueryResponse *response);

        void get_region_info(const proto::QueryRequest *request, proto::QueryResponse *response);

        void get_region_info_per_instance(const std::string &instance, proto::QueryResponse *response);

        void get_region_count_per_instance(
                const std::string &instance,
                int64_t &region_count,
                int64_t &region_leader_count);

        void get_peer_ids_per_instance(
                const std::string &instance,
                std::set<int64_t> &peer_ids);

        void send_transfer_leader(const proto::QueryRequest *request, proto::QueryResponse *response);

        void send_set_peer(const proto::QueryRequest *request, proto::QueryResponse *response);

        void get_region_peer_status(const proto::QueryRequest *request, proto::QueryResponse *response);

        void get_region_learner_status(const proto::QueryRequest *request, proto::QueryResponse *response);

        void send_remove_region_request(std::string instance_address, int64_t region_id);

        void check_region_and_update(
                const std::unordered_map<int64_t, proto::RegionHeartBeat> &
                region_version_map,
                proto::ConsoleHeartBeatResponse *response);

    private:
        void construct_query_region(const proto::RegionInfo *region_info,
                                    proto::QueryRegion *query_region_info);

        QueryRegionManager() {}
    }; //class

}  // namespace EA
