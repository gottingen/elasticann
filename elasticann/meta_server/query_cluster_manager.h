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

#include "elasticann/meta_server/cluster_manager.h"

namespace EA {
    class ClusterManager;

    class QueryClusterManager {
    public:
        ~QueryClusterManager() {}

        static QueryClusterManager *get_instance() {
            static QueryClusterManager instance;
            return &instance;
        }

        void get_logical_info(const proto::QueryRequest *request, proto::QueryResponse *response);

        void get_physical_info(const proto::QueryRequest *request, proto::QueryResponse *response);

        void get_instance_info(const proto::QueryRequest *request, proto::QueryResponse *response);

        void get_instance_param(const proto::QueryRequest *request, proto::QueryResponse *response);

        void get_flatten_instance(const proto::QueryRequest *request, proto::QueryResponse *response);

        void get_diff_region_ids(const proto::QueryRequest *request, proto::QueryResponse *response);

        void get_region_ids(const proto::QueryRequest *request, proto::QueryResponse *response);

        void get_network_segment(const proto::QueryRequest *request, proto::QueryResponse *response);

        void process_console_heartbeat(const proto::ConsoleHeartBeatRequest *request,
                                       proto::ConsoleHeartBeatResponse *response);

    private:
        void mem_instance_to_pb(const Instance &instance_mem, proto::InstanceInfo *instance_pb);

        void get_region_ids_per_instance(const std::string &instance,
                                         std::set<int64_t> &region_ids);

        void get_region_count_per_instance(const std::string &instance,
                                           int64_t &count);

        void construct_query_response_for_instance(const Instance &instance_info,
                                                   std::map<std::string, std::multimap<std::string, proto::QueryInstance>> &logical_instance_infos);

        QueryClusterManager() {}
    };

}  // namespace EA
