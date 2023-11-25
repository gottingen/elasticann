// Copyright 2023 The Elastic-AI Authors.
// part of Elastic AI Search
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

#ifndef ELASTICANN_META_SERVER_META_CONSTANTS_H_
#define ELASTICANN_META_SERVER_META_CONSTANTS_H_

#include <string>

namespace EA {

    struct MetaConstants {
        static const std::string CLUSTER_IDENTIFY;
        static const std::string LOGICAL_CLUSTER_IDENTIFY;
        static const std::string LOGICAL_KEY;
        static const std::string PHYSICAL_CLUSTER_IDENTIFY;
        static const std::string INSTANCE_CLUSTER_IDENTIFY;
        static const std::string INSTANCE_PARAM_CLUSTER_IDENTIFY;

        static const std::string PRIVILEGE_IDENTIFY;

        static const std::string SCHEMA_IDENTIFY;
        static const std::string MAX_ID_SCHEMA_IDENTIFY;
        static const std::string NAMESPACE_SCHEMA_IDENTIFY;
        static const std::string DATABASE_SCHEMA_IDENTIFY;
        static const std::string ZONE_SCHEMA_IDENTIFY;
        static const std::string SERVLET_SCHEMA_IDENTIFY;
        static const std::string TABLE_SCHEMA_IDENTIFY;
        static const std::string REGION_SCHEMA_IDENTIFY;
        static const std::string DDLWORK_IDENTIFY;
        static const std::string STATISTICS_IDENTIFY;
        static const std::string INDEX_DDLWORK_REGION_IDENTIFY;

        static const std::string CONFIG_IDENTIFY;

        static const std::string MAX_IDENTIFY;

        /// for schema
        static const std::string MAX_NAMESPACE_ID_KEY;
        static const std::string MAX_DATABASE_ID_KEY;
        static const std::string MAX_ZONE_ID_KEY;
        static const std::string MAX_SERVLET_ID_KEY;
        static const std::string MAX_TABLE_ID_KEY;
        static const std::string MAX_REGION_ID_KEY;
    };
}  // namespace EA

#endif  // ELASTICANN_META_SERVER_META_CONSTANTS_H_
