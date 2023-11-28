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

#include "elasticann/meta_server/meta_constants.h"

namespace EA {

    const std::string MetaConstants::CLUSTER_IDENTIFY(1, 0x01);
    const std::string MetaConstants::LOGICAL_CLUSTER_IDENTIFY(1, 0x01);
    const std::string MetaConstants::LOGICAL_KEY = "logical_room";
    const std::string MetaConstants::PHYSICAL_CLUSTER_IDENTIFY(1, 0x02);
    const std::string MetaConstants::INSTANCE_CLUSTER_IDENTIFY(1, 0x03);
    const std::string MetaConstants::INSTANCE_PARAM_CLUSTER_IDENTIFY(1, 0x04);

    const std::string MetaConstants::PRIVILEGE_IDENTIFY(1, 0x03);

    const std::string MetaConstants::SCHEMA_IDENTIFY(1, 0x02);
    const std::string MetaConstants::MAX_ID_SCHEMA_IDENTIFY(1, 0x01);
    const std::string MetaConstants::NAMESPACE_SCHEMA_IDENTIFY(1, 0x02);
    const std::string MetaConstants::DATABASE_SCHEMA_IDENTIFY(1, 0x03);
    const std::string MetaConstants::TABLE_SCHEMA_IDENTIFY(1, 0x04);
    const std::string MetaConstants::REGION_SCHEMA_IDENTIFY(1, 0x05);
    const std::string MetaConstants::ZONE_SCHEMA_IDENTIFY(1, 0x09);
    const std::string MetaConstants::SERVLET_SCHEMA_IDENTIFY(1, 0x0A);

    const std::string MetaConstants::DDLWORK_IDENTIFY(1, 0x06);
    const std::string MetaConstants::STATISTICS_IDENTIFY(1, 0x07);
    const std::string MetaConstants::INDEX_DDLWORK_REGION_IDENTIFY(1, 0x08);

    const std::string MetaConstants::CONFIG_IDENTIFY(1, 0x04);

    const std::string MetaConstants::MAX_IDENTIFY(1, 0xFF);

    /// for schema
    const std::string MetaConstants::MAX_NAMESPACE_ID_KEY = "max_namespace_id";
    const std::string MetaConstants::MAX_DATABASE_ID_KEY = "max_database_id";
    const std::string MetaConstants::MAX_ZONE_ID_KEY = "max_zone_id";
    const std::string MetaConstants::MAX_SERVLET_ID_KEY = "max_zone_id";
    const std::string MetaConstants::MAX_TABLE_ID_KEY = "max_table_id";
    const std::string MetaConstants::MAX_REGION_ID_KEY = "max_region_id";

    const int MetaConstants::MetaMachineRegion = 0;
    const int MetaConstants::AutoIDMachineRegion = 1;
    const int MetaConstants::TsoMachineRegion = 2;
}  // namespace EA
