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


#include "elasticann/meta_server/query_database_manager.h"
#include "elasticann/base/tlog.h"

namespace EA {
    void QueryDatabaseManager::get_database_info(const proto::QueryRequest *request,
                                                 proto::QueryResponse *response) {
        DatabaseManager *manager = DatabaseManager::get_instance();
        BAIDU_SCOPED_LOCK(manager->_database_mutex);
        if (!request->has_database()) {
            for (auto &database_info: manager->_database_info_map) {
                *(response->add_database_infos()) = database_info.second;
            }
        } else {
            std::string namespace_name = request->namespace_name();
            std::string database = namespace_name + "\001" + request->database();
            if (manager->_database_id_map.find(database) != manager->_database_id_map.end()) {
                int64_t id = manager->_database_id_map[database];
                *(response->add_database_infos()) = manager->_database_info_map[id];
            } else {
                response->set_errmsg("database not exist");
                response->set_errcode(proto::INPUT_PARAM_ERROR);
                TLOG_ERROR("namespace: {} database: {} not exist", namespace_name, database);
            }
        }
    }

}  // namespace EA
