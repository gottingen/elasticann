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


#ifndef ELASTICANN_META_CONFIG_MANAGER_H_
#define ELASTICANN_META_CONFIG_MANAGER_H_

#include "turbo/container/flat_hash_map.h"
#include "eaproto/meta/meta.interface.pb.h"
#include "turbo/container/flat_hash_map.h"
#include "elasticann/meta_server/meta_state_machine.h"
#include "turbo/module/module_version.h"
#include "elasticann/meta_server/meta_server.h"
#include <braft/raft.h>
#include <bthread/mutex.h>

namespace EA {

    class ConfigManager {
    public:
        static turbo::ModuleVersion kDefaultVersion;
        static ConfigManager *get_instance() {
            static ConfigManager ins;
            return &ins;
        }

        ~ConfigManager();

        void process_schema_info(google::protobuf::RpcController *controller,
                                                const proto::MetaManagerRequest *request,
                                                proto::MetaManagerResponse *response,
                                                google::protobuf::Closure *done);
        void create_config(const ::EA::proto::MetaManagerRequest &request, braft::Closure *done);

        void remove_config(const ::EA::proto::MetaManagerRequest &request, braft::Closure *done);

        int load_snapshot();

        static std::string make_config_key(const std::string &name, const turbo::ModuleVersion &version);

        void set_meta_state_machine(MetaStateMachine *machine);
    private:
        ConfigManager();

        friend class QueryConfigManager;

        int load_config_snapshot(const std::string &value);

        void remove_config_all(const ::EA::proto::MetaManagerRequest &request, braft::Closure *done);

    private:
        MetaStateMachine *_meta_state_machine;
        bthread_mutex_t _config_mutex;
        turbo::flat_hash_map<std::string, std::map<turbo::ModuleVersion, EA::proto::ConfigInfo>> _configs;

    };

    ///
    /// inlines
    ///

    inline ConfigManager::ConfigManager() {
        bthread_mutex_init(&_config_mutex, nullptr);
    }

    inline ConfigManager::~ConfigManager() {
        bthread_mutex_destroy(&_config_mutex);
    }

    inline void ConfigManager::set_meta_state_machine(MetaStateMachine *machine) {
        _meta_state_machine = machine;
    }
}  // namespace EA
#endif  // ELASTICANN_META_CONFIG_MANAGER_H_
