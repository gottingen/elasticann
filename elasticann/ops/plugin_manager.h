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


#ifndef ELASTICANN_OPS_PLUGIN_MANAGER_H_
#define ELASTICANN_OPS_PLUGIN_MANAGER_H_

#include <braft/raft.h>
#include "eaproto/service/file_service.pb.h"
#include <string>
#include <string_view>
#include <memory>
#include <turbo/container/flat_hash_map.h>
#include "turbo/module/module_version.h"

namespace EA {

    struct PluginEntity {
        PluginEntity(const std::string &name, const turbo::ModuleVersion &v);
        ~PluginEntity();
        int         fd{-1};
        int64_t    size{0};
        std::string cksm;
        std::string path;
        bool finish{false};
        bool open();
        void close();
    };

    struct Plugin {
        explicit Plugin(const std::string_view &n) : name(n) {

        }

        const std::string name;
        /// version string -> PluginEntity
        std::map<turbo::ModuleVersion, std::shared_ptr<PluginEntity>> entities;
    };

    class PluginManager {
    public:
        typedef turbo::flat_hash_map<std::string, std::shared_ptr<Plugin>> PluginMap;
    public:
        static PluginManager *get_instance() {
            static PluginManager ins;
            return &ins;
        }
        static std::string make_plugin_file_name(const std::string &name, const turbo::ModuleVersion &v);

        void create_plugin(const ::EA::proto::FileManageRequest &request, braft::Closure *done);

        void upload_plugin(const ::EA::proto::FileManageRequest &request, braft::Closure *done);

        void remove_plugin(const ::EA::proto::FileManageRequest &request, braft::Closure *done);

    private:
        PluginManager();


        std::shared_ptr<PluginEntity> get_ready_plugin_ptr(const std::string &name, const turbo::ModuleVersion &version);
        std::shared_ptr<PluginEntity> get_uploading_plugin_ptr(const std::string &name, const turbo::ModuleVersion &version);
        std::shared_ptr<PluginEntity> get_remove_plugin_ptr(const std::string &name, const turbo::ModuleVersion &version);
        bool make_plugin_ready(const std::string &name, const turbo::ModuleVersion &version);
        void remove_signal(const std::string &name, const turbo::ModuleVersion &version, braft::Closure *done);

        void remove_all(const std::string &name, braft::Closure *done);
        void add_plugin(PluginMap &map, const std::string &name);

    private:
        /// finish upload plugins
        std::mutex _meta_lock;
        PluginMap _plugin_map;
        /// uploading plugins
        PluginMap _uploading_plugin_map;
        PluginMap _removed_plugin_map;
    };
}  // namespace EA

#endif  // ELASTICANN_OPS_PLUGIN_MANAGER_H_
