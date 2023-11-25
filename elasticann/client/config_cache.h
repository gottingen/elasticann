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

#ifndef ELASTICANN_CLIENT_CONFIG_CACHE_H_
#define ELASTICANN_CLIENT_CONFIG_CACHE_H_

#include "turbo/container/flat_hash_map.h"
#include <map>
#include <mutex>
#include "turbo/base/status.h"
#include "turbo/module/module_version.h"
#include "eaproto/meta/meta.struct.pb.h"
#include "elasticann/base/bthread.h"

namespace EA::client {

    class ConfigCache {
    public:
        static ConfigCache *get_instance() {
            static ConfigCache ins;
            return &ins;
        }

        ///
        /// \brief init cache
        /// \return
        turbo::Status init();

        ///
        /// \param config
        /// \return
        turbo::Status add_config(const EA::proto::ConfigInfo &config);

        ///
        /// \brief get extract
        /// \param name
        /// \param version
        /// \param config
        /// \return
        turbo::Status
        get_config(const std::string &name, const turbo::ModuleVersion &version, EA::proto::ConfigInfo &config);

        ///
        /// \brief get latest version of config
        /// \param name
        /// \param config
        /// \return
        turbo::Status get_config(const std::string &name, EA::proto::ConfigInfo &config);

        ///
        /// \param name
        /// \return
        turbo::Status get_config_list(std::vector<std::string> &name);

        ///
        /// \param config_name
        /// \param versions
        /// \return
        turbo::Status
        get_config_version_list(const std::string &config_name, std::vector<turbo::ModuleVersion> &versions);

        ///
        /// \brief remove signal version of config
        /// \param config_name
        /// \param version
        /// \return
        turbo::Status remove_config(const std::string &config_name, const turbo::ModuleVersion &version);

        ///
        /// \param config_name
        /// \param version
        /// \return
        turbo::Status remove_config(const std::string &config_name, const std::vector<turbo::ModuleVersion> &version);

        ///
        /// \brief remove config versions less than the given version
        /// \param config_name
        /// \param version
        /// \return
        turbo::Status remove_config_less_than(const std::string &config_name, const turbo::ModuleVersion &version);

        ///
        /// \brief remove all version of config
        /// \param config_name
        /// \return
        turbo::Status remove_config(const std::string &config_name);

    private:
        turbo::Status write_config_file(const std::string &dir, const EA::proto::ConfigInfo &config);

        turbo::Status remove_config_file(const std::string &dir, const EA::proto::ConfigInfo &config);

        std::string make_cache_file_path(const std::string &dir, const EA::proto::ConfigInfo &config);

        void do_add_config(const EA::proto::ConfigInfo &config);

    private:
        typedef turbo::flat_hash_map<std::string, std::map<turbo::ModuleVersion, EA::proto::ConfigInfo>> CacheType;
        std::mutex _cache_mutex;
        CacheType _cache_map;
        std::string _cache_dir;
        bool        _init{false};
    };
}  // namespace EA::client

#endif  // ELASTICANN_CLIENT_CONFIG_CACHE_H_