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
#include "elasticann/ops/query_plugin_manager.h"
#include "elasticann/ops/plugin_manager.h"
#include "elasticann/ops/file_util.h"
#include "elasticann/common/common.h"

namespace EA {

    void
    QueryPluginManager::download_plugin(const ::EA::proto::QueryOpsServiceRequest *request,
                                        ::EA::proto::QueryOpsServiceResponse *response) {
        response->set_op_type(request->op_type());
        auto &download_request = request->query_plugin();
        if (!download_request.has_version()) {
            response->set_errmsg("plugin not set version");
            response->set_errcode(proto::INPUT_PARAM_ERROR);
            return;
        }
        turbo::ModuleVersion version = turbo::ModuleVersion(download_request.version().major(), download_request.version().minor(),
                                                            download_request.version().patch());
        EA::proto::PluginEntiry entity;
        auto &name = download_request.name();
        {
            BAIDU_SCOPED_LOCK(PluginManager::get_instance()->_plugin_mutex);
            auto &plugins = PluginManager::get_instance()->_plugins;
            auto it = plugins.find(name);
            if (it == plugins.end() || it->second.empty()) {
                response->set_errmsg("plugin not exist");
                response->set_errcode(proto::INPUT_PARAM_ERROR);
                return;
            }
            auto pit = it->second.find(version);
            if(pit == it->second.end()) {
                response->set_errmsg("plugin not exist");
                response->set_errcode(proto::INPUT_PARAM_ERROR);
                return;
            }
            entity = pit->second;
        }

        if (!download_request.has_offset()) {
            response->set_errmsg("plugin not set offset");
            response->set_errcode(proto::INPUT_PARAM_ERROR);
            return;
        }

        if (!download_request.has_count()) {
            response->set_errmsg("plugin not set count");
            response->set_errcode(proto::INPUT_PARAM_ERROR);
            return;
        }
        std::string key = PluginManager::make_plugin_key(name, version);

        CacheFd cfd;
        auto libname = PluginManager::make_plugin_path(name, version, entity.platform());
        std::string file_path = FLAGS_service_plugin_data_root + "/" + libname;
        if(_cache.find(key, &cfd) != 0) {
            int fd = ::open(file_path.c_str(), O_RDONLY, 0644);
            if(fd < 0 ) {
                response->set_errmsg("read plugin file error");
                response->set_errcode(proto::INTERNAL_ERROR);
                return;
            }
            _cache.add(key, cfd);
        }
        auto len = download_request.count();
        auto offset = download_request.offset();
        char* buf = new char[len];
        if (offset + len > entity.size()) {
            len = entity.size() - offset;
        }
        ssize_t n = full_pread(cfd.fd, buf, len, offset);
        // do not close, cache it.
        cfd.release();
        if (n < 0 || n != (ssize_t) len) {
            TLOG_ERROR("Fail to pread plugin:{} for req:{}", file_path, request->DebugString());
            response->set_errcode(proto::INTERNAL_ERROR);
            response->set_errmsg("plugin:" + name + " read failed");
            delete[] buf;
            return;
        }
        response->mutable_plugin_response()->set_content(buf, len);
        delete[] buf;
        PluginManager::transfer_entity_to_info(&entity, response->mutable_plugin_response()->mutable_plugin());
        response->set_errmsg("success");
        response->set_errcode(proto::SUCCESS);
    }

    void QueryPluginManager::plugin_info(const ::EA::proto::QueryOpsServiceRequest *request,
                                         ::EA::proto::QueryOpsServiceResponse *response) {
        response->set_op_type(request->op_type());
        BAIDU_SCOPED_LOCK(PluginManager::get_instance()->_plugin_mutex);
        auto &plugins = PluginManager::get_instance()->_plugins;
        auto &get_request = request->query_plugin();
        auto &name = get_request.name();
        auto it = plugins.find(name);
        if (it == plugins.end() || it->second.empty()) {
            response->set_errmsg("plugin not exist");
            response->set_errcode(proto::INPUT_PARAM_ERROR);
            return;
        }
        turbo::ModuleVersion version;

        if (!get_request.has_version()) {
            // use newest
            // version = it->second.rend()->first;
            auto cit = it->second.rbegin();
            PluginManager::transfer_entity_to_info(&cit->second, response->mutable_plugin_response()->mutable_plugin());
            response->set_errmsg("success");
            response->set_errcode(proto::SUCCESS);
            return;
        }

        version = turbo::ModuleVersion(get_request.version().major(), get_request.version().minor(),
                                       get_request.version().patch());

        auto cit = it->second.find(version);
        if (cit == it->second.end()) {
            /// not exists
            response->set_errmsg("plugin not exist");
            response->set_errcode(proto::INPUT_PARAM_ERROR);
            return;
        }

        PluginManager::transfer_entity_to_info(&cit->second, response->mutable_plugin_response()->mutable_plugin());
        response->set_errmsg("success");
        response->set_errcode(proto::SUCCESS);
    }

    void QueryPluginManager::tombstone_plugin_info(const ::EA::proto::QueryOpsServiceRequest *request,
                                                   ::EA::proto::QueryOpsServiceResponse *response) {
        response->set_op_type(request->op_type());
        BAIDU_SCOPED_LOCK(PluginManager::get_instance()->_tombstone_plugin_mutex);
        auto &tombstone_plugins = PluginManager::get_instance()->_tombstone_plugins;
        auto &get_request = request->query_plugin();
        auto &name = get_request.name();
        auto it = tombstone_plugins.find(name);
        if (it == tombstone_plugins.end() || it->second.empty()) {
            response->set_errmsg("plugin not exist");
            response->set_errcode(proto::INPUT_PARAM_ERROR);
            return;
        }
        turbo::ModuleVersion version;

        if (!get_request.has_version()) {
            // use newest
            // version = it->second.rend()->first;
            auto cit = it->second.rbegin();
            PluginManager::transfer_entity_to_info(&cit->second, response->mutable_plugin_response()->mutable_plugin());
            response->set_errmsg("success");
            response->set_errcode(proto::SUCCESS);
            return;
        }

        version = turbo::ModuleVersion(get_request.version().major(), get_request.version().minor(),
                                       get_request.version().patch());

        auto cit = it->second.find(version);
        if (cit == it->second.end()) {
            /// not exists
            response->set_errmsg("plugin not exist");
            response->set_errcode(proto::INPUT_PARAM_ERROR);
            return;
        }

        PluginManager::transfer_entity_to_info(&cit->second, response->mutable_plugin_response()->mutable_plugin());
        response->set_errmsg("success");
        response->set_errcode(proto::SUCCESS);
    }


    void QueryPluginManager::list_plugin(const ::EA::proto::QueryOpsServiceRequest *request,
                                         ::EA::proto::QueryOpsServiceResponse *response) {
        response->set_op_type(request->op_type());
        BAIDU_SCOPED_LOCK(PluginManager::get_instance()->_plugin_mutex);
        auto plugins = PluginManager::get_instance()->_plugins;
        response->mutable_plugin_response()->mutable_plugin_list()->Reserve(plugins.size());
        for (auto it = plugins.begin(); it != plugins.end(); ++it) {
            response->mutable_plugin_response()->add_plugin_list(it->first);
        }
        response->set_errmsg("success");
        response->set_errcode(proto::SUCCESS);
    }

    void QueryPluginManager::tombstone_list_plugin(const ::EA::proto::QueryOpsServiceRequest *request,
                                                   ::EA::proto::QueryOpsServiceResponse *response) {
        response->set_op_type(request->op_type());
        BAIDU_SCOPED_LOCK(PluginManager::get_instance()->_tombstone_plugin_mutex);
        auto &tombstone_plugins = PluginManager::get_instance()->_tombstone_plugins;
        response->mutable_plugin_response()->mutable_plugin_list()->Reserve(tombstone_plugins.size());
        for (auto it = tombstone_plugins.begin(); it != tombstone_plugins.end(); ++it) {
            response->mutable_plugin_response()->add_plugin_list(it->first);
        }
        response->set_errmsg("success");
        response->set_errcode(proto::SUCCESS);
    }

    void QueryPluginManager::list_plugin_version(const ::EA::proto::QueryOpsServiceRequest *request,
                                                 ::EA::proto::QueryOpsServiceResponse *response) {
        response->set_op_type(request->op_type());
        auto &get_request = request->query_plugin();
        BAIDU_SCOPED_LOCK(PluginManager::get_instance()->_plugin_mutex);
        auto &plugins = PluginManager::get_instance()->_plugins;
        auto &name = get_request.name();
        auto it = plugins.find(name);
        if (it == plugins.end()) {
            response->set_errmsg("plugin not exist");
            response->set_errcode(proto::INPUT_PARAM_ERROR);
            return;
        }
        response->mutable_plugin_response()->mutable_versions()->Reserve(it->second.size());
        for (auto vit = it->second.begin(); vit != it->second.end(); ++vit) {
            *(response->mutable_plugin_response()->add_versions()) = vit->second.version();
        }
        response->set_errmsg("success");
        response->set_errcode(proto::SUCCESS);
    }

    void QueryPluginManager::tombstone_list_plugin_version(const ::EA::proto::QueryOpsServiceRequest *request,
                                                           ::EA::proto::QueryOpsServiceResponse *response) {
        response->set_op_type(request->op_type());
        auto &get_request = request->query_plugin();
        BAIDU_SCOPED_LOCK(PluginManager::get_instance()->_tombstone_plugin_mutex);
        auto &tombstone_plugins = PluginManager::get_instance()->_tombstone_plugins;
        auto &name = get_request.name();
        auto it = tombstone_plugins.find(name);
        if (it == tombstone_plugins.end()) {
            response->set_errmsg("plugin not exist");
            response->set_errcode(proto::INPUT_PARAM_ERROR);
            return;
        }
        response->mutable_plugin_response()->mutable_versions()->Reserve(it->second.size());
        for (auto vit = it->second.begin(); vit != it->second.end(); ++vit) {
            *(response->mutable_plugin_response()->add_versions()) = vit->second.version();
        }
        response->set_errmsg("success");
        response->set_errcode(proto::SUCCESS);
    }
}  // namespace EA
