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
/*
#include "elasticann/ops/plugin_manager.h"
#include "elasticann/ops/plugin/plugin_state_machine.h"
#include "elasticann/common/common.h"
#include "elasticann/ops/file_util.h"

namespace EA {
    PluginEntity::PluginEntity(const std::string &name, const turbo::ModuleVersion &v) {
        turbo::filesystem::path pp(FLAGS_service_plugin_data_root);
        auto file_name = PluginManager::make_plugin_file_name(name, v);
        pp /= file_name;
        path = pp.string();
    }
    PluginEntity::~PluginEntity() {
        close();
    }

    bool PluginEntity::open() {
        if(fd > 0) {
            return true;
        }
        fd = ::open(path.c_str(), O_RDWR | O_CREAT, 0644);
        if(fd < 0) {
            return false;
        }
        int64_t n = md5_sum_file(path, cksm);
        if(n < 0) {
            TLOG_ERROR("fail to md5 sum {}", path);
        }
        return true;
    }

    void PluginEntity::close() {
        if(fd > 0) {
            ::close(fd);
            fd = -1;
        }
    }

    PluginManager::PluginManager() {
        if(!turbo::filesystem::exists(FLAGS_service_plugin_data_root)) {
            turbo::filesystem::create_directories(FLAGS_service_plugin_data_root);
        }
    }

    std::string PluginManager::make_plugin_file_name(const std::string &name, const turbo::ModuleVersion &v) {
        return turbo::Format("lib{}.so.{}", name, v.to_string());
    }

    void PluginManager::create_plugin(const ::EA::proto::FileManageRequest &request, braft::Closure *done) {
        auto &create_req = const_cast<EA::proto::CreatePluginRequest &>(request.create_plugin());
        std::string plugin_name = create_req.name();
        auto &plugin_proto_version = const_cast<EA::proto::Version &>(create_req.version());
        turbo::ModuleVersion plugin_version(plugin_proto_version.major(), plugin_proto_version.minor(),
                                            plugin_proto_version.patch());

        std::unique_lock l(_meta_lock);
        /// if the plugin already upload finish, response false;
        if(get_ready_plugin_ptr(plugin_name, plugin_version)) {
            TLOG_WARN("create plugin already commit request:{}", request.ShortDebugString());
            SET_DONE_AND_RESPONSE(done, proto::INTERNAL_ERROR, "plugin already commit")
            return;
        }
        /// if the plugin already removed, response false;
        if(get_remove_plugin_ptr(plugin_name, plugin_version)) {
            TLOG_WARN("create plugin version removed request:{}", request.ShortDebugString());
            SET_DONE_AND_RESPONSE(done, proto::INTERNAL_ERROR, "plugin already removed")
            return;
        }

        /// if the plugin already removed, response false;
        if(get_uploading_plugin_ptr(plugin_name, plugin_version)) {
            TLOG_WARN("create plugin failed already exists request:{}", request.ShortDebugString());
            SET_DONE_AND_RESPONSE(done, proto::INTERNAL_ERROR, "plugin already exists")
            return;
        }


        if (_uploading_plugin_map.find(plugin_name) == _uploading_plugin_map.end()) {
            _uploading_plugin_map[plugin_name] = std::make_shared<Plugin>(plugin_name);
        }
        auto it = _uploading_plugin_map.find(plugin_name);
        auto &entities = it->second->entities;

        if (entities.find(plugin_version) != entities.end()) {
            TLOG_WARN("create plugin version exists request:{}", request.ShortDebugString());
            SET_DONE_AND_RESPONSE(done, proto::INTERNAL_ERROR, "plugin already exists")
            return;
        }

        entities[plugin_version] = std::shared_ptr<PluginEntity>();
        SET_DONE_AND_RESPONSE(done, proto::SUCCESS, "success");
    }

    void PluginManager::upload_plugin(const ::EA::proto::FileManageRequest &request,  braft::Closure *done) {
        auto &upload_req = const_cast<EA::proto::UploadPluginRequest &>(request.upload_plugin());
        std::string plugin_name = upload_req.name();
        auto &plugin_proto_version = const_cast<EA::proto::Version &>(upload_req.version());
        turbo::ModuleVersion plugin_version(plugin_proto_version.major(), plugin_proto_version.minor(),
                                            plugin_proto_version.patch());

        std::unique_lock l(_meta_lock);
        auto r = get_uploading_plugin_ptr(plugin_name, plugin_version);
        if(r == nullptr) {
            TLOG_ERROR("upload plugin failed for plugin not create, request: {}", request.ShortDebugString());
            SET_DONE_AND_RESPONSE(done, proto::INPUT_PARAM_ERROR, "plugin not create")
            return;
        }
        ssize_t nw = full_pwrite(r->fd, upload_req.content().data(), upload_req.content().size(), upload_req.offset());
        if(nw < 0) {
            r->close();
            TLOG_ERROR("upload plugin failed to pwrite, request: {}", request.ShortDebugString());
            SET_DONE_AND_RESPONSE(done, proto::INTERNAL_ERROR, "plugin not create")
            return;
        }
        if (upload_req.offset() + (int64_t) (upload_req.content().size()) == upload_req.size()) {
            fsync(r->fd);
            int ret = ftruncate(r->fd, upload_req.size());
            int64_t nszie = md5_sum_file(r->path, r->cksm);
            if (nszie < 0) {
                TLOG_ERROR("Fail to cksm model: {} , ftruncate ret: {}", r->path, ret);
                SET_DONE_AND_RESPONSE(done, proto::INTERNAL_ERROR, "Fail to cksm plugin")
                return;
            }
            TLOG_INFO("{} persist at path: {} has computed size:{} and received size:{} has computed cksm:{} and received cksm:",
                      plugin_name, r->path, r->size, nszie, upload_req.size(), r->cksm, upload_req.cksm());
            if (upload_req.cksm() != r->cksm) {
                TLOG_ERROR("invalid md5 cksm, while computed cksm: {} , expected cksm: {}",r->cksm , upload_req.cksm());
                SET_DONE_AND_RESPONSE(done, proto::INTERNAL_ERROR, "Fail to cksm plugin")
                return;
            } else {
                r->size = upload_req.size();
            }
            r->finish = true;
            make_plugin_ready(plugin_name, plugin_version);
        }
        SET_DONE_AND_RESPONSE(done, proto::SUCCESS, "success");
    }

    void PluginManager::remove_plugin(const ::EA::proto::FileManageRequest &request, braft::Closure *done) {
        auto &remove_plugin = const_cast<EA::proto::RemovePluginRequest &>(request.remove_plugin());
        std::string plugin_name = remove_plugin.name();
        std::unique_lock l(_meta_lock);
        if(remove_plugin.has_version()) {
            auto &plugin_proto_version = const_cast<EA::proto::Version &>(remove_plugin.version());
            turbo::ModuleVersion plugin_version(plugin_proto_version.major(), plugin_proto_version.minor(),
                                                plugin_proto_version.patch());
            remove_signal(plugin_name, plugin_version, done);
        } else {
            remove_all(plugin_name, done);
        }
    }

    void PluginManager::remove_signal(const std::string &name, const turbo::ModuleVersion &version, braft::Closure *done) {

         /// from ready
        auto it = _plugin_map.find(name);
        if (it != _plugin_map.end()) {
            auto eit = it->second->entities.find(version);
            if(eit != it->second->entities.end()) {
                add_plugin(_removed_plugin_map, name);
            }
            return;
        }
    }

    void PluginManager::remove_all(const std::string &name, braft::Closure *done) {

    }

    std::shared_ptr<PluginEntity> PluginManager::get_ready_plugin_ptr(const std::string &name, const turbo::ModuleVersion &version) {
        auto it = _plugin_map.find(name);
        if (it == _plugin_map.end()) {
            return nullptr;
        }
        auto eit = it->second->entities.find(version);
        if(eit == it->second->entities.end()) {
            return nullptr;
        }
        return eit->second;
    }

    std::shared_ptr<PluginEntity> PluginManager::get_uploading_plugin_ptr(const std::string &name, const turbo::ModuleVersion &version) {
        auto it = _uploading_plugin_map.find(name);
        if (it == _uploading_plugin_map.end()) {
            return nullptr;
        }
        auto eit = it->second->entities.find(version);
        if(eit == it->second->entities.end()) {
            return nullptr;
        }
        return eit->second;
    }

    std::shared_ptr<PluginEntity> PluginManager::get_remove_plugin_ptr(const std::string &name, const turbo::ModuleVersion &version) {
        auto it = _removed_plugin_map.find(name);
        if (it == _removed_plugin_map.end()) {
            return nullptr;
        }
        auto eit = it->second->entities.find(version);
        if(eit == it->second->entities.end()) {
            return nullptr;
        }
        return eit->second;
    }

    bool PluginManager::make_plugin_ready(const std::string &name, const turbo::ModuleVersion &version) {
        auto it = _uploading_plugin_map.find(name);
        if (it == _uploading_plugin_map.end()) {
            TLOG_ERROR("fatal error for commit plugin no plugin");
            return false;
        }
        auto eit = it->second->entities.find(version);
        if(eit == it->second->entities.end()) {
            TLOG_ERROR("fatal error for commit plugin no version");
            return false;
        }

        if (_plugin_map.find(name) == _plugin_map.end()) {
            _plugin_map[name] = std::make_shared<Plugin>(name);
        }
        auto rit = _plugin_map.find(name);
        if(rit->second->entities.find(version) != rit->second->entities.end()) {
            TLOG_ERROR("fatal error for commit plugin no version");
            return false;
        }
        rit->second->entities[version] = eit->second;

        /// remove uploading map;
        it->second->entities.erase(version);
        if(it->second->entities.empty()) {
            _uploading_plugin_map.erase(name);
        }
        return true;
    }
    void PluginManager::add_plugin(PluginMap &map, const std::string &name) {
        if(map.find(name) == map.end()) {
            map[name] = std::make_shared<Plugin>(name);
        }
    }
}  // namespace EA
*/