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

#include "elasticann/ops/plugin_manager.h"
#include "elasticann/ops/constants.h"
#include "elasticann/ops/service_rocksdb.h"
#include "elasticann/ops/service_state_machine.h"
#include "elasticann/ops/file_util.h"
#include "turbo/files/utility.h"
#include "braft/raft.h"

namespace EA {
    void PluginManager::create_plugin(const ::EA::proto::OpsServiceRequest &request, braft::Closure *done) {
        if (!request.has_plugin()) {
            SERVICE_SET_DONE_AND_RESPONSE(done, proto::INPUT_PARAM_ERROR, "empty plugin request");
            return;
        }
        auto &create_request = request.plugin().plugin();
        auto &name = create_request.name();
        turbo::ModuleVersion version(create_request.version().major(), create_request.version().minor(),
                                     create_request.version().patch());
        {
            BAIDU_SCOPED_LOCK(_tombstone_plugin_mutex);
            auto tit = _tombstone_plugins.find(name);
            // do not rewrite.
            if ((tit != _tombstone_plugins.end()) && (tit->second.find(version) != tit->second.end())) {
                /// already exists
                TLOG_INFO("plugin :{} version: {} is tombstone", name, version.to_string());
                SERVICE_SET_DONE_AND_RESPONSE(done, proto::INPUT_PARAM_ERROR, "plugin already removed");
                return;
            }
        }

        BAIDU_SCOPED_LOCK(_plugin_mutex);
        if (_plugins.find(name) == _plugins.end()) {
            _plugins[name] = std::map<turbo::ModuleVersion, EA::proto::PluginEntiry>();
        }
        auto it = _plugins.find(name);
        // do not rewrite.
        if (it->second.find(version) != it->second.end()) {
            /// already exists
            TLOG_INFO("plugin :{} version: {} exist", name, version.to_string());
            SERVICE_SET_DONE_AND_RESPONSE(done, proto::INPUT_PARAM_ERROR, "plugin already exist");
            return;
        }
        if (!it->second.empty() && it->second.rbegin()->first >= version) {
            /// Version numbers must increase monotonically
            TLOG_INFO("plugin :{} version: {} must be larger than current:{}", name, version.to_string(),
                      it->second.rbegin()->first.to_string());
            SERVICE_SET_DONE_AND_RESPONSE(done, proto::INPUT_PARAM_ERROR,
                                          "Version numbers must increase monotonically");
            return;
        }
        std::string rocks_key = make_plugin_key(name, version);
        std::string rocks_value;
        EA::proto::PluginEntiry entity;
        auto st = transfer_info_to_entity(&create_request, &entity);
        if (!st.ok()) {
            SERVICE_SET_DONE_AND_RESPONSE(done, proto::PARSE_TO_PB_FAIL, std::string(st.message()));
            return;
        }
        if (!entity.SerializeToString(&rocks_value)) {
            SERVICE_SET_DONE_AND_RESPONSE(done, proto::PARSE_TO_PB_FAIL, "serializeToArray fail");
            return;
        }

        int ret = ServiceRocksdb::get_instance()->put_meta_info(rocks_key, rocks_value);
        if (ret < 0) {
            SERVICE_SET_DONE_AND_RESPONSE(done, proto::INTERNAL_ERROR, "write db fail");
            return;
        }
        it->second[version] = entity;
        TLOG_INFO("plugin :{} version: {} create", name, version.to_string());
        SERVICE_SET_DONE_AND_RESPONSE(done, proto::SUCCESS, "success");
    }

    void PluginManager::upload_plugin(const ::EA::proto::OpsServiceRequest &request, braft::Closure *done) {
        /// check valid
        if (!request.has_plugin()) {
            SERVICE_SET_DONE_AND_RESPONSE(done, proto::INPUT_PARAM_ERROR, "miss field plugin plugin");
            return;
        }
        auto &upload_request = request.plugin();
        if (!request.plugin().has_offset()) {
            SERVICE_SET_DONE_AND_RESPONSE(done, proto::INPUT_PARAM_ERROR, "miss field plugin offset");
            return;
        }

        if (!request.plugin().has_content()) {
            SERVICE_SET_DONE_AND_RESPONSE(done, proto::INPUT_PARAM_ERROR, "miss field plugin content");
            return;
        }
        if (request.plugin().content().empty()) {
            SERVICE_SET_DONE_AND_RESPONSE(done, proto::INPUT_PARAM_ERROR, "empty plugin content");
            return;
        }

        if (!upload_request.plugin().has_version()) {
            SERVICE_SET_DONE_AND_RESPONSE(done, proto::INPUT_PARAM_ERROR, "miss field plugin version");
            return;
        }

        auto &name = upload_request.plugin().name();

        BAIDU_SCOPED_LOCK(_plugin_mutex);
        auto it = _plugins.find(name);
        if (it == _plugins.end()) {
            SERVICE_SET_DONE_AND_RESPONSE(done, proto::PARSE_TO_PB_FAIL, "plugin not exist");
            return;
        }
        turbo::ModuleVersion version(upload_request.plugin().version().major(),
                                     upload_request.plugin().version().minor(),
                                     upload_request.plugin().version().patch());
        auto pit = it->second.find(version);
        if (pit == it->second.end()) {
            /// not exists
            TLOG_INFO("plugin :{} version: {} not exist", name, version.to_string());
            SERVICE_SET_DONE_AND_RESPONSE(done, proto::INPUT_PARAM_ERROR, "plugin not exist");
        }

        std::string file_path = make_plugin_store_path(name, version, pit->second.platform());
        int fd = ::open(file_path.c_str(), O_RDWR | O_CREAT, 0644);
        if (fd < 0) {
            TLOG_WARN("upload plugin :{} version: {} open file error", name, version.to_string());
            SERVICE_SET_DONE_AND_RESPONSE(done, proto::INTERNAL_ERROR, "open file error");
        }
        ssize_t nw = full_pwrite(fd, upload_request.content().data(), upload_request.content().size(),
                                 upload_request.offset());
        if (nw < 0) {
            TLOG_WARN("upload plugin :{} version: {} open file error", name, version.to_string());
            SERVICE_SET_DONE_AND_RESPONSE(done, proto::INTERNAL_ERROR, "open file error");
        }

        pit->second.set_upload_size(upload_request.offset() + nw);
        ::fsync(fd);
        ftruncate(fd, pit->second.upload_size());
        if (pit->second.upload_size() == pit->second.size()) {
            pit->second.set_finish(true);
        }
        if (pit->second.finish()) {
            /// check sum
            int64_t nszie;
            auto cksm = turbo::FileUtility::md5_sum_file(file_path, &nszie);
            if (!cksm.ok()) {
                TLOG_WARN("upload plugin :{} version: {} check md5 fail", name, version.to_string());
                SERVICE_SET_DONE_AND_RESPONSE(done, proto::INTERNAL_ERROR, "check md5 fail");
            }
            if (cksm.value() != pit->second.cksm()) {
                TLOG_WARN("upload plugin :{} version: {} check md5 fail, expect: {} get: {}", name, version.to_string(),
                          pit->second.cksm(), cksm.value());
                SERVICE_SET_DONE_AND_RESPONSE(done, proto::INTERNAL_ERROR, "md5 not match");
            }
        }
        ::close(fd);
        /// persist
        std::string rocks_key = make_plugin_key(name, version);
        std::string rocks_value;
        if (!pit->second.SerializeToString(&rocks_value)) {
            SERVICE_SET_DONE_AND_RESPONSE(done, proto::PARSE_TO_PB_FAIL, "serializeToArray fail");
            return;
        }

        int ret = ServiceRocksdb::get_instance()->put_meta_info(rocks_key, rocks_value);
        if (ret < 0) {
            SERVICE_SET_DONE_AND_RESPONSE(done, proto::INTERNAL_ERROR, "write db fail");
            return;
        }
        SERVICE_SET_DONE_AND_RESPONSE(done, proto::SUCCESS, "success");
    }

    void PluginManager::remove_plugin(const ::EA::proto::OpsServiceRequest &request, braft::Closure *done) {
        if (!request.has_plugin()) {
            SERVICE_SET_DONE_AND_RESPONSE(done, proto::INPUT_PARAM_ERROR, "empty plugin request");
            return;
        }
        auto &remove_request = request.plugin().plugin();
        auto &name = remove_request.name();
        bool remove_signal = remove_request.has_version();
        BAIDU_SCOPED_LOCK(_plugin_mutex);
        if (!remove_signal) {
            remove_plugin_all(request, done);
            return;
        }
        auto it = _plugins.find(name);
        if (it == _plugins.end()) {
            SERVICE_SET_DONE_AND_RESPONSE(done, proto::PARSE_TO_PB_FAIL, "plugin not exist");
            return;
        }
        turbo::ModuleVersion version(remove_request.version().major(), remove_request.version().minor(),
                                     remove_request.version().patch());
        auto pit = it->second.find(version);
        if (pit == it->second.end()) {
            /// not exists
            TLOG_INFO("plugin :{} version: {} not exist", name, version.to_string());
            SERVICE_SET_DONE_AND_RESPONSE(done, proto::INPUT_PARAM_ERROR, "plugin not exist");
            return;
        }

        /// mark move to tombstone and write to rocksdb
        std::string rocks_key = make_plugin_key(name, version);
        std::string rocks_value;
        pit->second.set_tombstone(true);
        if (!pit->second.SerializeToString(&rocks_value)) {
            SERVICE_SET_DONE_AND_RESPONSE(done, proto::PARSE_TO_PB_FAIL, "serializeToArray fail");
            return;
        }

        int ret = ServiceRocksdb::get_instance()->put_meta_info(rocks_key, rocks_value);
        if (ret < 0) {
            SERVICE_SET_DONE_AND_RESPONSE(done, proto::INTERNAL_ERROR, "delete from db fail");
            return;
        }

        /// update memory
        {
            /// move to tombstone
            BAIDU_SCOPED_LOCK(_tombstone_plugin_mutex);
            if (_tombstone_plugins.find(name) == _tombstone_plugins.end()) {
                _tombstone_plugins[name] = std::map<turbo::ModuleVersion, EA::proto::PluginEntiry>();
            }
            _tombstone_plugins[name][version] = pit->second;
        }
        /// erase from plugins
        it->second.erase(version);
        /// if no version under plugin, remove it
        if (it->second.empty()) {
            _plugins.erase(name);
        }
        SERVICE_SET_DONE_AND_RESPONSE(done, proto::SUCCESS, "success");
    }

    void PluginManager::remove_tombstone_plugin(const ::EA::proto::OpsServiceRequest &request, braft::Closure *done) {
        if (!request.has_plugin()) {
            SERVICE_SET_DONE_AND_RESPONSE(done, proto::INPUT_PARAM_ERROR, "empty plugin request");
            return;
        }
        auto &remove_request = request.plugin().plugin();
        auto &name = remove_request.name();
        bool remove_signal = remove_request.has_version();
        BAIDU_SCOPED_LOCK(_tombstone_plugin_mutex);
        if (!remove_signal) {
            remove_plugin_all(request, done);
            return;
        }
        auto it = _tombstone_plugins.find(name);
        if (it == _tombstone_plugins.end()) {
            SERVICE_SET_DONE_AND_RESPONSE(done, proto::PARSE_TO_PB_FAIL, "plugin not exist");
            return;
        }
        turbo::ModuleVersion version(remove_request.version().major(), remove_request.version().minor(),
                                     remove_request.version().patch());
        auto vit = it->second.find(version);
        if (vit == it->second.end()) {
            /// not exists
            TLOG_INFO("plugin :{} version: {} not exist", name, version.to_string());
            SERVICE_SET_DONE_AND_RESPONSE(done, proto::INPUT_PARAM_ERROR, "plugin not exist");
            return;
        }

        /// mark move to tombstone and write to rocksdb
        std::string rocks_key = make_plugin_key(name, version);

        std::string file_path = make_plugin_store_path(name, vit->first, vit->second.platform());

        int ret = ServiceRocksdb::get_instance()->delete_meta_info(std::vector{rocks_key});
        if (ret < 0) {
            SERVICE_SET_DONE_AND_RESPONSE(done, proto::INTERNAL_ERROR, "delete from db fail");
            return;
        }

        /// erase from plugins
        it->second.erase(version);
        /// if no version under plugin, remove it
        if (it->second.empty()) {
            _plugins.erase(name);
        }
        /// remove plugin file
        std::error_code ec;
        if (turbo::filesystem::exists(file_path, ec)) {
            turbo::filesystem::remove(file_path, ec);
        }
        SERVICE_SET_DONE_AND_RESPONSE(done, proto::SUCCESS, "success");
    }

    void PluginManager::remove_plugin_all(const ::EA::proto::OpsServiceRequest &request, braft::Closure *done) {
        auto &remove_request = request.plugin().plugin();
        auto &name = remove_request.name();
        auto it = _plugins.find(name);
        if (it == _plugins.end()) {
            SERVICE_SET_DONE_AND_RESPONSE(done, proto::PARSE_TO_PB_FAIL, "plugin not exist");
            return;
        }
        std::vector<std::string> keys;
        std::vector<std::string> values;

        for (auto vit = it->second.begin(); vit != it->second.end(); ++vit) {
            std::string key = make_plugin_key(name, vit->first);
            std::string value;
            vit->second.set_tombstone(true);
            if (!vit->second.SerializeToString(&value)) {
                SERVICE_SET_DONE_AND_RESPONSE(done, proto::PARSE_TO_PB_FAIL, "serializeToArray fail");
                return;
            }
            keys.push_back(key);
            values.push_back(value);
        }

        int ret = ServiceRocksdb::get_instance()->put_meta_info(keys, values);
        if (ret < 0) {
            SERVICE_SET_DONE_AND_RESPONSE(done, proto::INTERNAL_ERROR, "delete from db fail");
            return;
        }
        /// update memory
        {
            /// move to tombstone
            BAIDU_SCOPED_LOCK(_tombstone_plugin_mutex);
            if (_tombstone_plugins.find(name) == _tombstone_plugins.end()) {
                _tombstone_plugins[name] = std::map<turbo::ModuleVersion, EA::proto::PluginEntiry>();
            }
            auto tomb_it = _tombstone_plugins.find(name);
            for (auto vit = it->second.begin(); vit != it->second.end(); ++vit) {
                tomb_it->second[vit->first] = vit->second;
            }
        }
        _plugins.erase(name);
        SERVICE_SET_DONE_AND_RESPONSE(done, proto::SUCCESS, "success");
    }

    void
    PluginManager::remove_tombstone_plugin_all(const ::EA::proto::OpsServiceRequest &request, braft::Closure *done) {
        auto &remove_request = request.plugin().plugin();
        auto &name = remove_request.name();
        auto it = _plugins.find(name);
        if (it == _plugins.end()) {
            SERVICE_SET_DONE_AND_RESPONSE(done, proto::PARSE_TO_PB_FAIL, "plugin not exist");
            return;
        }
        std::vector<std::string> keys;
        std::vector<std::string> paths;
        for (auto vit = it->second.begin(); vit != it->second.end(); ++vit) {
            std::string key = make_plugin_key(name, vit->first);

            std::string file_path = make_plugin_store_path(name, vit->first, vit->second.platform());
            keys.push_back(key);
            paths.push_back(file_path);
        }

        int ret = ServiceRocksdb::get_instance()->delete_meta_info(keys);
        if (ret < 0) {
            SERVICE_SET_DONE_AND_RESPONSE(done, proto::INTERNAL_ERROR, "delete from db fail");
            return;
        }
        /// update memory
        _tombstone_plugins.erase(name);
        /// remove plugin files
        std::error_code ec;
        for (auto &f: paths) {
            if (turbo::filesystem::exists(f, ec)) {
                turbo::filesystem::remove(f, ec);
            }
        }
        SERVICE_SET_DONE_AND_RESPONSE(done, proto::SUCCESS, "success");
    }

    void PluginManager::restore_plugin(const ::EA::proto::OpsServiceRequest &request, braft::Closure *done) {
        if (!request.has_plugin()) {
            SERVICE_SET_DONE_AND_RESPONSE(done, proto::INPUT_PARAM_ERROR, "empty plugin request");
            return;
        }
        auto &restore_request = request.plugin().plugin();
        auto &name = restore_request.name();
        bool remove_signal = restore_request.has_version();
        BAIDU_SCOPED_LOCK(_tombstone_plugin_mutex);
        if (!remove_signal) {
            restore_plugin_all(request, done);
            return;
        }
        auto it = _tombstone_plugins.find(name);
        if (it == _tombstone_plugins.end()) {
            SERVICE_SET_DONE_AND_RESPONSE(done, proto::PARSE_TO_PB_FAIL, "plugin not exist");
            return;
        }
        turbo::ModuleVersion version(restore_request.version().major(), restore_request.version().minor(),
                                     restore_request.version().patch());
        auto pit = it->second.find(version);
        if (pit == it->second.end()) {
            /// not exists
            TLOG_INFO("plugin :{} version: {} not exist", name, version.to_string());
            SERVICE_SET_DONE_AND_RESPONSE(done, proto::INPUT_PARAM_ERROR, "plugin not exist");
        }

        /// mark move to tombstone and write to rocksdb
        std::string rocks_key = make_plugin_key(name, version);
        std::string rocks_value;
        pit->second.set_tombstone(false);
        if (!pit->second.SerializeToString(&rocks_value)) {
            SERVICE_SET_DONE_AND_RESPONSE(done, proto::PARSE_TO_PB_FAIL, "serializeToArray fail");
            return;
        }

        int ret = ServiceRocksdb::get_instance()->put_meta_info(rocks_key, rocks_value);
        if (ret < 0) {
            SERVICE_SET_DONE_AND_RESPONSE(done, proto::INTERNAL_ERROR, "write from db fail");
            return;
        }

        /// update memory
        {
            /// move to normal
            BAIDU_SCOPED_LOCK(_plugin_mutex);
            if (_plugins.find(name) == _plugins.end()) {
                _plugins[name] = std::map<turbo::ModuleVersion, EA::proto::PluginEntiry>();
            }
            _plugins[name][version] = pit->second;
        }
        /// erase from plugins
        it->second.erase(version);
        /// if no version under plugin, remove it
        if (it->second.empty()) {
            _plugins.erase(name);
        }
        SERVICE_SET_DONE_AND_RESPONSE(done, proto::SUCCESS, "success");
    }

    void PluginManager::restore_plugin_all(const ::EA::proto::OpsServiceRequest &request, braft::Closure *done) {
        auto &restore_request = request.plugin().plugin();
        auto &name = restore_request.name();
        auto it = _tombstone_plugins.find(name);
        if (it == _tombstone_plugins.end()) {
            SERVICE_SET_DONE_AND_RESPONSE(done, proto::PARSE_TO_PB_FAIL, "plugin not exist");
            return;
        }
        std::vector<std::string> keys;
        std::vector<std::string> values;


        for (auto vit = it->second.begin(); vit != it->second.end(); ++vit) {
            std::string key = make_plugin_key(name, vit->first);
            std::string value;
            vit->second.set_tombstone(false);
            if (!vit->second.SerializeToString(&value)) {
                SERVICE_SET_DONE_AND_RESPONSE(done, proto::PARSE_TO_PB_FAIL, "serializeToArray fail");
                return;
            }
            keys.push_back(key);
            values.push_back(value);
        }

        int ret = ServiceRocksdb::get_instance()->put_meta_info(keys, values);
        if (ret < 0) {
            SERVICE_SET_DONE_AND_RESPONSE(done, proto::INTERNAL_ERROR, "delete from db fail");
            return;
        }
        /// update memory
        {
            /// move to tombstone
            BAIDU_SCOPED_LOCK(_plugin_mutex);
            if (_plugins.find(name) == _plugins.end()) {
                _plugins[name] = std::map<turbo::ModuleVersion, EA::proto::PluginEntiry>();
            }
            auto normal_it = _plugins.find(name);
            for (auto vit = it->second.begin(); vit != it->second.end(); ++vit) {
                normal_it->second[vit->first] = vit->second;
            }
        }
        _tombstone_plugins.erase(name);
        SERVICE_SET_DONE_AND_RESPONSE(done, proto::SUCCESS, "success");
    }

    int PluginManager::load_snapshot() {
        BAIDU_SCOPED_LOCK(_plugin_mutex);
        BAIDU_SCOPED_LOCK(_tombstone_plugin_mutex);
        TLOG_INFO("start to load plugins snapshot");
        _plugins.clear();
        _tombstone_plugins.clear();
        std::string plugin_prefix = ServiceConstants::PLUGIN_IDENTIFY;
        rocksdb::ReadOptions read_options;
        read_options.prefix_same_as_start = true;
        read_options.total_order_seek = false;
        RocksWrapper *db = RocksWrapper::get_instance();
        std::unique_ptr<rocksdb::Iterator> iter(
                db->new_iterator(read_options, db->get_service_handle()));
        iter->Seek(plugin_prefix);
        for (; iter->Valid(); iter->Next()) {
            if (load_plugin_snapshot(iter->value().ToString()) != 0) {
                return -1;
            }
        }
        TLOG_INFO("load plugins snapshot done");
        return 0;
    }

    int PluginManager::load_snapshot_file(const std::string &file_path) {
        auto fname = turbo::filesystem::path(file_path).filename();
        auto local_path = turbo::filesystem::path(FLAGS_service_plugin_data_root) / fname;
        std::error_code ec;
        if (!turbo::filesystem::exists(local_path, ec)) {
            if (ec) {
                TLOG_ERROR("{}", ec.message());
                return -1;
            }
            turbo::filesystem::copy(file_path, local_path, ec);
            if (ec) {
                TLOG_ERROR("{}", ec.message());
                return -1;
            }
            return 0;
        }
        auto s_size = turbo::filesystem::file_size(file_path, ec);
        if (ec) {
            TLOG_ERROR("{}", ec.message());
            return -1;
        }
        auto l_size = turbo::filesystem::file_size(local_path, ec);
        if (ec) {
            TLOG_ERROR("{}", ec.message());
            return -1;
        }
        if (l_size != s_size) {
            turbo::filesystem::remove(local_path, ec);
            turbo::filesystem::copy(file_path, local_path, ec);
            if (ec) {
                TLOG_ERROR("{}", ec.message());
                return -1;
            }
        }
        return 0;
    }

    int PluginManager::load_plugin_snapshot(const std::string &value) {
        proto::PluginEntiry plugin_pb;
        if (!plugin_pb.ParseFromString(value)) {
            TLOG_ERROR("parse from pb fail when load database snapshot, key:{}", value);
            return -1;
        }
        if (plugin_pb.tombstone()) {
            if (_tombstone_plugins.find(plugin_pb.name()) == _tombstone_plugins.end()) {
                _tombstone_plugins[plugin_pb.name()] = std::map<turbo::ModuleVersion, EA::proto::PluginEntiry>();
            }
            auto it = _tombstone_plugins.find(plugin_pb.name());
            turbo::ModuleVersion version(plugin_pb.version().major(), plugin_pb.version().minor(),
                                         plugin_pb.version().patch());
            it->second[version] = plugin_pb;
        } else {
            if (_plugins.find(plugin_pb.name()) == _plugins.end()) {
                _plugins[plugin_pb.name()] = std::map<turbo::ModuleVersion, EA::proto::PluginEntiry>();
            }
            auto it = _plugins.find(plugin_pb.name());
            turbo::ModuleVersion version(plugin_pb.version().major(), plugin_pb.version().minor(),
                                         plugin_pb.version().patch());
            it->second[version] = plugin_pb;
        }

        return 0;
    }

    int PluginManager::save_snapshot(const std::string &base_dir, const std::string &prefix,
                                     std::vector<std::string> &files) {
        //
        std::error_code ec;
        {
            BAIDU_SCOPED_LOCK(_plugin_mutex);
            for (auto it = _plugins.begin(); it != _plugins.end(); ++it) {
                for (auto pit = it->second.begin(); pit != it->second.end(); ++pit) {
                    auto filename = make_plugin_filename(pit->second.name(), pit->first, pit->second.platform());
                    std::string file_path = turbo::Format("{}/{}", prefix, filename);
                    std::string target = base_dir + file_path;
                    std::string source = turbo::Format("{}/{}", FLAGS_service_plugin_data_root, filename);

                    if (!turbo::filesystem::exists(source, ec)) {
                        continue;
                    }
                    turbo::filesystem::create_hard_link(source, target, ec);
                    if (ec) {
                        TLOG_ERROR("plugin snapshot error: {}", ec.message());
                        return -1;
                    }
                    files.push_back(file_path);
                }
            }
        }
        {
            BAIDU_SCOPED_LOCK(_tombstone_plugin_mutex);
            for (auto it = _tombstone_plugins.begin(); it != _tombstone_plugins.end(); ++it) {
                for (auto pit = it->second.begin(); pit != it->second.end(); ++pit) {
                    auto filename = make_plugin_filename(pit->second.name(), pit->first, pit->second.platform());
                    std::string file_path = turbo::Format("{}/{}", prefix, filename);
                    std::string target = base_dir + file_path;
                    std::string source = turbo::Format("{}/{}", FLAGS_service_plugin_data_root, filename);

                    if (!turbo::filesystem::exists(source, ec)) {
                        continue;
                    }
                    turbo::filesystem::create_hard_link(source, target, ec);
                    if (ec) {
                        TLOG_ERROR("plugin snapshot error: {}", ec.message());
                        return -1;
                    }
                    files.push_back(file_path);
                }
            }
        }
        return 0;
    }

    std::string PluginManager::make_plugin_key(const std::string &name, const turbo::ModuleVersion &version) {
        return ServiceConstants::PLUGIN_IDENTIFY + name + version.to_string();
    }

    turbo::Status
    PluginManager::transfer_info_to_entity(const EA::proto::PluginInfo *info, EA::proto::PluginEntiry *entity) {
        entity->set_upload_size(0);
        entity->set_finish(false);
        entity->set_tombstone(false);
        entity->set_name(info->name());
        entity->set_time(info->time());
        entity->set_platform(info->platform());
        entity->set_size(info->size());
        if (!info->has_cksm()) {
            return turbo::InvalidArgumentError("no chsm");
        }
        entity->set_cksm(info->cksm());
        if (!info->has_time()) {
            return turbo::InvalidArgumentError("no time");
        }
        entity->set_time(info->time());
        if (!info->has_version()) {
            return turbo::InvalidArgumentError("no version");
        }
        *(entity->mutable_version()) = info->version();
        return turbo::OkStatus();
    }

    void PluginManager::transfer_entity_to_info(const EA::proto::PluginEntiry *entity, EA::proto::PluginInfo *info) {
        info->set_upload_size(entity->upload_size());
        info->set_finish(entity->finish());
        info->set_tombstone(entity->tombstone());
        info->set_name(entity->name());
        info->set_time(entity->time());
        info->set_platform(entity->platform());
        info->set_size(entity->size());
        info->set_cksm(entity->cksm());
        info->set_time(entity->time());
        *(info->mutable_version()) = entity->version();
    }

    std::string PluginManager::make_plugin_filename(const std::string &name, const turbo::ModuleVersion &version,
                                                    EA::proto::Platform platform) {
        if (platform == EA::proto::PF_lINUX) {
            return turbo::Format("lib{}.so.{}", name, version.to_string());
        } else if (platform == EA::proto::PF_OSX) {
            return turbo::Format("lib{}.{}.dylib", name, version.to_string());
        } else {
            return turbo::Format("lib{}.{}.dll", name, version.to_string());
        }

    }

    std::string PluginManager::make_plugin_store_path(const std::string &name, const turbo::ModuleVersion &version,
                                                      EA::proto::Platform platform) {
        if (platform == EA::proto::PF_lINUX) {
            return turbo::Format("{}/lib{}.so.{}", FLAGS_service_plugin_data_root, name, version.to_string());
        } else if (platform == EA::proto::PF_OSX) {
            return turbo::Format("{}/lib{}.{}.dylib", FLAGS_service_plugin_data_root, name, version.to_string());
        } else {
            return turbo::Format("{}/lib{}.{}.dll", FLAGS_service_plugin_data_root, name, version.to_string());
        }
    }

}  // namespace EA