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
#include "elasticann/ops/config_manager.h"
#include "elasticann/ops/constants.h"
#include "elasticann/ops/service_rocksdb.h"
#include "elasticann/ops/service_state_machine.h"

namespace EA {

    void ConfigManager::create_config(const ::EA::proto::OpsServiceRequest &request, braft::Closure *done) {
        auto &create_request = request.config();
        auto &name = create_request.name();
        turbo::ModuleVersion version(create_request.version().major(), create_request.version().minor(),
                                     create_request.version().patch());

        if (_configs.find(name) == _configs.end()) {
            _configs[name] = std::map<turbo::ModuleVersion, EA::proto::ConfigEntity>();
        }
        auto it = _configs.find(name);
        if (it->second.find(version) != it->second.end()) {
            /// already exists
            TLOG_INFO("config :{} version: {} exist", name, version.to_string());
            SERVICE_SET_DONE_AND_RESPONSE(done, proto::INPUT_PARAM_ERROR, "config already exist");
            return;
        }
        std::string rocks_key = make_config_key(name, version);
        std::string rocks_value;
        if (!create_request.SerializeToString(&rocks_value)) {
            SERVICE_SET_DONE_AND_RESPONSE(done, proto::PARSE_TO_PB_FAIL, "serializeToArray fail");
            return;
        }

        int ret = ServiceRocksdb::get_instance()->put_meta_info(rocks_key, rocks_value);
        if (ret < 0) {
            SERVICE_SET_DONE_AND_RESPONSE(done, proto::INTERNAL_ERROR, "write db fail");
            return;
        }
        it->second[version] = create_request;
        TLOG_INFO("config :{} version: {} create", name, version.to_string());
        SERVICE_SET_DONE_AND_RESPONSE(done, proto::SUCCESS, "success");
    }


    void ConfigManager::remove_config(const ::EA::proto::OpsServiceRequest &request, braft::Closure *done) {
        auto &remove_request = request.config();
        auto &name = remove_request.name();
        bool remove_signal = remove_request.has_version();
        if (!remove_signal) {
            remove_config_all(request, done);
            return;
        }
        auto it = _configs.find(name);
        if (it == _configs.end()) {
            SERVICE_SET_DONE_AND_RESPONSE(done, proto::PARSE_TO_PB_FAIL, "config not exist");
            return;
        }
        turbo::ModuleVersion version(remove_request.version().major(), remove_request.version().minor(),
                                     remove_request.version().patch());

        if (it->second.find(version) == it->second.end()) {
            /// not exists
            TLOG_INFO("config :{} version: {} not exist", name, version.to_string());
            SERVICE_SET_DONE_AND_RESPONSE(done, proto::INPUT_PARAM_ERROR, "config not exist");
        }

        std::string rocks_key = make_config_key(name, version);
        int ret = ServiceRocksdb::get_instance()->delete_meta_info(std::vector{rocks_key});
        if (ret < 0) {
            SERVICE_SET_DONE_AND_RESPONSE(done, proto::INTERNAL_ERROR, "delete from db fail");
            return;
        }
        it->second.erase(version);
        SERVICE_SET_DONE_AND_RESPONSE(done, proto::SUCCESS, "success");
    }

    void ConfigManager::remove_config_all(const ::EA::proto::OpsServiceRequest &request, braft::Closure *done) {
        auto &remove_request = request.config();
        auto &name = remove_request.name();
        auto it = _configs.find(name);
        if (it == _configs.end()) {
            SERVICE_SET_DONE_AND_RESPONSE(done, proto::PARSE_TO_PB_FAIL, "config not exist");
            return;
        }
        std::vector<std::string> del_keys;

        for(auto vit = it->second.begin(); vit != it->second.end(); ++vit) {
            std::string key = make_config_key(name, vit->first);
            del_keys.push_back(key);
        }

        int ret = ServiceRocksdb::get_instance()->delete_meta_info(del_keys);
        if (ret < 0) {
            SERVICE_SET_DONE_AND_RESPONSE(done, proto::INTERNAL_ERROR, "delete from db fail");
            return;
        }
        _configs.erase(name);
        SERVICE_SET_DONE_AND_RESPONSE(done, proto::SUCCESS, "success");
    }

    void ConfigManager::get_config(const ::EA::proto::OpsServiceRequest *request, ::EA::proto::OpsServiceResponse *response) {
        auto &get_request = request->config();
        auto &name = get_request.name();
        if(!get_request.has_version()) {
            //SERVICE_SET_DONE_AND_RESPONSE(done, proto::INPUT_PARAM_ERROR, "no version");
            response->set_errmsg("no version");
            response->set_errcode(proto::INPUT_PARAM_ERROR);
            return;
        }
        auto it = _configs.find(name);
        if (it == _configs.end()) {
            response->set_errmsg("config not exist");
            response->set_errcode(proto::INPUT_PARAM_ERROR);
            return;
        }
        turbo::ModuleVersion version(get_request.version().major(), get_request.version().minor(),
                                     get_request.version().patch());
        auto cit = it->second.find(version);
        if ( cit == it->second.end()) {
            /// not exists
            response->set_errmsg("config not exist");
            response->set_errcode(proto::INPUT_PARAM_ERROR);
            return;
        }

        *response->mutable_config() = cit->second;
        response->set_errmsg("success");
        response->set_errcode(proto::SUCCESS);
    }

    void ConfigManager::list_config(const ::EA::proto::OpsServiceRequest *request, ::EA::proto::OpsServiceResponse *response) {
        response->mutable_config_list()->Reserve(_configs.size());
        for(auto it = _configs.begin(); it != _configs.end(); ++it) {
            response->add_config_list(it->first);
        }
        response->set_errmsg("success");
        response->set_errcode(proto::SUCCESS);
    }

    void ConfigManager::list_config_version(const ::EA::proto::OpsServiceRequest *request, ::EA::proto::OpsServiceResponse *response) {
        auto &get_request = request->config();
        auto &name = get_request.name();
        auto it = _configs.find(name);
        if (it == _configs.end()) {
            response->set_errmsg("config not exist");
            response->set_errcode(proto::INPUT_PARAM_ERROR);
            return;
        }
        response->mutable_versions()->Reserve(it->second.size());
        for(auto vit = it->second.begin(); vit != it->second.end(); ++vit) {
            *(response->add_versions()) = vit->second.version();
        }
        response->set_errmsg("success");
        response->set_errcode(proto::SUCCESS);
    }

    int ConfigManager::load_snapshot() {
        _configs.clear();
        std::string config_prefix = ServiceConstants::CONFIG_IDENTIFY;
        rocksdb::ReadOptions read_options;
        read_options.prefix_same_as_start = true;
        read_options.total_order_seek = false;
        RocksWrapper *db = RocksWrapper::get_instance();
        std::unique_ptr<rocksdb::Iterator> iter(
                db->new_iterator(read_options, db->get_meta_info_handle()));
        iter->Seek(config_prefix);
        for (; iter->Valid(); iter->Next()) {
            proto::ConfigEntity entity;
            if(load_config_snapshot(iter->value().ToString()) != 0) {
                return -1;
            }
        }
        return 0;
    }

    int ConfigManager::load_config_snapshot(const std::string &value) {
        proto::ConfigEntity config_pb;
        if (!config_pb.ParseFromString(value)) {
            TLOG_ERROR("parse from pb fail when load database snapshot, key:{}", value);
            return -1;
        }
        if(_configs.find(config_pb.name()) == _configs.end()) {
            _configs[config_pb.name()] = std::map<turbo::ModuleVersion, EA::proto::ConfigEntity>();
        }
        auto it = _configs.find(config_pb.name());
        turbo::ModuleVersion version(config_pb.version().major(), config_pb.version().minor(),
                                     config_pb.version().patch());
        it->second[version] = config_pb;
        return 0;
    }

    std::string ConfigManager::make_config_key(const std::string &name, const turbo::ModuleVersion &version) {
        return ServiceConstants::CONFIG_IDENTIFY + name + version.to_string();
    }

}  // namespace EA