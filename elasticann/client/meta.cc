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
#include "elasticann/client/meta.h"
#include "elasticann/client/utility.h"
#include "turbo/files/sequential_read_file.h"
#include "json2pb/json_to_pb.h"
#include "json2pb/pb_to_json.h"
#include "elasticann/client/config_info_builder.h"

namespace EA::client {

    turbo::Status MetaClient::init(BaseMessageSender *sender) {
        _sender = sender;
        return turbo::OkStatus();
    }

    turbo::Status MetaClient::load_proto_from_file(const std::string &path, google::protobuf::Message &message) {
        turbo::SequentialReadFile file;
        auto rs = file.open(path);
        if (!rs.ok()) {
            return rs;
        }
        std::string config_data;
        auto rr = file.read(&config_data);
        if (!rr.ok()) {
            return rr.status();
        }
        return load_proto(config_data, message);
    }

    turbo::Status MetaClient::dump_proto_to_file(const std::string &path, const google::protobuf::Message &message) {
        std::string content;
        auto rs = dump_proto(message, content);
        if (!rs.ok()) {
            return rs;
        }

        turbo::SequentialWriteFile file;
        rs = file.open(path, true);
        if (!rs.ok()) {
            return rs;
        }
        rs = file.write(content);
        if (!rs.ok()) {
            return rs;
        }
        file.close();
        return turbo::OkStatus();
    }

    turbo::Status MetaClient::load_proto(const std::string &content, google::protobuf::Message &message) {
        std::string err;
        if (!json2pb::JsonToProtoMessage(content, &message, &err)) {
            return turbo::InvalidArgumentError(err);
        }
        return turbo::OkStatus();
    }

    turbo::Status MetaClient::dump_proto(const google::protobuf::Message &message, std::string &content) {
        std::string err;
        content.clear();
        if (!json2pb::ProtoMessageToJson(message, &content, &err)) {
            return turbo::InvalidArgumentError(err);
        }
        return turbo::OkStatus();
    }

    turbo::Status MetaClient::check_config(const std::string &json_content) {
        EA::proto::ConfigInfo config_pb;
        std::string errmsg;
        if (!json2pb::JsonToProtoMessage(json_content, &config_pb, &errmsg)) {
            return turbo::InvalidArgumentError(errmsg);
        }
        return turbo::OkStatus();
    }

    turbo::Status MetaClient::check_config_file(const std::string &config_path) {
        turbo::SequentialReadFile file;
        auto rs = file.open(config_path);
        if (!rs.ok()) {
            return rs;
        }
        std::string config_data;
        auto rr = file.read(&config_data);
        if (!rr.ok()) {
            return rr.status();
        }
        return check_config(config_data);
    }

    turbo::Status MetaClient::dump_config_file(const std::string &config_path, const EA::proto::ConfigInfo &config) {
        turbo::SequentialWriteFile file;
        auto rs = file.open(config_path, true);
        if (!rs.ok()) {
            return rs;
        }
        std::string json;
        std::string err;
        if (!json2pb::ProtoMessageToJson(config, &json, &err)) {
            return turbo::InvalidArgumentError(err);
        }
        rs = file.write(json);
        if (!rs.ok()) {
            return rs;
        }
        file.close();
        return turbo::OkStatus();
    }

    turbo::Status
    MetaClient::create_config(const std::string &config_name, const std::string &content,
                              const std::string &version, const std::string &config_type, int *retry_times) {
        EA::proto::MetaManagerRequest request;
        EA::proto::MetaManagerResponse response;

        request.set_op_type(EA::proto::OP_CREATE_CONFIG);
        auto rc = request.mutable_config_info();
        ConfigInfoBuilder builder(rc);
        auto rs = builder.build_from_content(config_name, content, version, config_type);
        if(!rs.ok()) {
            return rs;
        }
        rs = meta_manager(request, response, retry_times);
        if (!rs.ok()) {
            return rs;
        }
        if (response.errcode() != EA::proto::SUCCESS) {
            return turbo::UnavailableError(response.errmsg());
        }
        return turbo::OkStatus();
    }

    turbo::Status
    MetaClient::create_config(const EA::proto::ConfigInfo &request,
                              int *retry_times) {
        EA::proto::MetaManagerRequest meta_request;
        EA::proto::MetaManagerResponse response;
        meta_request.set_op_type(EA::proto::OP_CREATE_CONFIG);
        *meta_request.mutable_config_info() = request;
        auto rs = meta_manager(meta_request, response, retry_times);
        if (!rs.ok()) {
            return rs;
        }
        if (response.errcode() != EA::proto::SUCCESS) {
            return turbo::UnavailableError(response.errmsg());
        }
        return turbo::OkStatus();
    }

    turbo::Status MetaClient::create_config_by_file(const std::string &config_name,
                                                    const std::string &path,
                                                    const std::string &config_type, const std::string &version,
                                                    int *retry_times) {
        EA::proto::MetaManagerRequest request;
        EA::proto::MetaManagerResponse response;
        EA::client::ConfigInfoBuilder builder(request.mutable_config_info());
        auto rs = builder.build_from_file(config_name, path, version, config_type);
        if(!rs.ok()) {
            return rs;
        }
        rs =  meta_manager(request, response, retry_times);
        if (!rs.ok()) {
            return rs;
        }
        if (response.errcode() != EA::proto::SUCCESS) {
            return turbo::UnavailableError(response.errmsg());
        }
        return turbo::OkStatus();
    }

    turbo::Status MetaClient::create_config_by_json(const std::string &json_path, int *retry_times) {
        EA::proto::MetaManagerRequest request;
        EA::proto::MetaManagerResponse response;
        EA::client::ConfigInfoBuilder builder(request.mutable_config_info());
        auto rs = builder.build_from_json_file(json_path);
        if(!rs.ok()) {
            return rs;
        }
        rs =  meta_manager(request, response, retry_times);
        if (!rs.ok()) {
            return rs;
        }
        if (response.errcode() != EA::proto::SUCCESS) {
            return turbo::UnavailableError(response.errmsg());
        }
        return turbo::OkStatus();
    }

    turbo::Status MetaClient::list_config(std::vector<std::string> &configs, int *retry_time) {
        EA::proto::QueryRequest request;
        EA::proto::QueryResponse response;
        request.set_op_type(EA::proto::QUERY_LIST_CONFIG);
        auto rs = meta_query(request, response, retry_time);
        if (!rs.ok()) {
            return rs;
        }
        if (response.errcode() != EA::proto::SUCCESS) {
            return turbo::UnknownError(response.errmsg());
        }
        auto res_configs = response.config_infos();
        for (auto config: res_configs) {
            configs.push_back(config.name());
        }
        return turbo::OkStatus();
    }

    turbo::Status MetaClient::list_config_version(const std::string &config_name, std::vector<std::string> &versions,
                                                  int *retry_time) {
        EA::proto::QueryRequest request;
        EA::proto::QueryResponse response;
        request.set_op_type(EA::proto::QUERY_LIST_CONFIG_VERSION);
        request.set_config_name(config_name);
        auto rs = meta_query(request, response, retry_time);
        if (!rs.ok()) {
            return rs;
        }
        if (response.errcode() != EA::proto::SUCCESS) {
            return turbo::UnknownError(response.errmsg());
        }
        auto res_configs = response.config_infos();
        for (auto &config: res_configs) {
            versions.push_back(version_to_string(config.version()));
        }
        return turbo::OkStatus();
    }

    turbo::Status
    MetaClient::list_config_version(const std::string &config_name, std::vector<turbo::ModuleVersion> &versions,
                                    int *retry_time) {
        EA::proto::QueryRequest request;
        EA::proto::QueryResponse response;
        request.set_op_type(EA::proto::QUERY_LIST_CONFIG_VERSION);
        request.set_config_name(config_name);
        auto rs = meta_query(request, response, retry_time);
        if (!rs.ok()) {
            return rs;
        }
        if (response.errcode() != EA::proto::SUCCESS) {
            return turbo::UnknownError(response.errmsg());
        }
        auto res_configs = response.config_infos();
        for (auto config: res_configs) {
            versions.emplace_back(config.version().major(),
                                  config.version().minor(), config.version().patch());
        }
        return turbo::OkStatus();
    }

    turbo::Status
    MetaClient::get_config(const std::string &config_name, const std::string &version, EA::proto::ConfigInfo &config,
                           int *retry_time) {
        EA::proto::QueryRequest request;
        EA::proto::QueryResponse response;
        request.set_op_type(EA::proto::QUERY_GET_CONFIG);
        request.set_config_name(config_name);
        auto rs = string_to_version(version, request.mutable_config_version());
        if (!rs.ok()) {
            return rs;
        }
        rs = meta_query(request, response, retry_time);
        if (!rs.ok()) {
            return rs;
        }
        if (response.errcode() != EA::proto::SUCCESS) {
            return turbo::UnknownError(response.errmsg());
        }
        if (response.config_infos_size() != 1) {
            return turbo::InvalidArgumentError("bad proto for config list size not 1");
        }

        config = response.config_infos(0);
        return turbo::OkStatus();
    }

    turbo::Status
    MetaClient::get_config(const std::string &config_name, const std::string &version, std::string &config,
                           int *retry_time, std::string *type, uint32_t *time) {
        EA::proto::ConfigInfo config_pb;
        auto rs = get_config(config_name, version, config_pb, retry_time);
        if (!rs.ok()) {
            return rs;
        }
        config = config_pb.content();
        if (type) {
            *type = config_type_to_string(config_pb.type());
        }
        if (time) {
            *time = config_pb.time();
        }
        return turbo::OkStatus();
    }

    turbo::Status MetaClient::save_config(const std::string &config_name, const std::string &version, std::string &path,
                                          int *retry_time) {
        std::string content;
        auto rs = get_config(config_name, version, content, retry_time);
        if (!rs.ok()) {
            return rs;
        }
        turbo::SequentialWriteFile file;
        rs = file.open(path, true);
        if (!rs.ok()) {
            return rs;
        }
        rs = file.write(content);
        if (!rs.ok()) {
            return rs;
        }
        file.close();
        return turbo::OkStatus();
    }

    turbo::Status
    MetaClient::save_config(const std::string &config_name, const std::string &version, int *retry_time) {
        std::string content;
        std::string type;
        auto rs = get_config(config_name, version, content, retry_time, &type);
        if (!rs.ok()) {
            return rs;
        }
        turbo::SequentialWriteFile file;
        auto path = turbo::Format("{}.{}", config_name, type);
        rs = file.open(path, true);
        if (!rs.ok()) {
            return rs;
        }
        rs = file.write(content);
        if (!rs.ok()) {
            return rs;
        }
        file.close();
        return turbo::OkStatus();
    }

    turbo::Status
    MetaClient::get_config_latest(const std::string &config_name, EA::proto::ConfigInfo &config,
                                  int *retry_time) {
        EA::proto::QueryRequest request;
        EA::proto::QueryResponse response;
        request.set_op_type(EA::proto::QUERY_GET_CONFIG);
        request.set_config_name(config_name);
        auto rs = meta_query(request, response, retry_time);
        if (!rs.ok()) {
            return rs;
        }
        if (response.errcode() != EA::proto::SUCCESS) {
            return turbo::UnknownError(response.errmsg());
        }
        if (response.config_infos_size() != 1) {
            return turbo::InvalidArgumentError("bad proto for config list size not 1");
        }

        config = response.config_infos(0);
        return turbo::OkStatus();
    }

    turbo::Status
    MetaClient::get_config_latest(const std::string &config_name, std::string &config, std::string &version,
                                  int *retry_time) {
        EA::proto::ConfigInfo config_pb;
        auto rs = get_config_latest(config_name, config_pb, retry_time);
        if (!rs.ok()) {
            return rs;
        }
        config = config_pb.content();
        version = version_to_string(config_pb.version());
        return turbo::OkStatus();
    }

    turbo::Status
    MetaClient::get_config_latest(const std::string &config_name, std::string &config, std::string &version,
                                  std::string &type,
                                  int *retry_time) {
        EA::proto::ConfigInfo config_pb;
        auto rs = get_config_latest(config_name, config_pb, retry_time);
        if (!rs.ok()) {
            return rs;
        }
        config = config_pb.content();
        version = version_to_string(config_pb.version());
        type = config_type_to_string(config_pb.type());
        return turbo::OkStatus();
    }

    turbo::Status
    MetaClient::get_config_latest(const std::string &config_name, std::string &config, turbo::ModuleVersion &version,
                                  int *retry_time) {
        EA::proto::ConfigInfo config_pb;
        auto rs = get_config_latest(config_name, config_pb, retry_time);
        if (!rs.ok()) {
            return rs;
        }
        config = config_pb.content();
        version = turbo::ModuleVersion(config_pb.version().major(), config_pb.version().minor(),
                                       config_pb.version().patch());
        return turbo::OkStatus();
    }

    turbo::Status
    MetaClient::get_config_latest(const std::string &config_name, std::string &config, turbo::ModuleVersion &version,
                                  std::string &type,
                                  int *retry_time) {
        EA::proto::ConfigInfo config_pb;
        auto rs = get_config_latest(config_name, config_pb, retry_time);
        if (!rs.ok()) {
            return rs;
        }
        config = config_pb.content();
        version = turbo::ModuleVersion(config_pb.version().major(), config_pb.version().minor(),
                                       config_pb.version().patch());
        type = config_type_to_string(config_pb.type());
        return turbo::OkStatus();
    }

    turbo::Status
    MetaClient::get_config_latest(const std::string &config_name, std::string &config, int *retry_time) {
        EA::proto::ConfigInfo config_pb;
        auto rs = get_config_latest(config_name, config_pb, retry_time);
        if (!rs.ok()) {
            return rs;
        }
        config = config_pb.content();
        return turbo::OkStatus();
    }

    turbo::Status
    MetaClient::remove_config(const std::string &config_name, const std::string &version, int *retry_time) {
        EA::proto::MetaManagerRequest request;
        EA::proto::MetaManagerResponse response;

        request.set_op_type(EA::proto::OP_REMOVE_CONFIG);
        auto rc = request.mutable_config_info();
        rc->set_name(config_name);
        auto rs = string_to_version(version, rc->mutable_version());
        if (!rs.ok()) {
            return rs;
        }
        rs = meta_manager(request, response, retry_time);
        if (!rs.ok()) {
            return rs;
        }
        if (response.errcode() != EA::proto::SUCCESS) {
            return turbo::UnknownError(response.errmsg());
        }
        return turbo::OkStatus();
    }

    turbo::Status
    MetaClient::remove_config(const std::string &config_name, const turbo::ModuleVersion &version, int *retry_time) {
        EA::proto::MetaManagerRequest request;
        EA::proto::MetaManagerResponse response;

        request.set_op_type(EA::proto::OP_REMOVE_CONFIG);
        auto rc = request.mutable_config_info();
        rc->set_name(config_name);
        rc->mutable_version()->set_major(version.major);
        rc->mutable_version()->set_minor(version.minor);
        rc->mutable_version()->set_minor(version.patch);
        auto rs = meta_manager(request, response, retry_time);
        if (!rs.ok()) {
            return rs;
        }
        if (response.errcode() != EA::proto::SUCCESS) {
            return turbo::UnknownError(response.errmsg());
        }
        return turbo::OkStatus();
    }

    turbo::Status
    MetaClient::remove_config_all_version(const std::string &config_name, int *retry_time) {
        EA::proto::MetaManagerRequest request;
        EA::proto::MetaManagerResponse response;

        request.set_op_type(EA::proto::OP_REMOVE_CONFIG);
        auto rc = request.mutable_config_info();
        rc->set_name(config_name);
        auto rs = meta_manager(request, response, retry_time);
        if (!rs.ok()) {
            return rs;
        }
        if (response.errcode() != EA::proto::SUCCESS) {
            return turbo::UnknownError(response.errmsg());
        }
        return turbo::OkStatus();
    }

    turbo::Status MetaClient::create_namespace(EA::proto::NameSpaceInfo &info, int *retry_time) {
        EA::proto::MetaManagerRequest request;
        EA::proto::MetaManagerResponse response;
        request.set_op_type(EA::proto::OP_CREATE_NAMESPACE);

        EA::proto::NameSpaceInfo *ns_req = request.mutable_namespace_info();
        *ns_req = info;
        auto rs = meta_manager(request, response, retry_time);
        if (!rs.ok()) {
            return rs;
        }
        return turbo::OkStatus();
    }

    turbo::Status MetaClient::create_namespace(const std::string &ns, int64_t quota, int *retry_time) {
        EA::proto::MetaManagerRequest request;
        EA::proto::MetaManagerResponse response;
        request.set_op_type(EA::proto::OP_CREATE_NAMESPACE);

        EA::proto::NameSpaceInfo *ns_req = request.mutable_namespace_info();
        auto rs = CheckValidNameType(ns);
        if (!rs.ok()) {
            return rs;
        }
        ns_req->set_namespace_name(ns);
        if (quota != 0) {
            ns_req->set_quota(quota);
        }
        rs = meta_manager(request, response, retry_time);
        if (!rs.ok()) {
            return rs;
        }
        return turbo::OkStatus();
    }

    turbo::Status MetaClient::create_namespace_by_json(const std::string &json_str, int *retry_time) {
        EA::proto::MetaManagerRequest request;
        EA::proto::MetaManagerResponse response;
        request.set_op_type(EA::proto::OP_CREATE_NAMESPACE);
        auto rs = load_proto(json_str, *request.mutable_namespace_info());
        if (!rs.ok()) {
            return rs;
        }
        rs = meta_manager(request, response, retry_time);
        if (!rs.ok()) {
            return rs;
        }
        return turbo::OkStatus();
    }

    turbo::Status MetaClient::create_namespace_by_file(const std::string &path, int *retry_time) {
        EA::proto::MetaManagerRequest request;
        EA::proto::MetaManagerResponse response;
        request.set_op_type(EA::proto::OP_CREATE_NAMESPACE);
        auto rs = load_proto_from_file(path, *request.mutable_namespace_info());
        if (!rs.ok()) {
            return rs;
        }
        rs = meta_manager(request, response, retry_time);
        if (!rs.ok()) {
            return rs;
        }
        return turbo::OkStatus();
    }

    turbo::Status MetaClient::remove_namespace(const std::string &ns, int *retry_time) {
        EA::proto::MetaManagerRequest request;
        EA::proto::MetaManagerResponse response;
        request.set_op_type(EA::proto::OP_DROP_NAMESPACE);

        EA::proto::NameSpaceInfo *ns_req = request.mutable_namespace_info();
        auto rs = CheckValidNameType(ns);
        if (!rs.ok()) {
            return rs;
        }
        ns_req->set_namespace_name(ns);
        rs = meta_manager(request, response, retry_time);
        if (!rs.ok()) {
            return rs;
        }
        return turbo::OkStatus();
    }

    turbo::Status MetaClient::modify_namespace(EA::proto::NameSpaceInfo &ns_info, int *retry_time) {
        EA::proto::MetaManagerRequest request;
        EA::proto::MetaManagerResponse response;
        request.set_op_type(EA::proto::OP_MODIFY_NAMESPACE);
        *request.mutable_namespace_info() = ns_info;
        auto rs = meta_manager(request, response, retry_time);
        if (!rs.ok()) {
            return rs;
        }
        return turbo::OkStatus();
    }

    turbo::Status MetaClient::modify_namespace_by_json(const std::string &json_str, int *retry_time) {
        EA::proto::MetaManagerRequest request;
        EA::proto::MetaManagerResponse response;
        request.set_op_type(EA::proto::OP_MODIFY_NAMESPACE);
        auto rs = load_proto(json_str, *request.mutable_namespace_info());
        if (!rs.ok()) {
            return rs;
        }
        rs = meta_manager(request, response, retry_time);
        if (!rs.ok()) {
            return rs;
        }
        return turbo::OkStatus();
    }

    turbo::Status MetaClient::modify_namespace_by_file(const std::string &path, int *retry_time) {
        EA::proto::MetaManagerRequest request;
        EA::proto::MetaManagerResponse response;
        request.set_op_type(EA::proto::OP_MODIFY_NAMESPACE);
        auto rs = load_proto_from_file(path, *request.mutable_namespace_info());
        if (!rs.ok()) {
            return rs;
        }
        rs = meta_manager(request, response, retry_time);
        if (!rs.ok()) {
            return rs;
        }
        return turbo::OkStatus();
    }

    turbo::Status MetaClient::list_namespace(std::vector<std::string> &ns_list, int *retry_time) {
        std::vector<EA::proto::NameSpaceInfo> ns_proto_list;
        auto rs = list_namespace(ns_proto_list, retry_time);
        if (!rs.ok()) {
            return rs;
        }
        for (auto &ns: ns_proto_list) {
            ns_list.push_back(ns.namespace_name());
        }
        return turbo::OkStatus();
    }

    turbo::Status MetaClient::list_namespace(std::vector<EA::proto::NameSpaceInfo> &ns_list, int *retry_time) {
        EA::proto::QueryRequest request;
        EA::proto::QueryResponse response;
        request.set_op_type(EA::proto::QUERY_NAMESPACE);
        auto rs = meta_query(request, response, retry_time);
        if (!rs.ok()) {
            return rs;
        }
        if (response.errcode() != EA::proto::SUCCESS) {
            return turbo::UnknownError(response.errmsg());
        }
        for (auto &ns: response.namespace_infos()) {
            ns_list.push_back(ns);
        }
        return turbo::OkStatus();
    }

    turbo::Status MetaClient::list_namespace_to_json(std::vector<std::string> &ns_list, int *retry_time) {
        std::vector<EA::proto::NameSpaceInfo> ns_proto_list;
        auto rs = list_namespace(ns_proto_list, retry_time);
        if (!rs.ok()) {
            return rs;
        }
        for (auto &ns: ns_proto_list) {
            std::string json_content;
            auto r = dump_proto(ns, json_content);
            if (!r.ok()) {
                return r;
            }
            ns_list.push_back(json_content);
        }
        return turbo::OkStatus();
    }

    turbo::Status MetaClient::list_namespace_to_file(const std::string &save_path, int *retry_time) {
        std::vector<std::string> json_list;
        auto rs = list_namespace_to_json(json_list, retry_time);
        if (!rs.ok()) {
            return rs;
        }

        turbo::SequentialWriteFile file;
        rs = file.open(save_path, true);
        if (!rs.ok()) {
            return rs;
        }
        for (auto &ns: json_list) {
            rs = file.write(ns);
            if (!rs.ok()) {
                return rs;
            }
        }
        file.close();
        return turbo::OkStatus();
    }

    turbo::Status
    MetaClient::get_namespace(const std::string &ns_name, EA::proto::NameSpaceInfo &ns_pb, int *retry_time) {
        EA::proto::QueryRequest request;
        EA::proto::QueryResponse response;
        request.set_op_type(EA::proto::QUERY_NAMESPACE);
        if (ns_name.empty()) {
            return turbo::InvalidArgumentError("namespace name empty");
        }
        request.set_namespace_name(ns_name);
        auto rs = meta_query(request, response, retry_time);
        if (!rs.ok()) {
            return rs;
        }
        if (response.errcode() != EA::proto::SUCCESS) {
            return turbo::UnknownError(response.errmsg());
        }
        if (response.namespace_infos_size() != 1) {
            return turbo::UnknownError("bad proto format for namespace info size {}", response.namespace_infos_size());
        }
        ns_pb = response.namespace_infos(0);
        return turbo::OkStatus();
    }

    turbo::Status MetaClient::get_namespace_json(const std::string &ns_name, std::string &json_str, int *retry_time) {
        EA::proto::NameSpaceInfo ns_pb;
        auto rs = get_namespace(ns_name, ns_pb, retry_time);
        if (!rs.ok()) {
            return rs;
        }
        return dump_proto(ns_pb, json_str);
    }

    turbo::Status
    MetaClient::save_namespace_json(const std::string &ns_name, const std::string &json_path, int *retry_time) {
        EA::proto::NameSpaceInfo ns_pb;
        auto rs = get_namespace(ns_name, ns_pb, retry_time);
        if (!rs.ok()) {
            return rs;
        }
        return dump_proto_to_file(json_path, ns_pb);
    }

    turbo::Status MetaClient::create_database(EA::proto::DataBaseInfo &info, int *retry_time) {
        EA::proto::MetaManagerRequest request;
        EA::proto::MetaManagerResponse response;
        request.set_op_type(EA::proto::OP_CREATE_DATABASE);

        auto *db_req = request.mutable_database_info();
        *db_req = info;
        auto rs = meta_manager(request, response, retry_time);
        if (!rs.ok()) {
            return rs;
        }
        return turbo::OkStatus();
    }

    turbo::Status
    MetaClient::create_database(const std::string &ns, const std::string &database, int64_t quota, int *retry_time) {
        EA::proto::DataBaseInfo database_pb;
        database_pb.set_namespace_name(ns);
        database_pb.set_database(database);
        if (quota != 0) {
            database_pb.set_quota(quota);
        }
        return create_database(database_pb, retry_time);
    }

    turbo::Status MetaClient::create_database_by_json(const std::string &json_str, int *retry_time) {
        EA::proto::MetaManagerRequest request;
        EA::proto::MetaManagerResponse response;
        request.set_op_type(EA::proto::OP_CREATE_DATABASE);
        auto rs = load_proto(json_str, *request.mutable_database_info());
        if (!rs.ok()) {
            return rs;
        }
        rs = meta_manager(request, response, retry_time);
        if (!rs.ok()) {
            return rs;
        }
        return turbo::OkStatus();
    }

    turbo::Status MetaClient::create_database_by_file(const std::string &path, int *retry_time) {
        EA::proto::MetaManagerRequest request;
        EA::proto::MetaManagerResponse response;
        request.set_op_type(EA::proto::OP_CREATE_DATABASE);
        auto rs = load_proto_from_file(path, *request.mutable_database_info());
        if (!rs.ok()) {
            return rs;
        }
        rs = meta_manager(request, response, retry_time);
        if (!rs.ok()) {
            return rs;
        }
        return turbo::OkStatus();
    }

    turbo::Status MetaClient::remove_database(const std::string &ns, const std::string &database, int *retry_time) {
        EA::proto::MetaManagerRequest request;
        EA::proto::MetaManagerResponse response;
        request.set_op_type(EA::proto::OP_DROP_DATABASE);

        auto *db_req = request.mutable_database_info();
        db_req->set_namespace_name(ns);
        db_req->set_database(database);
        auto rs = meta_manager(request, response, retry_time);
        if (!rs.ok()) {
            return rs;
        }
        return turbo::OkStatus();
    }

    turbo::Status MetaClient::modify_database(EA::proto::DataBaseInfo &db_info, int *retry_time) {
        EA::proto::MetaManagerRequest request;
        EA::proto::MetaManagerResponse response;
        request.set_op_type(EA::proto::OP_MODIFY_DATABASE);

        auto *db_req = request.mutable_database_info();
        *db_req = db_info;
        auto rs = meta_manager(request, response, retry_time);
        if (!rs.ok()) {
            return rs;
        }
        return turbo::OkStatus();
    }

    turbo::Status MetaClient::modify_database_by_json(const std::string &json_str, int *retry_time) {
        EA::proto::MetaManagerRequest request;
        EA::proto::MetaManagerResponse response;
        request.set_op_type(EA::proto::OP_MODIFY_DATABASE);
        auto rs = load_proto(json_str, *request.mutable_database_info());
        if (!rs.ok()) {
            return rs;
        }
        rs = meta_manager(request, response, retry_time);
        if (!rs.ok()) {
            return rs;
        }
        return turbo::OkStatus();
    }

    turbo::Status MetaClient::modify_database_by_file(const std::string &path, int *retry_time) {
        EA::proto::MetaManagerRequest request;
        EA::proto::MetaManagerResponse response;
        request.set_op_type(EA::proto::OP_MODIFY_DATABASE);
        auto rs = load_proto_from_file(path, *request.mutable_database_info());
        if (!rs.ok()) {
            return rs;
        }
        rs = meta_manager(request, response, retry_time);
        if (!rs.ok()) {
            return rs;
        }
        return turbo::OkStatus();
    }

    turbo::Status MetaClient::list_database(std::vector<EA::proto::DataBaseInfo> &db_list, int *retry_time) {
        EA::proto::QueryRequest request;
        EA::proto::QueryResponse response;
        request.set_op_type(EA::proto::QUERY_DATABASE);
        auto rs = meta_query(request, response, retry_time);
        if (!rs.ok()) {
            return rs;
        }
        if (response.errcode() != EA::proto::SUCCESS) {
            return turbo::UnknownError(response.errmsg());
        }
        for (auto &ns: response.database_infos()) {
            db_list.push_back(ns);
        }
        return turbo::OkStatus();
    }

    turbo::Status
    MetaClient::list_database(const std::string &ns, std::vector<EA::proto::DataBaseInfo> &db_list, int *retry_time) {
        std::vector<EA::proto::DataBaseInfo> all_db_list;
        auto rs = list_database(db_list, retry_time);
        if (!rs.ok()) {
            return rs;
        }
        for (auto &db: all_db_list) {
            if (db.namespace_name() == ns) {
                db_list.push_back(db);
            }
        }
        return turbo::OkStatus();
    }

    turbo::Status MetaClient::list_database(std::vector<std::string> &db_list, int *retry_time) {
        std::vector<EA::proto::DataBaseInfo> db_proto_list;
        auto rs = list_database(db_proto_list, retry_time);
        if (!rs.ok()) {
            return rs;
        }
        for (auto &db: db_proto_list) {
            db_list.push_back(turbo::Format("{},{}", db.namespace_name(), db.database()));
        }
        return turbo::OkStatus();
    }

    turbo::Status MetaClient::list_database(std::string &ns, std::vector<std::string> &db_list, int *retry_time) {
        std::vector<EA::proto::DataBaseInfo> db_proto_list;
        auto rs = list_database(ns, db_proto_list, retry_time);
        if (!rs.ok()) {
            return rs;
        }
        for (auto &db: db_proto_list) {
            db_list.push_back(turbo::Format("{},{}", db.namespace_name(), db.database()));
        }
        return turbo::OkStatus();
    }


    turbo::Status MetaClient::list_database_to_json(std::vector<std::string> &db_list, int *retry_time) {
        std::vector<EA::proto::DataBaseInfo> db_proto_list;
        auto rs = list_database(db_proto_list, retry_time);
        if (!rs.ok()) {
            return rs;
        }
        for (auto &db: db_proto_list) {
            std::string json_content;
            auto r = dump_proto(db, json_content);
            if (!r.ok()) {
                return r;
            }
            db_list.push_back(json_content);
        }
        return turbo::OkStatus();
    }

    turbo::Status
    MetaClient::list_database_to_json(const std::string &ns, std::vector<std::string> &db_list, int *retry_time) {
        std::vector<EA::proto::DataBaseInfo> db_proto_list;
        auto rs = list_database(ns, db_proto_list, retry_time);
        if (!rs.ok()) {
            return rs;
        }
        for (auto &db: db_proto_list) {
            std::string json_content;
            auto r = dump_proto(db, json_content);
            if (!r.ok()) {
                return r;
            }
            db_list.push_back(json_content);
        }
        return turbo::OkStatus();
    }

    turbo::Status MetaClient::list_database_to_file(const std::string &save_path, int *retry_time) {
        std::vector<std::string> json_list;
        auto rs = list_database_to_json(json_list, retry_time);
        if (!rs.ok()) {
            return rs;
        }

        turbo::SequentialWriteFile file;
        rs = file.open(save_path, true);
        if (!rs.ok()) {
            return rs;
        }
        for (auto &db: json_list) {
            rs = file.write(db);
            if (!rs.ok()) {
                return rs;
            }
        }
        file.close();
        return turbo::OkStatus();
    }

    turbo::Status
    MetaClient::list_database_to_file(const std::string &ns, const std::string &save_path, int *retry_time) {
        std::vector<std::string> json_list;
        auto rs = list_database_to_json(ns, json_list, retry_time);
        if (!rs.ok()) {
            return rs;
        }

        turbo::SequentialWriteFile file;
        rs = file.open(save_path, true);
        if (!rs.ok()) {
            return rs;
        }
        for (auto &db: json_list) {
            rs = file.write(db);
            if (!rs.ok()) {
                return rs;
            }
        }
        file.close();
        return turbo::OkStatus();
    }

    turbo::Status
    MetaClient::get_database(const std::string &ns_name, const std::string &db_name, EA::proto::DataBaseInfo &db_pb,
                             int *retry_time) {
        EA::proto::QueryRequest request;
        EA::proto::QueryResponse response;
        request.set_op_type(EA::proto::QUERY_DATABASE);
        if (ns_name.empty()) {
            return turbo::InvalidArgumentError("namespace name empty");
        }
        request.set_namespace_name(ns_name);
        request.set_database(db_name);
        auto rs = meta_query(request, response, retry_time);
        if (!rs.ok()) {
            return rs;
        }
        if (response.errcode() != EA::proto::SUCCESS) {
            return turbo::UnknownError(response.errmsg());
        }
        if (response.database_infos_size() != 1) {
            return turbo::UnknownError("bad proto format for database info size {}", response.database_infos_size());
        }
        db_pb = response.database_infos(0);
        return turbo::OkStatus();
    }

    turbo::Status
    MetaClient::get_database_json(const std::string &ns_name, const std::string &db_name, std::string &json_str,
                                  int *retry_time) {
        EA::proto::DataBaseInfo db_pb;
        auto rs = get_database(ns_name, db_name, db_pb, retry_time);
        if (!rs.ok()) {
            return rs;
        }
        return dump_proto(db_pb, json_str);
    }

    turbo::Status
    MetaClient::save_database_json(const std::string &ns_name, const std::string &db_name, const std::string &json_path,
                                   int *retry_time) {
        EA::proto::DataBaseInfo db_pb;
        auto rs = get_database(ns_name, db_name, db_pb, retry_time);
        if (!rs.ok()) {
            return rs;
        }
        return dump_proto_to_file(json_path, db_pb);
    }

    turbo::Status MetaClient::create_zone(EA::proto::ZoneInfo &zone_info, int *retry_time) {
        EA::proto::MetaManagerRequest request;
        EA::proto::MetaManagerResponse response;
        request.set_op_type(EA::proto::OP_CREATE_ZONE);

        auto *zone_req = request.mutable_zone_info();
        *zone_req = zone_info;
        auto rs = meta_manager(request, response, retry_time);
        if (!rs.ok()) {
            return rs;
        }
        return turbo::OkStatus();
    }

    turbo::Status
    MetaClient::create_zone(const std::string &ns, const std::string &zone, int64_t quota, int *retry_time) {
        EA::proto::ZoneInfo zone_pb;
        zone_pb.set_namespace_name(ns);
        zone_pb.set_zone(zone);
        if (quota != 0) {
            zone_pb.set_quota(quota);
        }
        return create_zone(zone_pb, retry_time);
    }

    turbo::Status MetaClient::create_zone_by_json(const std::string &json_str, int *retry_time) {
        EA::proto::MetaManagerRequest request;
        EA::proto::MetaManagerResponse response;
        request.set_op_type(EA::proto::OP_CREATE_ZONE);
        auto rs = load_proto(json_str, *request.mutable_zone_info());
        if (!rs.ok()) {
            return rs;
        }
        rs = meta_manager(request, response, retry_time);
        if (!rs.ok()) {
            return rs;
        }
        return turbo::OkStatus();
    }

    turbo::Status MetaClient::create_zone_by_file(const std::string &path, int *retry_time) {
        EA::proto::MetaManagerRequest request;
        EA::proto::MetaManagerResponse response;
        request.set_op_type(EA::proto::OP_CREATE_ZONE);
        auto rs = load_proto_from_file(path, *request.mutable_zone_info());
        if (!rs.ok()) {
            return rs;
        }
        rs = meta_manager(request, response, retry_time);
        if (!rs.ok()) {
            return rs;
        }
        return turbo::OkStatus();
    }

    turbo::Status MetaClient::remove_zone(const std::string &ns, const std::string &zone, int *retry_time) {
        EA::proto::MetaManagerRequest request;
        EA::proto::MetaManagerResponse response;
        request.set_op_type(EA::proto::OP_DROP_ZONE);

        auto *zone_req = request.mutable_zone_info();
        zone_req->set_namespace_name(ns);
        zone_req->set_zone(zone);
        auto rs = meta_manager(request, response, retry_time);
        if (!rs.ok()) {
            return rs;
        }
        return turbo::OkStatus();
    }

    turbo::Status MetaClient::modify_zone(EA::proto::ZoneInfo &zone_info, int *retry_time) {
        EA::proto::MetaManagerRequest request;
        EA::proto::MetaManagerResponse response;
        request.set_op_type(EA::proto::OP_MODIFY_ZONE);

        auto *zone_req = request.mutable_zone_info();
        *zone_req = zone_info;
        auto rs = meta_manager(request, response, retry_time);
        if (!rs.ok()) {
            return rs;
        }
        return turbo::OkStatus();
    }

    turbo::Status MetaClient::modify_zone_by_json(const std::string &json_str, int *retry_time) {
        EA::proto::MetaManagerRequest request;
        EA::proto::MetaManagerResponse response;
        request.set_op_type(EA::proto::OP_MODIFY_ZONE);
        auto rs = load_proto(json_str, *request.mutable_zone_info());
        if (!rs.ok()) {
            return rs;
        }
        rs = meta_manager(request, response, retry_time);
        if (!rs.ok()) {
            return rs;
        }
        return turbo::OkStatus();
    }

    turbo::Status MetaClient::modify_zone_by_file(const std::string &path, int *retry_time) {
        EA::proto::MetaManagerRequest request;
        EA::proto::MetaManagerResponse response;
        request.set_op_type(EA::proto::OP_MODIFY_ZONE);
        auto rs = load_proto_from_file(path, *request.mutable_zone_info());
        if (!rs.ok()) {
            return rs;
        }
        rs = meta_manager(request, response, retry_time);
        if (!rs.ok()) {
            return rs;
        }
        return turbo::OkStatus();
    }

    turbo::Status MetaClient::list_zone(std::vector<EA::proto::ZoneInfo> &zone_list, int *retry_time) {
        EA::proto::QueryRequest request;
        EA::proto::QueryResponse response;
        request.set_op_type(EA::proto::QUERY_ZONE);
        auto rs = meta_query(request, response, retry_time);
        if (!rs.ok()) {
            return rs;
        }
        if (response.errcode() != EA::proto::SUCCESS) {
            return turbo::UnknownError(response.errmsg());
        }
        for (auto &zone: response.zone_infos()) {
            zone_list.push_back(zone);
        }
        return turbo::OkStatus();
    }

    turbo::Status
    MetaClient::list_zone(const std::string &ns, std::vector<EA::proto::ZoneInfo> &zone_list, int *retry_time) {
        std::vector<EA::proto::ZoneInfo> all_zone_list;
        auto rs = list_zone(zone_list, retry_time);
        if (!rs.ok()) {
            return rs;
        }
        for (auto &zone: all_zone_list) {
            if (zone.namespace_name() == ns) {
                zone_list.push_back(zone);
            }
        }
        return turbo::OkStatus();
    }

    turbo::Status MetaClient::list_zone(std::vector<std::string> &zone_list, int *retry_time) {
        std::vector<EA::proto::ZoneInfo> zone_proto_list;
        auto rs = list_zone(zone_proto_list, retry_time);
        if (!rs.ok()) {
            return rs;
        }
        for (auto &zone: zone_proto_list) {
            zone_list.push_back(turbo::Format("{},{}", zone.namespace_name(), zone.zone()));
        }
        return turbo::OkStatus();
    }

    turbo::Status MetaClient::list_zone(std::string &ns, std::vector<std::string> &zone_list, int *retry_time) {
        std::vector<EA::proto::ZoneInfo> zone_proto_list;
        auto rs = list_zone(ns, zone_proto_list, retry_time);
        if (!rs.ok()) {
            return rs;
        }
        for (auto &zone: zone_proto_list) {
            zone_list.push_back(turbo::Format("{},{}", zone.namespace_name(), zone.zone()));
        }
        return turbo::OkStatus();
    }


    turbo::Status MetaClient::list_zone_to_json(std::vector<std::string> &zone_list, int *retry_time) {
        std::vector<EA::proto::ZoneInfo> zone_proto_list;
        auto rs = list_zone(zone_proto_list, retry_time);
        if (!rs.ok()) {
            return rs;
        }
        for (auto &zone: zone_proto_list) {
            std::string json_content;
            auto r = dump_proto(zone, json_content);
            if (!r.ok()) {
                return r;
            }
            zone_list.push_back(json_content);
        }
        return turbo::OkStatus();
    }

    turbo::Status
    MetaClient::list_zone_to_json(const std::string &ns, std::vector<std::string> &zone_list, int *retry_time) {
        std::vector<EA::proto::ZoneInfo> zone_proto_list;
        auto rs = list_zone(ns, zone_proto_list, retry_time);
        if (!rs.ok()) {
            return rs;
        }
        for (auto &zone: zone_proto_list) {
            std::string json_content;
            auto r = dump_proto(zone, json_content);
            if (!r.ok()) {
                return r;
            }
            zone_list.push_back(json_content);
        }
        return turbo::OkStatus();
    }

    turbo::Status MetaClient::list_zone_to_file(const std::string &save_path, int *retry_time) {
        std::vector<std::string> json_list;
        auto rs = list_zone_to_json(json_list, retry_time);
        if (!rs.ok()) {
            return rs;
        }

        turbo::SequentialWriteFile file;
        rs = file.open(save_path, true);
        if (!rs.ok()) {
            return rs;
        }
        for (auto &zone: json_list) {
            rs = file.write(zone);
            if (!rs.ok()) {
                return rs;
            }
        }
        file.close();
        return turbo::OkStatus();
    }

    turbo::Status MetaClient::list_zone_to_file(const std::string &ns, const std::string &save_path, int *retry_time) {
        std::vector<std::string> json_list;
        auto rs = list_zone_to_json(ns, json_list, retry_time);
        if (!rs.ok()) {
            return rs;
        }

        turbo::SequentialWriteFile file;
        rs = file.open(save_path, true);
        if (!rs.ok()) {
            return rs;
        }
        for (auto &zone: json_list) {
            rs = file.write(zone);
            if (!rs.ok()) {
                return rs;
            }
        }
        file.close();
        return turbo::OkStatus();
    }

    turbo::Status
    MetaClient::get_zone(const std::string &ns_name, const std::string &zone_name, EA::proto::ZoneInfo &zone_pb,
                         int *retry_time) {
        EA::proto::QueryRequest request;
        EA::proto::QueryResponse response;
        request.set_op_type(EA::proto::QUERY_ZONE);
        if (ns_name.empty()) {
            return turbo::InvalidArgumentError("namespace name empty");
        }
        request.set_namespace_name(ns_name);
        request.set_zone(zone_name);
        auto rs = meta_query(request, response, retry_time);
        if (!rs.ok()) {
            return rs;
        }
        if (response.errcode() != EA::proto::SUCCESS) {
            return turbo::UnknownError(response.errmsg());
        }
        if (response.zone_infos_size() != 1) {
            return turbo::UnknownError("bad proto format for zone info size {}", response.zone_infos_size());
        }
        zone_pb = response.zone_infos(0);
        return turbo::OkStatus();
    }

    turbo::Status
    MetaClient::get_zone_json(const std::string &ns_name, const std::string &zone_name, std::string &json_str,
                              int *retry_time) {
        EA::proto::ZoneInfo zone_pb;
        auto rs = get_zone(ns_name, zone_name, zone_pb, retry_time);
        if (!rs.ok()) {
            return rs;
        }
        return dump_proto(zone_pb, json_str);
    }

    turbo::Status
    MetaClient::save_zone_json(const std::string &ns_name, const std::string &zone_name, const std::string &json_path,
                               int *retry_time) {
        EA::proto::ZoneInfo zone_pb;
        auto rs = get_zone(ns_name, zone_name, zone_pb, retry_time);
        if (!rs.ok()) {
            return rs;
        }
        return dump_proto_to_file(json_path, zone_pb);
    }

    turbo::Status MetaClient::create_servlet(EA::proto::ServletInfo &servlet_info, int *retry_time) {
        EA::proto::MetaManagerRequest request;
        EA::proto::MetaManagerResponse response;
        request.set_op_type(EA::proto::OP_CREATE_SERVLET);

        auto *servlet_req = request.mutable_servlet_info();
        *servlet_req = servlet_info;
        auto rs = meta_manager(request, response, retry_time);
        if (!rs.ok()) {
            return rs;
        }
        return turbo::OkStatus();
    }

    turbo::Status
    MetaClient::create_servlet(const std::string &ns, const std::string &zone, const std::string &servlet,
                               int *retry_time) {
        EA::proto::ServletInfo servlet_pb;
        servlet_pb.set_namespace_name(ns);
        servlet_pb.set_zone(zone);
        servlet_pb.set_servlet_name(servlet);
        return create_servlet(servlet_pb, retry_time);
    }

    turbo::Status MetaClient::create_servlet_by_json(const std::string &json_str, int *retry_time) {
        EA::proto::ServletInfo servlet_pb;
        auto rs = load_proto(json_str, servlet_pb);
        if (!rs.ok()) {
            return rs;
        }
        return create_servlet(servlet_pb, retry_time);
    }

    turbo::Status MetaClient::create_servlet_by_file(const std::string &path, int *retry_time) {
        EA::proto::ServletInfo servlet_pb;
        auto rs = load_proto_from_file(path, servlet_pb);
        if (!rs.ok()) {
            return rs;
        }
        return create_servlet(servlet_pb, retry_time);
    }

    turbo::Status MetaClient::remove_servlet(const std::string &ns, const std::string &zone, const std::string &servlet,
                                             int *retry_time) {
        EA::proto::MetaManagerRequest request;
        EA::proto::MetaManagerResponse response;
        request.set_op_type(EA::proto::OP_DROP_SERVLET);

        auto *servlet_req = request.mutable_servlet_info();
        servlet_req->set_namespace_name(ns);
        servlet_req->set_zone(zone);
        servlet_req->set_servlet_name(servlet);
        auto rs = meta_manager(request, response, retry_time);
        if (!rs.ok()) {
            return rs;
        }
        return turbo::OkStatus();
    }

    turbo::Status MetaClient::modify_servlet(EA::proto::ServletInfo &servlet_info, int *retry_time) {
        EA::proto::MetaManagerRequest request;
        EA::proto::MetaManagerResponse response;
        request.set_op_type(EA::proto::OP_CREATE_SERVLET);

        auto *servlet_req = request.mutable_servlet_info();
        *servlet_req = servlet_info;
        auto rs = meta_manager(request, response, retry_time);
        if (!rs.ok()) {
            return rs;
        }
        return turbo::OkStatus();
    }

    turbo::Status MetaClient::modify_servlet_by_json(const std::string &json_str, int *retry_time) {
        EA::proto::ServletInfo servlet_pb;
        auto rs = load_proto(json_str, servlet_pb);
        if (!rs.ok()) {
            return rs;
        }
        return modify_servlet(servlet_pb, retry_time);
    }

    turbo::Status MetaClient::modify_servlet_by_file(const std::string &path, int *retry_time) {
        EA::proto::ServletInfo servlet_pb;
        auto rs = load_proto_from_file(path, servlet_pb);
        if (!rs.ok()) {
            return rs;
        }
        return modify_servlet(servlet_pb, retry_time);
    }

    turbo::Status MetaClient::list_servlet(std::vector<EA::proto::ServletInfo> &servlet_list, int *retry_time) {
        EA::proto::QueryRequest request;
        EA::proto::QueryResponse response;
        request.set_op_type(EA::proto::QUERY_ZONE);
        auto rs = meta_query(request, response, retry_time);
        if (!rs.ok()) {
            return rs;
        }
        if (response.errcode() != EA::proto::SUCCESS) {
            return turbo::UnknownError(response.errmsg());
        }
        for (auto &servlet: response.servlet_infos()) {
            servlet_list.push_back(servlet);
        }
        return turbo::OkStatus();
    }

    turbo::Status
    MetaClient::list_servlet(const std::string &ns, std::vector<EA::proto::ServletInfo> &servlet_list,
                             int *retry_time) {
        std::vector<EA::proto::ServletInfo> all_servlet_list;
        auto rs = list_servlet(all_servlet_list, retry_time);
        if (!rs.ok()) {
            return rs;
        }
        for (auto &servlet: all_servlet_list) {
            if (servlet.namespace_name() == ns) {
                servlet_list.push_back(servlet);
            }
        }
        return turbo::OkStatus();
    }

    turbo::Status
    MetaClient::list_servlet(const std::string &ns, const std::string &zone,
                             std::vector<EA::proto::ServletInfo> &servlet_list, int *retry_time) {
        std::vector<EA::proto::ServletInfo> all_servlet_list;
        auto rs = list_servlet(all_servlet_list, retry_time);
        if (!rs.ok()) {
            return rs;
        }
        for (auto &servlet: all_servlet_list) {
            if (servlet.namespace_name() == ns && servlet.zone() == zone) {
                servlet_list.push_back(servlet);
            }
        }
        return turbo::OkStatus();
    }

    turbo::Status MetaClient::list_servlet(std::vector<std::string> &servlet_list, int *retry_time) {
        std::vector<EA::proto::ServletInfo> all_servlet_list;
        auto rs = list_servlet(all_servlet_list, retry_time);
        if (!rs.ok()) {
            return rs;
        }
        for (auto &servlet: all_servlet_list) {
            servlet_list.push_back(servlet.servlet_name());
        }
        return turbo::OkStatus();
    }

    turbo::Status
    MetaClient::list_servlet(const std::string &ns, std::vector<std::string> &servlet_list, int *retry_time) {
        std::vector<EA::proto::ServletInfo> all_servlet_list;
        auto rs = list_servlet(all_servlet_list, retry_time);
        if (!rs.ok()) {
            return rs;
        }
        for (auto &servlet: all_servlet_list) {
            if (servlet.namespace_name() == ns) {
                servlet_list.push_back(servlet.servlet_name());
            }
        }
        return turbo::OkStatus();
    }

    turbo::Status
    MetaClient::list_servlet(const std::string &ns, const std::string &zone, std::vector<std::string> &servlet_list,
                             int *retry_time) {
        std::vector<EA::proto::ServletInfo> all_servlet_list;
        auto rs = list_servlet(all_servlet_list, retry_time);
        if (!rs.ok()) {
            return rs;
        }
        for (auto &servlet: all_servlet_list) {
            if (servlet.namespace_name() == ns && servlet.zone() == zone) {
                servlet_list.push_back(servlet.servlet_name());
            }
        }
        return turbo::OkStatus();
    }

    turbo::Status MetaClient::list_servlet_to_json(std::vector<std::string> &servlet_list, int *retry_time) {
        std::vector<EA::proto::ServletInfo> servlet_proto_list;
        auto rs = list_servlet(servlet_proto_list, retry_time);
        if (!rs.ok()) {
            return rs;
        }
        for (auto &servlet: servlet_proto_list) {
            std::string json_content;
            auto r = dump_proto(servlet, json_content);
            if (!r.ok()) {
                return r;
            }
            servlet_list.push_back(json_content);
        }
        return turbo::OkStatus();
    }

    turbo::Status
    MetaClient::list_servlet_to_json(const std::string &ns, std::vector<std::string> &servlet_list, int *retry_time) {
        std::vector<EA::proto::ServletInfo> servlet_proto_list;
        auto rs = list_servlet(servlet_proto_list, retry_time);
        if (!rs.ok()) {
            return rs;
        }
        for (auto &servlet: servlet_proto_list) {
            if (servlet.namespace_name() == ns) {
                std::string json_content;
                auto r = dump_proto(servlet, json_content);
                if (!r.ok()) {
                    return r;
                }
                servlet_list.push_back(json_content);
            }
        }
        return turbo::OkStatus();
    }

    turbo::Status MetaClient::list_servlet_to_json(const std::string &ns, const std::string &zone,
                                                   std::vector<std::string> &servlet_list, int *retry_time) {
        std::vector<EA::proto::ServletInfo> servlet_proto_list;
        auto rs = list_servlet(servlet_proto_list, retry_time);
        if (!rs.ok()) {
            return rs;
        }
        for (auto &servlet: servlet_proto_list) {
            if (servlet.namespace_name() == ns && servlet.zone() == zone) {
                std::string json_content;
                auto r = dump_proto(servlet, json_content);
                if (!r.ok()) {
                    return r;
                }
                servlet_list.push_back(json_content);
            }
        }
        return turbo::OkStatus();
    }

    turbo::Status MetaClient::list_servlet_to_file(const std::string &save_path, int *retry_time) {
        std::vector<std::string> json_list;
        auto rs = list_servlet_to_json(json_list, retry_time);
        if (!rs.ok()) {
            return rs;
        }

        turbo::SequentialWriteFile file;
        rs = file.open(save_path, true);
        if (!rs.ok()) {
            return rs;
        }
        for (auto &zone: json_list) {
            rs = file.write(zone);
            if (!rs.ok()) {
                return rs;
            }
        }
        file.close();
        return turbo::OkStatus();
    }

    turbo::Status
    MetaClient::list_servlet_to_file(const std::string &ns, const std::string &save_path, int *retry_time) {
        std::vector<std::string> json_list;
        auto rs = list_servlet_to_json(ns, json_list, retry_time);
        if (!rs.ok()) {
            return rs;
        }

        turbo::SequentialWriteFile file;
        rs = file.open(save_path, true);
        if (!rs.ok()) {
            return rs;
        }
        for (auto &zone: json_list) {
            rs = file.write(zone);
            if (!rs.ok()) {
                return rs;
            }
        }
        file.close();
        return turbo::OkStatus();
    }

    turbo::Status
    MetaClient::list_servlet_to_file(const std::string &ns, const std::string &zone, const std::string &save_path,
                                     int *retry_time) {
        std::vector<std::string> json_list;
        auto rs = list_servlet_to_json(ns, zone, json_list, retry_time);
        if (!rs.ok()) {
            return rs;
        }

        turbo::SequentialWriteFile file;
        rs = file.open(save_path, true);
        if (!rs.ok()) {
            return rs;
        }
        for (auto &zone: json_list) {
            rs = file.write(zone);
            if (!rs.ok()) {
                return rs;
            }
        }
        file.close();
        return turbo::OkStatus();
    }

    turbo::Status
    MetaClient::get_servlet(const std::string &ns_name, const std::string &zone_name, const std::string &servlet,
                            EA::proto::ServletInfo &servlet_pb,
                            int *retry_time) {
        EA::proto::QueryRequest request;
        EA::proto::QueryResponse response;
        request.set_op_type(EA::proto::QUERY_ZONE);
        auto rs = meta_query(request, response, retry_time);
        if (!rs.ok()) {
            return rs;
        }
        if (response.errcode() != EA::proto::SUCCESS) {
            return turbo::UnknownError(response.errmsg());
        }
        if (response.servlet_infos_size() != 1) {
            return turbo::UnknownError("bad proto format for servlet infos size: {}", response.servlet_infos_size());
        }
        servlet_pb = response.servlet_infos(0);
        return turbo::OkStatus();
    }

    turbo::Status
    MetaClient::get_servlet_json(const std::string &ns_name, const std::string &zone_name, const std::string &servlet,
                                 std::string &json_str,
                                 int *retry_time) {
        EA::proto::ServletInfo servlet_pb;
        auto rs = get_servlet(ns_name, zone_name, servlet, servlet_pb, retry_time);
        if (!rs.ok()) {
            return rs;
        }
        return dump_proto(servlet_pb, json_str);
    }

    turbo::Status
    MetaClient::save_servlet_json(const std::string &ns_name, const std::string &zone_name, const std::string &servlet,
                                  const std::string &json_path,
                                  int *retry_time) {
        EA::proto::ServletInfo servlet_pb;
        auto rs = get_servlet(ns_name, zone_name, servlet, servlet_pb, retry_time);
        if (!rs.ok()) {
            return rs;
        }
        rs = dump_proto_to_file(json_path, servlet_pb);
        return rs;

    }

    turbo::Status MetaClient::add_logical(const std::vector<std::string> &logical_list, int *retry_time) {
        EA::proto::MetaManagerRequest request;
        EA::proto::MetaManagerResponse response;
        request.set_op_type(EA::proto::OP_ADD_LOGICAL);
        for (auto &logical: logical_list) {
            request.mutable_logical_rooms()->add_logical_rooms(logical);
        }
        auto rs = meta_manager(request, response, retry_time);
        if (!rs.ok()) {
            return rs;
        }
        if (response.errcode() != EA::proto::SUCCESS) {
            return turbo::UnknownError(response.errmsg());
        }
        return turbo::OkStatus();
    }


    turbo::Status MetaClient::add_logical(const std::string &logical, int *retry_time) {
        return add_logical({logical}, retry_time);
    }

    turbo::Status MetaClient::remove_logical(const std::vector<std::string> &logical_list, int *retry_time) {
        EA::proto::MetaManagerRequest request;
        EA::proto::MetaManagerResponse response;
        request.set_op_type(EA::proto::OP_DROP_LOGICAL);
        for (auto &logical: logical_list) {
            request.mutable_logical_rooms()->add_logical_rooms(logical);
        }
        auto rs = meta_manager(request, response, retry_time);
        if (!rs.ok()) {
            return rs;
        }
        if (response.errcode() != EA::proto::SUCCESS) {
            return turbo::UnknownError(response.errmsg());
        }
        return turbo::OkStatus();
    }

    turbo::Status MetaClient::remove_logical(const std::string &logical, int *retry_time) {
        return remove_logical({logical}, retry_time);
    }

    turbo::Status
    MetaClient::add_physical(const std::string &logical, const std::vector<std::string> &physicals, int *retry_time) {
        EA::proto::MetaManagerRequest request;
        EA::proto::MetaManagerResponse response;
        request.set_op_type(EA::proto::OP_ADD_PHYSICAL);
        request.mutable_physical_rooms()->set_logical_room(logical);
        for (auto &pm: physicals) {
            request.mutable_physical_rooms()->add_physical_rooms(pm);
        }

        auto rs = meta_manager(request, response, retry_time);
        if (!rs.ok()) {
            return rs;
        }
        if (response.errcode() != EA::proto::SUCCESS) {
            return turbo::UnknownError(response.errmsg());
        }
        return turbo::OkStatus();
    }

    turbo::Status MetaClient::add_physical(const std::string &logical, const std::string &physical, int *retry_time) {
        return add_physical(logical, {physical}, retry_time);
    }

    turbo::Status MetaClient::remove_physical(const std::string &logical, const std::vector<std::string> &physicals,
                                              int *retry_time) {
        EA::proto::MetaManagerRequest request;
        EA::proto::MetaManagerResponse response;
        request.set_op_type(EA::proto::OP_DROP_PHYSICAL);
        request.mutable_physical_rooms()->set_logical_room(logical);
        for (auto &pm: physicals) {
            request.mutable_physical_rooms()->add_physical_rooms(pm);
        }

        auto rs = meta_manager(request, response, retry_time);
        if (!rs.ok()) {
            return rs;
        }
        if (response.errcode() != EA::proto::SUCCESS) {
            return turbo::UnknownError(response.errmsg());
        }
        return turbo::OkStatus();
    }

    turbo::Status
    MetaClient::remove_physical(const std::string &logical, const std::string &physical, int *retry_time) {
        return remove_physical(logical, {physical}, retry_time);
    }

    turbo::Status MetaClient::move_physical(const std::string &logical_from, const std::string &logical_to,
                                            const std::string &physical, int *retry_time) {
        EA::proto::MetaManagerRequest request;
        EA::proto::MetaManagerResponse response;
        request.set_op_type(EA::proto::OP_MOVE_PHYSICAL);
        request.mutable_move_physical_request()->set_old_logical_room(logical_from);
        request.mutable_move_physical_request()->set_new_logical_room(logical_to);
        request.mutable_move_physical_request()->set_physical_room(physical);
        auto rs = meta_manager(request, response, retry_time);
        if (!rs.ok()) {
            return rs;
        }
        if (response.errcode() != EA::proto::SUCCESS) {
            return turbo::UnknownError(response.errmsg());
        }
        return turbo::OkStatus();
    }

    turbo::Status MetaClient::list_logical(std::vector<std::string> &logicals, int *retry_time) {
        EA::proto::QueryRequest request;
        EA::proto::QueryResponse response;
        request.set_op_type(EA::proto::QUERY_LOGICAL);
        auto rs = meta_query(request, response, retry_time);
        if (!rs.ok()) {
            return rs;
        }
        if (response.errcode() != EA::proto::SUCCESS) {
            return turbo::UnknownError(response.errmsg());
        }
        for (auto &logical: response.physical_rooms()) {
            logicals.push_back(logical.logical_room());
        }
        return turbo::OkStatus();
    }

    turbo::Status MetaClient::get_logical(const std::string &logical, EA::proto::PhysicalRoom &rooms, int *retry_time) {
        EA::proto::QueryRequest request;
        EA::proto::QueryResponse response;
        request.set_op_type(EA::proto::QUERY_LOGICAL);
        request.set_logical_room(logical);
        auto rs = meta_query(request, response, retry_time);
        if (!rs.ok()) {
            return rs;
        }
        if (response.errcode() != EA::proto::SUCCESS) {
            return turbo::UnknownError(response.errmsg());
        }
        rooms = response.physical_rooms(0);
        return turbo::OkStatus();
    }

    turbo::Status
    MetaClient::get_logical(const std::string &logical, std::vector<std::string> &physicals, int *retry_time) {
        EA::proto::PhysicalRoom ph;
        auto rs = get_logical(logical, ph, retry_time);
        if (!rs.ok()) {
            return rs;
        }
        for (auto &p: ph.physical_rooms()) {
            physicals.push_back(p);
        }
        return turbo::OkStatus();
    }

    turbo::Status MetaClient::list_physical(std::vector<EA::proto::PhysicalRoom> &rooms, int *retry_time) {
        EA::proto::QueryRequest request;
        EA::proto::QueryResponse response;
        request.set_op_type(EA::proto::QUERY_PHYSICAL);
        auto rs = meta_query(request, response, retry_time);
        if (!rs.ok()) {
            return rs;
        }
        if (response.errcode() != EA::proto::SUCCESS) {
            return turbo::UnknownError(response.errmsg());
        }
        for (auto &ph: response.physical_rooms()) {
            rooms.push_back(ph);
        }
        return turbo::OkStatus();
    }

}  // namespace EA::client

