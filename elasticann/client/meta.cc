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

namespace EA::client {

    turbo::Status MetaClient::init(const std::string &meta_address) {
        _master_leader_address.ip = butil::IP_ANY;
        _master_leader_address.port = 0;
        _connect_timeout = FLAGS_meta_connect_timeout;
        _request_timeout = FLAGS_meta_request_timeout;
        brpc::ChannelOptions channel_opt;
        channel_opt.timeout_ms = FLAGS_meta_request_timeout;
        channel_opt.connect_timeout_ms = FLAGS_meta_connect_timeout;
        std::string meta_server_addr = meta_address;
        if (_backup_channel.Init(meta_server_addr.c_str(), "rr", &channel_opt) != 0) {
            TLOG_ERROR("meta server bns pool init fail. bns_name:{}", meta_server_addr);
            return turbo::UnavailableError("meta server bns pool init fail. bns_name:{}", meta_server_addr);
        }
        _is_inited = true;
        return turbo::OkStatus();
    }

    void MetaClient::set_leader_address(const butil::EndPoint &addr) {
        std::unique_lock<std::mutex> lock(_master_leader_mutex);
        _master_leader_address = addr;
    }

    turbo::Status
    MetaClient::create_config(const std::string &config_name, const std::string &content,
                              const std::string &config_type, const std::string &version) {
        EA::proto::MetaManagerRequest request;
        EA::proto::MetaManagerResponse response;

        request.set_op_type(EA::proto::OP_CREATE_CONFIG);
        auto rc = request.mutable_config_info();
        rc->set_name(config_name);

        rc->set_time(static_cast<int>(turbo::ToTimeT(turbo::Now())));
        auto r = string_to_config_type(config_type);
        if (!r.ok()) {
            return r.status();
        }
        rc->set_type(r.value());
        if (!version.empty()) {
            auto v = rc->mutable_version();
            auto st = string_to_version(version, v);
            if (!st.ok()) {
                return st;
            }
        }

        if (content.empty()) {
            return turbo::InvalidArgumentError("config content empty");
        }
        rc->set_content(content);
        return meta_manager(request, response);
    }

    turbo::Status MetaClient::create_config_by_file(const std::string &config_name,
                                        const std::string &path,
                                        const std::string &config_type, const std::string & version) {
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
        return create_config(config_name, config_data, config_type, version);

    }

    turbo::Status MetaClient::create_config_by_json(const std::string &json_path) {
        turbo::SequentialReadFile file;
        auto rs = file.open(json_path);
        if (!rs.ok()) {
            return rs;
        }
        std::string config_data;
        auto rr = file.read(&config_data);
        if (!rr.ok()) {
            return rr.status();
        }

        EA::proto::MetaManagerRequest request;
        EA::proto::MetaManagerResponse response;
        std::string errmsg;
        if(!json2pb::JsonToProtoMessage(config_data, &request, &errmsg)) {
            return turbo::InvalidArgumentError(errmsg);
        }
        return meta_manager(request, response);
    }


}  // namespace EA::client

