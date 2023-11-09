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
#include "elasticann/client/proto_help.h"
#include "turbo/strings/utility.h"

namespace EA::client {

    std::string config_type_to_string(EA::proto::ConfigType type) {
        switch (type) {
            case EA::proto::CF_JSON:
                return "json";
            case EA::proto::CF_TEXT:
                return "text";
            case EA::proto::CF_INI:
                return "ini";
            case EA::proto::CF_YAML:
                return "yaml";
            case EA::proto::CF_XML:
                return "xml";
            case EA::proto::CF_GFLAGS:
                return "gflags";
            case EA::proto::CF_TOML:
                return "toml";
            default:
                return "unknown format";
        }
    }

    turbo::ResultStatus<EA::proto::ConfigType> string_to_config_type(const std::string &str) {
        auto lc = turbo::StrToLower(str);
        if(lc == "json") {
            return EA::proto::CF_JSON;
        } else if(lc == "text") {
            return EA::proto::CF_TEXT;
        } else if(lc == "ini") {
            return EA::proto::CF_INI;
        } else if(lc == "yaml") {
            return EA::proto::CF_YAML;
        } else if(lc == "xml") {
            return EA::proto::CF_XML;
        }else if(lc == "gflags") {
            return EA::proto::CF_GFLAGS;
        }else if(lc == "toml") {
            return EA::proto::CF_TOML;
        }
        return turbo::InvalidArgumentError("unknown format '{}'", str);
    }

    std::string platform_to_string(EA::proto::Platform type) {
        switch (type) {
            case EA::proto::PF_lINUX:
                return "linux";
            case EA::proto::PF_OSX:
                return "osx";
            case EA::proto::PF_WINDOWS:
                return "windows";
            default:
                return "unknown format";
        }
    }
    turbo::ResultStatus<EA::proto::Platform> string_to_platform(const std::string &str) {
        auto lc = turbo::StrToLower(str);
        if(str == "linux") {
            return EA::proto::PF_lINUX;
        } else if(str == "osx") {
            return EA::proto::PF_OSX;
        } else if(str == "windows") {
            return EA::proto::PF_WINDOWS;
        }
        return turbo::InvalidArgumentError("unknown platform '{}'", str);
    }

    std::string get_op_string(EA::proto::OpType type) {
        switch (type) {
            case EA::proto::OP_CREATE_NAMESPACE:
                return "create namespace";
            case EA::proto::OP_DROP_NAMESPACE:
                return "remove namespace";
            case EA::proto::OP_MODIFY_NAMESPACE:
                return "modify namespace";
            case EA::proto::OP_CREATE_DATABASE:
                return "create database";
            case EA::proto::OP_DROP_DATABASE:
                return "remove database";
            case EA::proto::OP_MODIFY_DATABASE:
                return "modify database";
            case EA::proto::OP_ADD_LOGICAL:
                return "add logical idc";
            case EA::proto::OP_DROP_LOGICAL:
                return "remove logical idc";
            case EA::proto::OP_ADD_PHYSICAL:
                return "create physical idc";
            case EA::proto::OP_DROP_PHYSICAL:
                return "remove physical idc";
            case EA::proto::OP_MOVE_PHYSICAL:
                return "move physical idc";
            case EA::proto::OP_CREATE_CONFIG:
                return "create config";
            case EA::proto::OP_REMOVE_CONFIG:
                return "remove config";
            default:
                return "unknown operation";
        }
    }

    std::string get_op_string(EA::proto::QueryOpType type) {
        switch (type) {
            case EA::proto::QUERY_NAMESPACE:
                return "query namespace";
            case EA::proto::QUERY_DATABASE:
                return "query database";
            case EA::proto::QUERY_LOGICAL:
                return "query logical";
            case EA::proto::QUERY_PHYSICAL:
                return "query physical";
            case EA::proto::QUERY_LIST_CONFIG_VERSION:
                return "list config version";
            case EA::proto::QUERY_LIST_CONFIG:
                return "list config";
            case EA::proto::QUERY_GET_CONFIG:
                return "get config";
            default:
                return "unknown operation";
        }
    }

    turbo::Status string_to_version(const std::string &str, EA::proto::Version*v) {
        std::vector<std::string> vs = turbo::StrSplit(str, ".");
        if(vs.size() != 3)
            return turbo::InvalidArgumentError("version error, should be like 1.2.3");
        int64_t m;
        if(!turbo::SimpleAtoi(vs[0], &m)) {
            return turbo::InvalidArgumentError("version error, should be like 1.2.3");
        }
        v->set_major(m);
        if(!turbo::SimpleAtoi(vs[1], &m)) {
            return turbo::InvalidArgumentError("version error, should be like 1.2.3");
        }
        v->set_minor(m);
        if(!turbo::SimpleAtoi(vs[2], &m)) {
            return turbo::InvalidArgumentError("version error, should be like 1.2.3");
        }
        v->set_patch(m);
        return turbo::OkStatus();
    }

}  // namespace EA::client
