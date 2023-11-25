// Copyright 2023 The Elastic AI Search Authors.
//
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


#pragma once

#include <string>
#include <vector>
#include "elasticann/common/common.h"
#include "turbo/strings/str_split.h"
#include "elasticann/meta_server/base_state_machine.h"

namespace EA {
    struct IdcInfo {
        std::string resource_tag;
        std::string logical_room;
        std::string physical_room;

        IdcInfo() = default;

        IdcInfo(const std::string &resource, const std::string &logical, const std::string &physical)
                : resource_tag(resource), logical_room(logical), physical_room(physical) {};

        IdcInfo(std::string_view str) {
            std::vector<std::string> split_vec = turbo::StrSplit(str, ':', turbo::SkipEmpty());
            if (split_vec.size() >= 1) {
                resource_tag = split_vec[0];
            }
            if (split_vec.size() >= 2) {
                logical_room = split_vec[1];
            }
            if (split_vec.size() == 3) {
                physical_room = split_vec[2];
            }
        }

        [[nodiscard]] std::string to_string() const {
            return resource_tag + ":" + logical_room + ":" + physical_room;
        }

        [[nodiscard]] std::string logical_room_level() const {
            return resource_tag + ":" + logical_room + ":";
        }

        [[nodiscard]] std::string resource_tag_level() const {
            return resource_tag + "::";
        }

        bool match(const IdcInfo &other) const {
            if ((!resource_tag.empty() && !other.resource_tag.empty() && resource_tag != other.resource_tag)
                || (!logical_room.empty() && !other.logical_room.empty() && logical_room != other.logical_room)
                || (!physical_room.empty() && !other.physical_room.empty() && physical_room != other.physical_room)) {
                return false;
            }
            return true;
        }
    };

#define ERROR_SET_RESPONSE(response, errcode, err_message, op_type, log_id) \
    do {\
        TLOG_ERROR("request op_type:{}, {} ,log_id:{}",\
                op_type, err_message, log_id);\
        if (response != nullptr) {\
            response->set_errcode(errcode);\
            response->set_errmsg(err_message);\
            response->set_op_type(op_type);\
        }\
    }while (0);

#define ERROR_SET_RESPONSE_WARN(response, errcode, err_message, op_type, log_id) \
    do {\
        TLOG_WARN("request op_type:{}, {} ,log_id:{}",\
                op_type, err_message, log_id);\
        if (response != nullptr) {\
            response->set_errcode(errcode);\
            response->set_errmsg(err_message);\
            response->set_op_type(op_type);\
        }\
    }while (0);

#define IF_DONE_SET_RESPONSE(done, errcode, err_message) \
    do {\
        if (done && ((MetaServerClosure*)done)->response) {\
            ((MetaServerClosure*)done)->response->set_errcode(errcode);\
            ((MetaServerClosure*)done)->response->set_errmsg(err_message);\
        }\
    }while (0);

#define SET_RESPONSE(response, errcode, err_message) \
    do {\
        if (response) {\
            response->set_errcode(errcode);\
            response->set_errmsg(err_message);\
        }\
    }while (0);

#define RETURN_IF_NOT_INIT(init, response, log_id) \
    do {\
        if (!init) {\
            TLOG_WARN("have not init, log_id:{}", log_id);\
            response->set_errcode(proto::HAVE_NOT_INIT);\
            response->set_errmsg("have not init");\
            return;\
        }\
    } while (0);

}//namespace EA

