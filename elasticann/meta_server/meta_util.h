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
#include "turbo/strings/str_split.h"
#include "elasticann/meta_server/base_state_machine.h"

namespace EA::servlet {

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
            response->set_errcode(EA::servlet::HAVE_NOT_INIT);\
            response->set_errmsg("have not init");\
            return;\
        }\
    } while (0);

}//namespace EA::servlet

