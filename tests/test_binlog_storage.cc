// Copyright 2023 The Turbo Authors.
// Copyright (c) 2018-present Baidu, Inc. All Rights Reserved.
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

#include <vector>
#include <string>
#include <atomic>
#include <fstream>
#include <unordered_set>
#include <brpc/channel.h>

#include "elasticann/common/meta_server_interact.h"

namespace EA {
    DECLARE_string(meta_server_bns);

    int64_t get_tso() {
        proto::TsoRequest request;
        proto::TsoResponse response;
        request.set_op_type(proto::OP_GEN_TSO);
        request.set_count(1);
        MetaServerInteract msi;
        msi.init_internal(FLAGS_meta_server_bns);
        //发送请求，收到响应
        if (msi.send_request("tso_service", request, response) == 0) {
            //处理响应
            if (response.errcode() != proto::SUCCESS) {
                std::cout << "faield line:" << __LINE__ << std::endl;
                return -1;
            }

        } else {
            std::cout << "faield line:" << __LINE__ << std::endl;
            return -1;
        }

        int64_t tso_physical = response.start_timestamp().physical();
        int64_t tso_logical = response.start_timestamp().logical();
        std::cout << "physical: " << tso_physical << " logical:" << tso_logical << std::endl;
        return tso_physical << 18 + tso_logical;
    }

}  // namespace EA

int main(int argc, char **argv) {
    google::ParseCommandLineFlags(&argc, &argv, true);
    int64_t tso = EA::get_tso();
    std::cout << "tso:" << tso << std::endl;

    return 0;
}