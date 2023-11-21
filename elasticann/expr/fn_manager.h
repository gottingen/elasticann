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

#include <functional>
#include "elasticann/common/expr_value.h"
#include "eaproto/meta/expr.pb.h"
#include "elasticann/common/object_manager.h"

namespace EA {
    class FunctionManager : public ObjectManager<
            std::function<ExprValue(const std::vector<ExprValue> &)>,
            FunctionManager> {
    public:
        int init();

        bool swap_op(proto::Function &fn);

        static int complete_fn(proto::Function &fn, std::vector<proto::PrimitiveType> types);

        static void complete_common_fn(proto::Function &fn, std::vector<proto::PrimitiveType> &types);

    private:
        void register_operators();

        static void complete_fn_simple(proto::Function &fn, int num_args,
                                       proto::PrimitiveType arg_type, proto::PrimitiveType ret_type);

        static void complete_fn(proto::Function &fn, int num_args,
                                proto::PrimitiveType arg_type, proto::PrimitiveType ret_type);
    };
}

