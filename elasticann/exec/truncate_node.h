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


// Brief:  truncate table exec node
#pragma once

#include "elasticann/exec/exec_node.h"

namespace EA {
    class TruncateNode : public ExecNode {
    public:
        TruncateNode() {
        }

        virtual ~TruncateNode() {
        }

        virtual int init(const proto::PlanNode &node);

        virtual int open(RuntimeState *state);

        virtual void transfer_pb(int64_t region_id, proto::PlanNode *pb_node);

        int64_t table_id() {
            return _table_id;
        }

        int32_t get_partition_field() {
            return _table_info->partition_info.partition_field();
        }

        int64_t get_partition_num() {
            return _table_info->partition_num;
        }

    private:
        int64_t _region_id = 0;
        int64_t _table_id = 0;
        SmartTable _table_info;
    };

}

