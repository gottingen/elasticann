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

#include "elasticann/exec/dml_manager_node.h"
#include "elasticann/exec/dml_node.h"

namespace EA {
    class DeleteManagerNode : public DmlManagerNode {
    public:
        DeleteManagerNode() {
        }

        virtual ~DeleteManagerNode() {
        }

        virtual int open(RuntimeState *state);

        int open_global_delete(RuntimeState *state);

        int init_delete_info(const proto::DeleteNode &delete_node);

        int init_delete_info(const proto::UpdateNode &update_node);

        std::vector<SmartRecord> &get_real_delete_records() {
            return _del_scan_records;
        }

        int process_binlog(RuntimeState *state, bool is_local);

    private:
        int64_t _table_id = -1;
        int32_t _tuple_id = -1;
        std::vector<proto::SlotDescriptor> _primary_slots;
        SmartTable _table_info;
    };
}
