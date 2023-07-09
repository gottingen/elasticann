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
#include "elasticann/mem_row/mem_row_descriptor.h"
#include "elasticann/mem_row/mem_row.h"
#include "elasticann/engine/table_iterator.h"
#include <vector>

int main(int argc, char *argv[]) {
    EA::MemRowDescriptor *desc = new EA::MemRowDescriptor;

    std::vector<EA::proto::TupleDescriptor> tuple_desc;
    for (int idx = 0; idx < 10; idx++) {
        EA::proto::TupleDescriptor tuple;
        tuple.set_tuple_id(idx);
        tuple.set_table_id(idx);

        for (int jdx = 1; jdx <= 8; ++jdx) {
            EA::proto::SlotDescriptor *slot = tuple.add_slots();
            slot->set_slot_id(jdx);
            slot->set_slot_type(EA::proto::UINT16);
            slot->set_tuple_id(idx);
        }
        tuple_desc.push_back(tuple);
    }

    if (0 != desc->init(tuple_desc)) {
        TLOG_WARN("init failed");
        return -1;
    }

    std::vector<google::protobuf::Message *> messages;
    for (int idx = 0; idx < 10000000; ++idx) {
        auto msg = desc->new_tuple_message(idx % 10);
        messages.push_back(msg);
    }

    TLOG_WARN("create message success");

    sleep(20);

    for (auto msg: messages) {
        delete msg;
    }
    delete desc;

    TLOG_WARN("delete message success");

    sleep(30);

    return 0;
}
