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


#pragma once

#include <unordered_map>
#include "elasticann/meta_server/base_state_machine.h"

namespace EA {
    class AutoIncrStateMachine : public EA::BaseStateMachine {
    public:

        AutoIncrStateMachine(const braft::PeerId &peerId) :
                BaseStateMachine(1, "auto_incr_raft", "/auto_incr", peerId) {}

        virtual ~AutoIncrStateMachine() {}

        // state machine method
        virtual void on_apply(braft::Iterator &iter);

        void add_table_id(const proto::MetaManagerRequest &request, braft::Closure *done);

        void drop_table_id(const proto::MetaManagerRequest &request, braft::Closure *done);

        void gen_id(const proto::MetaManagerRequest &request, braft::Closure *done);

        void update(const proto::MetaManagerRequest &request, braft::Closure *done);

        virtual void on_snapshot_save(braft::SnapshotWriter *writer, braft::Closure *done);

        virtual int on_snapshot_load(braft::SnapshotReader *reader);

    private:
        void save_auto_increment(std::string &max_id_string);

        void save_snapshot(braft::Closure *done,
                           braft::SnapshotWriter *writer,
                           std::string max_id_string);

        int load_auto_increment(const std::string &max_id_file);

        int parse_json_string(const std::string &json_string);

        std::unordered_map<int64_t, uint64_t> _auto_increment_map;
    };

} //namespace EA

