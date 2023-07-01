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

#include "elasticann/common/common.h"

namespace EA {

inline std::string get_task_id(const proto::RegionDdlWork& work) {
    return std::to_string(work.table_id()) + "_" + std::to_string(work.region_id());
}

inline std::string get_task_id(const proto::DdlWorkInfo& work) {
    return std::to_string(work.table_id());
}

template<typename TaskType>
class TaskFactory : public Singleton<TaskFactory<TaskType>> {
public:
    int fetch_task(TaskType& task);
    int finish_task(const TaskType& task);
    int process_ddl_work(const TaskType& work);
    int process_heartbeat(const proto::BaikalHeartBeatResponse& response,
        const google::protobuf::RepeatedPtrField<TaskType>& (proto::BaikalHeartBeatResponse::*method)() const);
    int construct_heartbeat(proto::BaikalHeartBeatRequest& request, TaskType* (proto::BaikalHeartBeatRequest::*method)());

private:
    std::unordered_map<std::string, TaskType> _todo_tasks; 
    std::unordered_map<std::string, TaskType> _doing_tasks; 
    std::unordered_map<std::string, TaskType> _done_tasks; 
    bthread::Mutex _mutex;
};
}

#include "task_fetcher.hpp"
