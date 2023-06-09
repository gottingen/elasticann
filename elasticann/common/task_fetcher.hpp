namespace EA {

    template<typename TaskType>
    int TaskFactory<TaskType>::process_heartbeat(const proto::BaikalHeartBeatResponse &response,
                                                 const google::protobuf::RepeatedPtrField<TaskType> &(proto::BaikalHeartBeatResponse::*method)() const) {
        for (const auto &ddl_work: (response.*method)()) {
            process_ddl_work(ddl_work);
        }
        return 0;
    }

    template<typename TaskType>
    int TaskFactory<TaskType>::fetch_task(TaskType &task) {
        BAIDU_SCOPED_LOCK(_mutex);
        if (_todo_tasks.size() == 0) {
            return -1;
        }

        auto iter = _todo_tasks.begin();
        task = iter->second;
        _doing_tasks.insert(*iter);
        _todo_tasks.erase(iter);
        return 0;
    }

    template<typename TaskType>
    int TaskFactory<TaskType>::finish_task(const TaskType &task) {
        BAIDU_SCOPED_LOCK(_mutex);
        auto task_id = get_task_id(task);
        _doing_tasks.erase(task_id);
        _done_tasks.emplace(task_id, task);
        return 0;
    }

    template<typename TaskType>
    int TaskFactory<TaskType>::process_ddl_work(const TaskType &task) {
        BAIDU_SCOPED_LOCK(_mutex);
        auto task_id = get_task_id(task);
        if (_todo_tasks.count(task_id) == 1) {
            TLOG_INFO("task {} is running.", task_id);
        } else {
            _todo_tasks.emplace(task_id, task);
        }
        return 0;
    }

    template<typename TaskType>
    int TaskFactory<TaskType>::construct_heartbeat(proto::BaikalHeartBeatRequest &request,
                                                   TaskType *(proto::BaikalHeartBeatRequest::*method)()) {
        BAIDU_SCOPED_LOCK(_mutex);
        for (auto iter = _done_tasks.begin(); iter != _done_tasks.end();) {
            auto add_ddl_work = (request.*method)();
            add_ddl_work->CopyFrom(iter->second);
            iter = _done_tasks.erase(iter);
        }
        for (auto iter = _doing_tasks.begin(); iter != _doing_tasks.end(); iter++) {
            auto add_ddl_work = (request.*method)();
            add_ddl_work->CopyFrom(iter->second);
        }
        for (auto iter = _todo_tasks.begin(); iter != _todo_tasks.end(); iter++) {
            auto add_ddl_work = (request.*method)();
            add_ddl_work->CopyFrom(iter->second);
        }
        TLOG_DEBUG("heartbeat {}", request.ShortDebugString());
        return 0;
    }
}
