#pragma once

#include "elasticann/common/common.h"
#include "elasticann/common/task_fetcher.h"
#include "elasticann/base/concurrency.h"

namespace EA {

    DECLARE_int32(worker_number);

    class TaskManager : public Singleton<TaskManager> {
    public:
        int init();

        void fetch_thread();

        void process_ddl_work(proto::RegionDdlWork work);

        void process_txn_ddl_work(proto::DdlWorkInfo work);

    private:
        ConcurrencyBthread _workers{FLAGS_worker_number};
    };

} // namespace EA
