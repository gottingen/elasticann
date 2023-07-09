#include "elasticann/protocol/task_manager.h"
#include "elasticann/physical_plan/physical_planner.h"
#include "elasticann/session/network_socket.h"
#include "elasticann/logical_plan/ddl_work_planner.h"
#include "elasticann/protocol/network_server.h"

namespace EA {

    DEFINE_int32(worker_number, 20, "baikaldb worker number.");

    int TaskManager::init() {
        _workers.run([this]() {
            this->fetch_thread();
        });
        return 0;
    }

    void TaskManager::fetch_thread() {
        TLOG_WARN("fetch ok!");

        while (true) {
            //process ddlwork
            proto::RegionDdlWork work;
            if (TaskFactory<proto::RegionDdlWork>::get_instance()->fetch_task(work) != 0) {
                //TLOG_WARN("no write ddl region data task");
            } else {
                _workers.run([work, this]() {
                    process_ddl_work(work);
                });
            }

            // 等待事务完成任务
            proto::DdlWorkInfo txn_work;
            if (TaskFactory<proto::DdlWorkInfo>::get_instance()->fetch_task(txn_work) != 0) {
                TLOG_DEBUG("no wait txn done task");
            } else {
                _workers.run([txn_work, this]() {
                    process_txn_ddl_work(txn_work);
                });
            }
            bthread_usleep(5 * 1000 * 1000LL);
        }
    }

    void TaskManager::process_txn_ddl_work(proto::DdlWorkInfo work) {
        TLOG_INFO("process txn ddl work {}", work.ShortDebugString().c_str());
        TimeCost tc;

        while (true) {
            if (tc.get_time() > 30 * 60 * 1000 * 1000LL) {
                TLOG_WARN("time_out txn not ready.");
                work.set_status(proto::DdlWorkFail);
                break;
            }
            int64_t write_only_time = -1;
            {
                auto index_ptr = SchemaFactory::get_instance()->get_index_info_ptr(work.index_id());
                if (index_ptr != nullptr && index_ptr->write_only_time != -1) {
                    write_only_time = index_ptr->write_only_time;
                } else {
                    TLOG_INFO("wait ddl work {} txn done.", work.ShortDebugString().c_str());
                    bthread_usleep(30 * 1000 * 1000LL);
                    continue;
                }
            }
            auto epool_ptr = NetworkServer::get_instance()->get_epoll_info();
            if (epool_ptr != nullptr) {
                if (epool_ptr->all_txn_time_large_then(write_only_time, work.table_id())) {
                    TLOG_INFO("epool time write_only_time {}", write_only_time);
                    work.set_status(proto::DdlWorkDone);
                    break;
                } else {
                    TLOG_INFO("wait ddl work {} txn done.", work.ShortDebugString().c_str());
                    bthread_usleep(30 * 1000 * 1000LL);
                    continue;
                }
            }
        }
        if (TaskFactory<proto::DdlWorkInfo>::get_instance()->finish_task(work) != 0) {
            TLOG_WARN("finish work {} error", work.ShortDebugString().c_str());
        }
    }

    void TaskManager::process_ddl_work(proto::RegionDdlWork work) {
        TLOG_INFO("begin ddl work task_{}_{} : {}", work.table_id(), work.region_id(),
                  work.ShortDebugString().c_str());
        int ret = 0;
        SmartSocket client(new NetworkSocket);
        client->query_ctx->client_conn = client.get();
        client->is_index_ddl = true;
        client->server_instance_id = NetworkServer::get_instance()->get_instance_id();
        std::unique_ptr<DDLWorkPlanner> planner_ptr(new DDLWorkPlanner(client->query_ctx.get()));
        ret = planner_ptr->set_ddlwork(work);
        if (ret != 0) {
            TLOG_ERROR("ddl work[{}] set ddlwork fail.", work.ShortDebugString().c_str());
            work.set_status(proto::DdlWorkFail);
            TaskFactory<proto::RegionDdlWork>::get_instance()->finish_task(work);
            return;
        }
        ret = planner_ptr->plan();
        if (ret != 0) {
            TLOG_ERROR("ddl work[{}] fail plan error.", work.ShortDebugString().c_str());
            //plan失败，建二级索引初始化region失败。回滚，不重试
            work.set_status(proto::DdlWorkError);
            TaskFactory<proto::RegionDdlWork>::get_instance()->finish_task(work);
            return;
        }
        ret = planner_ptr->execute();
        if (ret == 0) {
            TLOG_INFO("process ddlwork {} success.", work.ShortDebugString().c_str());
        }

        if (TaskFactory<proto::RegionDdlWork>::get_instance()->finish_task(planner_ptr->get_ddlwork()) != 0) {
            TLOG_WARN("finish work error");
        }
        TLOG_INFO("ddl work task_{}_{} finish ok! {}", work.table_id(), work.region_id(),
                  work.ShortDebugString().c_str());
    }

} // namespace EA
