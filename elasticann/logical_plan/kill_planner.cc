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


#include "elasticann/logical_plan/kill_planner.h"
#include "eaproto/db/plan.pb.h"
#include "elasticann/session/network_socket.h"
#include "elasticann/protocol/network_server.h"
#include "elasticann/protocol/state_machine.h"

namespace EA {

    int KillPlanner::plan() {
        create_packet_node(proto::OP_KILL);
        auto client = _ctx->client_conn;
        parser::KillStmt *k = (parser::KillStmt *) _ctx->stmt;
        proto::PlanNode *kill_node = _ctx->add_plan_node();
        kill_node->set_node_type(proto::KILL_NODE);
        kill_node->set_limit(-1);
        kill_node->set_num_children(0);
        proto::DerivePlanNode *derive = kill_node->mutable_derive_node();
        proto::KillNode *kill = derive->mutable_kill_node();
        uint64_t instance_part = client->server_instance_id & 0x7FFFFF;
        uint64_t db_conn_id = (instance_part << 40 | (k->conn_id & 0xFFFFFFFFFFUL));
        kill->set_db_conn_id(db_conn_id);
        kill->set_is_query(k->is_query);

        EpollInfo *epoll_info = NetworkServer::get_instance()->get_epoll_info();
        TLOG_WARN("kill {}", k->conn_id);
        for (int32_t idx = 0; idx < CONFIG_MPL_EPOLL_MAX_SIZE; ++idx) {
            SmartSocket sock = epoll_info->get_fd_mapping(idx);
            if (sock == nullptr || sock->is_free || sock->fd == -1 || sock->ip == "") {
                continue;
            }
            if (sock->conn_id == k->conn_id) {
                TLOG_WARN("conn_id equal {} is_query:{}", k->conn_id, k->is_query);
                _ctx->kill_ctx = sock->query_ctx;
                _ctx->kill_ctx->kill_all_ctx();
                // kill xxx 复用client_free,会导致被kill的sock的DataBuffer被继续占用，导致下一次建立连接失败
                // 但是kill指令用的很少，后续再考虑优化
                // kill query xx没问题
                if (!k->is_query) {
//                client->state = STATE_ERROR;
                    StateMachine::get_instance()->client_free(sock, epoll_info);
                }
                break;
            }
        }
        return 0;
    }
} // end of namespace EA
