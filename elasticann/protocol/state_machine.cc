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


#include "elasticann/protocol/state_machine.h"
#include "elasticann/protocol/network_server.h"
#include "elasticann/logical_plan/query_context.h"
#include "elasticann/common/store_interact.h"
#include <rapidjson/reader.h>
#include <rapidjson/document.h>
#include "re2/re2.h"
#include "turbo/strings/str_split.h"
#include "turbo/strings/utility.h"
#include "turbo/format/format.h"

namespace EA {
    DEFINE_int32(max_connections_per_user, 4000, "default user max connections");
    DEFINE_int32(query_quota_per_user, 3000, "default user query quota by 1 second");
    DEFINE_int64(baikal_max_allowed_packet, 268435456LL, "The largest possible packet : 256M");
    DECLARE_int64(print_time_us);
    DECLARE_int32(baikal_port);
    DECLARE_bool(open_to_collect_slow_query_infos);
    DECLARE_int32(slow_query_timeout_s);

    void StateMachine::run_machine(SmartSocket client,
                                   EpollInfo *epoll_info,
                                   bool shutdown) {

        switch (client->state) {
            case STATE_CONNECTED_CLIENT: {
                if (shutdown) {
                    client->state = STATE_ERROR;
                    run_machine(client, epoll_info, shutdown);
                    break;
                }
                // Send handshake package.
                TimeCost cost;
                int ret = _wrapper->handshake_send(client);
                if (ret == RET_SUCCESS) {
                    client->state = STATE_SEND_HANDSHAKE;
                    epoll_info->poll_events_mod(client, EPOLLIN);
                    TLOG_WARN("{}, handshake_send success",*client);
                } else if (ret == RET_WAIT_FOR_EVENT) {
                    epoll_info->poll_events_mod(client, EPOLLOUT);
                } else {
                    TLOG_ERROR("{}, Failed to send handshake packet to client."
                               "state={}, ret={}, errno={}", *client, static_cast<int>(client->state), ret, errno);
                    client->state = STATE_ERROR;
                    run_machine(client, epoll_info, shutdown);
                }
                gettimeofday(&(client->connect_time), nullptr);
                break;
            }
            case STATE_SEND_HANDSHAKE: {
                if (shutdown) {
                    TLOG_WARN("{}, socket is going to shutdown.",*client);
                    client->state = STATE_ERROR;
                    run_machine(client, epoll_info, shutdown);
                    break;
                }
                // Read auth info.
                TimeCost cost;
                int go_on = 0;
                //auth user password and ip
                static bvar::LatencyRecorder auth_read_time_cost("auth_read_time_cost");
                int ret = _auth_read(client);
                auth_read_time_cost << cost.get_time();
                if (ret == RET_SUCCESS) {
                    TLOG_WARN("{}, auth read success, cost:{}", *client,cost.get_time());
                    if (!client->user_info->connection_inc()) {
                        char msg[256];
                        snprintf(msg, 256, "Username {} has reach the max connection limit({})",
                                 client->username.c_str(),
                                 client->user_info->max_connection);
                        if (_wrapper->fill_auth_failed_packet(client, msg) != RET_SUCCESS) {
                            TLOG_WARN("{}, Failed to fill auth failed message.",*client);
                        }
                        TLOG_WARN("Username {} has reach the max connection limit({})",
                                  client->username.c_str(),
                                  client->user_info->max_connection);
                        client->state = STATE_ERROR;
                        run_machine(client, epoll_info, shutdown);
                        break;
                    }
                    client->is_counted = true;
                    client->state = STATE_READ_AUTH;
                    go_on = 1;
                } else if (ret == RET_AUTH_FAILED) {
                    char msg[256];
                    snprintf(msg, 256, "Access denied for user '{}'@'{}' (using password: YES)",
                             client->username.c_str(), client->ip.c_str());
                    if (_wrapper->fill_auth_failed_packet(client, msg) != RET_SUCCESS) {
                        TLOG_WARN("{}, Failed to fill auth failed message.",*client);
                    }
                    client->state = STATE_ERROR;
                    run_machine(client, epoll_info, shutdown);
                } else if (ret == RET_WAIT_FOR_EVENT) { // Read auth info partly.
                    TLOG_WARN("{}, Read auth info partly, go on reading. ",*client);
                    epoll_info->poll_events_mod(client, EPOLLIN);
                } else {
                    //ret == RET_SHUTDOWN or others
                    TLOG_WARN("{}, read auth packet from client error: "
                                              "state={} ret={}, errno={}",*client,
                                      static_cast<int>(client->state), ret, errno);
                    client->state = STATE_ERROR;
                    run_machine(client, epoll_info, shutdown);
                }
                gettimeofday(&(client->connect_time), nullptr);
                // If auth is ok, go on doing next status.
                if (go_on == 0) { break; }
            }
            case STATE_READ_AUTH: {
                if (shutdown) {
                    client->state = STATE_ERROR;
                    run_machine(client, epoll_info, shutdown);
                    break;
                }
                // Send auth result.
                TimeCost cost;
                int ret = _wrapper->auth_result_send(client);
                if (ret == RET_SUCCESS) {
                    client->state = STATE_SEND_AUTH_RESULT;
                    epoll_info->poll_events_mod(client, EPOLLIN);
                } else if (ret == RET_WAIT_FOR_EVENT) {
                    TLOG_WARN("{}, send auth info partly, go on sending.",*client);
                    epoll_info->poll_events_mod(client, EPOLLOUT);
                } else {
                    TLOG_ERROR("{}, send auth result packet to client error "
                               "state={} ret={},errno={}", *client,
                               static_cast<int>(client->state), ret, errno);
                    client->state = STATE_ERROR;
                    run_machine(client, epoll_info, shutdown);
                }
                client->is_authed = true;
                break;
            }
            case STATE_SEND_AUTH_RESULT: {
                if (shutdown) {
                    client->state = STATE_ERROR;
                    run_machine(client, epoll_info, shutdown);
                    break;
                }
                gettimeofday(&(client->query_ctx->stat_info.start_stamp), nullptr);
                // Read query.
                TimeCost cost_read;
                int ret = _query_read(client);
                client->query_ctx->stat_info.query_read_time = cost_read.get_time();
                if (ret == RET_SUCCESS) {
                } else if (ret == RET_CMD_DONE) {
                    client->state = STATE_READ_QUERY_RESULT;
                    run_machine(client, epoll_info, shutdown);
                    break;
                } else if (ret == RET_WAIT_FOR_EVENT) {
                    epoll_info->poll_events_mod(client, EPOLLIN);
                    break;
                } else if (ret == RET_COMMAND_SHUTDOWN || ret == RET_SHUTDOWN) {
                    TLOG_TRACE("{}, Connect is closed by client.", *client);
                    client->state = STATE_ERROR;
                    run_machine(client, epoll_info, shutdown);
                    break;
                } else if (ret == RET_CMD_UNSUPPORT) {
                    TLOG_WARN("{}, un-supported query type.",*client);
                    client->state = STATE_ERROR_REUSE;
                    run_machine(client, epoll_info, shutdown);
                    break;
                } else {
                    TLOG_ERROR("{}, read query from client error "
                               "state={}, ret={}, errno={}", *client, static_cast<int>(client->state), ret, errno);
                    _wrapper->make_err_packet(client,
                                              ER_ERROR_ON_READ,
                                              "read query from client error, errno: {}-{}",
                                              errno,
                                              strerror(errno));
                    client->state = STATE_ERROR;
                    run_machine(client, epoll_info, shutdown);
                    break;
                }

                //auto query_ctx = client->query_ctx;
                //stat_info = &(query_ctx->stat_info);
                // Process query.
                bool res = _query_process(client);
                if (!res || STATE_ERROR == client->state || STATE_ERROR_REUSE == client->state) {
                    TLOG_WARN("{}, handle query failed. sql=[{}] state:{}",*client,
                                      client->query_ctx->sql.c_str(), static_cast<int>(client->state));
                    _wrapper->make_err_packet(client, ER_ERROR_COMMON, "handle query failed");
                    client->state = (client->state == STATE_ERROR) ? STATE_ERROR : STATE_ERROR_REUSE;
                    _print_query_time(client);
                    run_machine(client, epoll_info, shutdown);
                } else if (STATE_READ_QUERY == client->state) {
                    // Set client socket event 0.
                    epoll_info->poll_events_mod(client, 0);
                } else if (STATE_READ_QUERY_RESULT == client->state) {
                    //epoll_info->poll_events_mod(client, EPOLLOUT);
                    gettimeofday(&(client->query_ctx->stat_info.send_stamp), nullptr); // start send
                    run_machine(client, epoll_info, shutdown);
                } else if (STATE_SEND_AUTH_RESULT == client->state) {
                    epoll_info->poll_events_mod(client, EPOLLIN);
                } else {
                    TLOG_ERROR("{}, handle should not return state[{}]", *client, static_cast<int>(client->state));
                    _wrapper->make_err_packet(client, ER_ERROR_COMMON, "expected return state");
                    client->state = STATE_ERROR;
                    _print_query_time(client);
                    run_machine(client, epoll_info, shutdown);
                }
                break;
            }
            case STATE_READ_QUERY_RESULT_MORE: {
                if (shutdown) {
                    client->state = STATE_ERROR;
                    run_machine(client, epoll_info, shutdown);
                    break;
                }
                int ret = 0;
                ret = _send_result_to_client_and_reset_status(epoll_info, client);
                if (ret == RET_WAIT_FOR_EVENT) {
                    TLOG_WARN("{}, send partly, wait for fd ready.",*client);
                    break;
                } else if (ret < 0 || ret == RET_SHUTDOWN) {
                    TLOG_WARN("{}, handle query failed. sql=[{}]",*client,
                                      client->query_ctx->sql.c_str());
                    client->state = STATE_ERROR;
                    _print_query_time(client);
                    run_machine(client, epoll_info, shutdown);
                } else if (client->state == STATE_SEND_AUTH_RESULT) {
                    _print_query_time(client);
                    break;
                } else if (ret == RET_SUCCESS) {
                    do {
                        ret = _query_more(client, shutdown);
                        if (ret >= 0) {
                            ret = _send_result_to_client_and_reset_status(epoll_info, client);
                        } else {
                            TLOG_WARN("{}, query_more failed sql=[{}]", *client,client->query_ctx->sql.c_str());
                            break;
                        }
                    } while (ret == 0 && client->state == STATE_READ_QUERY_RESULT_MORE);
                }
                if (ret == RET_WAIT_FOR_EVENT) {
                    TLOG_WARN("{}, send partly, wait for fd ready.",*client);
                } else if (client->state == STATE_SEND_AUTH_RESULT) {
                    _print_query_time(client);
                    // query结束后及时释放内存
                    client->reset_query_ctx(new(std::nothrow)QueryContext(client->user_info, client->current_db));
                } else if (ret < 0 || client->state == STATE_ERROR || ret == RET_SHUTDOWN) {
                    TLOG_WARN("{}, handle query failed. sql=[{}]",*client,
                                      client->query_ctx->sql.c_str());
                    client->state = STATE_ERROR;
                    _print_query_time(client);
                    run_machine(client, epoll_info, shutdown);
                }
                break;
            }
            case STATE_READ_QUERY_RESULT: {
                if (shutdown) {
                    client->state = STATE_ERROR;
                    run_machine(client, epoll_info, shutdown);
                    break;
                }
                //send result to client, and reset client status
                int ret = _send_result_to_client_and_reset_status(epoll_info, client);
                // result send out
                if (ret < 0 || ret == RET_SHUTDOWN) {
                    TLOG_WARN("{}, handle query failed. sql=[{}]",*client,
                                      client->query_ctx->sql.c_str());
                    client->state = STATE_ERROR;
                    _print_query_time(client);
                    run_machine(client, epoll_info, shutdown);
                } else if (client->state == STATE_SEND_AUTH_RESULT) {
                    _print_query_time(client);
                    // query结束后及时释放内存
                    client->reset_query_ctx(new(std::nothrow)QueryContext(client->user_info, client->current_db));
                } else if (client->state == STATE_READ_QUERY_RESULT && ret == RET_WAIT_FOR_EVENT) {
                    TLOG_WARN("{}, send partly, wait for fd ready.",*client);
                } else if (client->state == STATE_READ_QUERY_RESULT_MORE) {
                    run_machine(client, epoll_info, shutdown);
                }
                break;
            }
            case STATE_ERROR_REUSE: {
                if (shutdown) {
                    client->state = STATE_ERROR;
                    run_machine(client, epoll_info, shutdown);
                    break;
                }
                _query_result_send(client);
                client->reset_when_err();
                client->state = STATE_SEND_AUTH_RESULT;
                epoll_info->poll_events_mod(client, EPOLLIN);
                break;
            }
            case STATE_ERROR: {
                _query_result_send(client);
                //Scheduler::get_instance()->disconnect(client->fd);
                client_free(client, epoll_info);
                break;
            }
            default: {
                TLOG_ERROR("unknown state[{}]", static_cast<int>(client->state));
                break;
            }
        }
        return;
    }

    int StateMachine::_query_more(SmartSocket client, bool shutdown) {
        TimeCost cost;
        int ret = 0;
        shutdown = shutdown || client->state == STATE_ERROR;
        ret = PhysicalPlanner::full_export_next(client->query_ctx.get(), client->send_buf, shutdown);
        if (ret < 0) {
            TLOG_WARN("{}, Failed to PhysicalPlanner::batch_execute: {}",*client,
                              client->query_ctx->sql.c_str());
            if (client->query_ctx->stat_info.error_code == ER_ERROR_FIRST) {
                client->query_ctx->stat_info.error_code = ER_EXEC_PLAN_FAILED;
                client->query_ctx->stat_info.error_msg << "exec physical plan failed";
            }
            _wrapper->make_err_packet(client,
                                      client->query_ctx->stat_info.error_code, "{}",
                                      client->query_ctx->stat_info.error_msg.str().c_str());

            return ret;
        }
        client->query_ctx->stat_info.query_exec_time += cost.get_time();
        client->query_ctx->stat_info.send_buf_size += client->send_buf->_size;
        return 0;
    }

    void StateMachine::_print_query_time(SmartSocket client) {
        auto ctx = client->query_ctx;
        auto stat_info = &(ctx->stat_info);
        gettimeofday(&(stat_info->end_stamp), nullptr);
        stat_info->result_send_time = timestamp_diff(
                stat_info->send_stamp, stat_info->end_stamp);
        stat_info->total_time = timestamp_diff(
                stat_info->start_stamp, stat_info->end_stamp);
        PacketNode *root = (PacketNode *) (ctx->root);
        int64_t rows = 0;
        proto::OpType op_type = proto::OP_NONE;
        if (root != nullptr) {
            op_type = root->op_type();
            rows = (root->op_type() == proto::OP_SELECT || root->op_type() == proto::OP_UNION) ?
                   stat_info->num_returned_rows : stat_info->num_affected_rows;
        }

        if (ctx->mysql_cmd == COM_QUERY
            || ctx->mysql_cmd == COM_STMT_EXECUTE) {
            // PREPARE不应该统计进去
            // 降级去备库后不再统计，table_id序列不一样，防止再次判断降级流程
            if (!ctx->use_backup) {
                int64_t index_id = 0; //0 没有使用索引，否则选index_ids中的第一个，对于join涉及多个索引可能展示不完整 TODO
                int64_t err_count = stat_info->error_code != 1000;
                if (ctx->index_ids.size() > 1) {
                    index_id = *ctx->index_ids.begin();
                } else if (ctx->index_ids.size() == 1) {
                    index_id = *ctx->index_ids.begin();
                    /*
                    index_recommend_st << BvarMap(stat_info->sample_sql.str(), index_id, stat_info->table_id,
                            stat_info->total_time, err_count * stat_info->total_time, rows, stat_info->num_scan_rows,
                            stat_info->num_filter_rows, stat_info->region_count,
                            ctx->field_range_type, err_count);
                    */
                }
                std::map<int32_t, int> field_range_type;
                auto subquery_signs = client->get_subquery_signs();
                sql_agg_cost << BvarMap(stat_info->sample_sql.str(), index_id, stat_info->table_id,
                                        stat_info->total_time, err_count * stat_info->total_time, rows,
                                        stat_info->num_scan_rows,
                                        stat_info->num_filter_rows, stat_info->region_count,
                                        field_range_type, err_count, stat_info->sign, subquery_signs);
            }

            if (op_type == proto::OP_SELECT) {
                select_time_cost << stat_info->total_time;
                std::unique_lock<std::mutex> lock(_mutex);
                if (select_by_users.find(client->username) == select_by_users.end()) {
                    select_by_users[client->username].reset(new bvar::LatencyRecorder("select_" + client->username));
                }
                (*select_by_users[client->username]) << stat_info->total_time;
            } else if (op_type == proto::OP_INSERT ||
                       op_type == proto::OP_UPDATE ||
                       op_type == proto::OP_DELETE) {
                dml_time_cost << stat_info->total_time;
                std::unique_lock<std::mutex> lock(_mutex);
                if (dml_by_users.find(client->username) == dml_by_users.end()) {
                    dml_by_users[client->username].reset(new bvar::LatencyRecorder("dml_" + client->username));
                }
                (*dml_by_users[client->username]) << stat_info->total_time;
            }
            if (stat_info->error_code != 1000) {
                sql_error << 1;
                if (stat_info->error_code == 10004) {
                    exec_sql_error << 1;
                }
            }
            if (stat_info->txn_alive_time > 0) {
                txn_alive_time_cost << stat_info->txn_alive_time;
            }
        }

        if (ctx->mysql_cmd == COM_QUERY
            || ctx->mysql_cmd == COM_STMT_PREPARE
            || ctx->mysql_cmd == COM_STMT_EXECUTE
            || ctx->mysql_cmd == COM_STMT_CLOSE
            || ctx->mysql_cmd == COM_STMT_RESET
            || ctx->mysql_cmd == COM_STMT_SEND_LONG_DATA) {
            std::string namespace_name = client->user_info->namespace_;
            std::string database = namespace_name + "." + stat_info->family;
            if (stat_info->family.empty()) {
                stat_info->family = "no";
                database += "adp";
            }
            if (stat_info->table.empty()) {
                stat_info->table = "no";
            }
            if (stat_info->total_time > FLAGS_print_time_us || stat_info->error_code != 1000) {
                std::string sql;
                if (ctx->mysql_cmd == COM_QUERY || ctx->mysql_cmd == COM_STMT_CLOSE
                    || ctx->mysql_cmd == COM_STMT_RESET) {
                    sql = ctx->sql;
                } else {
                    auto iter = client->prepared_plans.find(ctx->prepare_stmt_name);
                    if (iter != client->prepared_plans.end()) {
                        sql = iter->second->sql;
                    }
                }
                size_t slow_idx = 0;
                bool is_blank = false;
                for (size_t i = 0; i < sql.size(); i++) {
                    if (sql[i] == ' ' || sql[i] == '\t' || sql[i] == '\n') {
                        if (!is_blank) {
                            sql[slow_idx++] = ' ';
                            is_blank = true;
                        }
                    } else {
                        is_blank = false;
                        sql[slow_idx++] = sql[i];
                    }
                }
                sql.resize(slow_idx);
                TLOG_INFO(
                        "bthread:{} common_query: family=[{}] table=[{}] op_type=[{}] cmd=[{:#x}] plat=[{}] ip=[{}:{}] fd=[{}] "
                        "cost=[{}] field_time=[{} {} {} {} {} {} {} {} {}] row=[{}] scan_row=[{}] bufsize=[{}] "
                        "key=[{}] changeid=[{}] logid=[{}] traceid=[{}] family_ip=[{}] cache=[{}] stmt_name=[{}] "
                        "user=[{}] charset=[{}] errno=[{}] txn=[{}:{}] 1pc=[{}] sign=[{}] region_count=[{}] sqllen=[{}] "
                        "sql=[{}] id=[{}] bkup=[{}] server_addr=[{}:{}]",
                        bthread_self(),
                        stat_info->family.c_str(),
                        stat_info->table.c_str(),
                        op_type,
                        ctx->mysql_cmd,
                        FLAGS_log_plat_name.c_str(),
                        client->ip.c_str(),
                        client->port,
                        client->fd,
                        stat_info->total_time,
                        stat_info->query_read_time,
                        stat_info->query_plan_time,
                        stat_info->query_exec_time,
                        stat_info->result_pack_time,
                        stat_info->result_send_time,
                        stat_info->server_talk_time,
                        stat_info->buf_to_res_time,
                        stat_info->res_to_table_time,
                        stat_info->table_get_row_time,
                        rows,
                        stat_info->num_scan_rows,
                        stat_info->send_buf_size,
                        stat_info->partition_key,
                        stat_info->version,
                        stat_info->log_id,
                        stat_info->trace_id.c_str(),
                        stat_info->server_ip.c_str(),
                        stat_info->hit_cache,
                        ctx->prepare_stmt_name.c_str(),
                        client->username.c_str(),
                        client->charset_name.c_str(),
                        stat_info->error_code,
                        stat_info->old_txn_id,
                        stat_info->old_seq_id,
                        ctx->get_runtime_state()->optimize_1pc(),
                        stat_info->sign,
                        stat_info->region_count,
                        sql.length(),
                        sql.c_str(),
                        client->last_insert_id,
                        ctx->use_backup, butil::my_ip_cstr(), FLAGS_baikal_port);
            }
        } else {
            if ('\x0e' == ctx->mysql_cmd) {
                TLOG_DEBUG("stmt_query ip=[{}:{}] fd=[{}] cost=[{}] key=[{}] "
                           "cmd=[{}] type=[{}] user=[{}]",
                           client->ip.c_str(),
                           client->port,
                           client->fd,
                           stat_info->total_time,
                           stat_info->partition_key,
                           ctx->mysql_cmd,
                           ctx->type,
                           client->username.c_str());
            } else {
                TLOG_DEBUG("stmt_query ip=[{}:{}] fd=[{}] cost=[{}] key=[{}] "
                           "cmd=[{}] type=[{}] user=[{}]",
                           client->ip.c_str(),
                           client->port,
                           client->fd,
                           stat_info->total_time,
                           stat_info->partition_key,
                           ctx->mysql_cmd,
                           ctx->type,
                           client->username.c_str());
            }
        }
        SchemaFactory::use_backup.set_bthread_local(false);
        ctx->mysql_cmd = COM_SLEEP;
        client->last_active = time(nullptr);
        return;
    }

    int StateMachine::_auth_read(SmartSocket sock) {
        if (!sock) {
            TLOG_ERROR("sock==nullptr");
            return RET_ERROR;
        }
        // Read packet to socket self buffer.
        int ret = _read_packet(sock);
        if (RET_SUCCESS != ret) {
            // Using debug log because of shutdown by client is normal,so no need to fatal.
            TLOG_DEBUG("{}, Failed to read packet", *sock);
            return ret;
        }
        // Get charset.
        uint8_t *packet = sock->self_buf->_data;
        uint32_t off = PACKET_HEADER_LEN;
        uint64_t capability = 0;
        if (RET_SUCCESS !=
            _wrapper->protocol_get_length_fixed_int(packet, sock->packet_len + PACKET_HEADER_LEN, off, 4, capability)) {
            TLOG_WARN("read capability failed");
            return RET_ERROR;
        }

        off = PACKET_HEADER_LEN + 8;
        uint8_t charset_num = 0;
        if (RET_SUCCESS !=
            _wrapper->protocol_get_char(packet, sock->packet_len + PACKET_HEADER_LEN, off, &charset_num)) {
            TLOG_ERROR("{}, get charset_num failed, off={}, len=1", *sock, off);
            return RET_ERROR;
        }
        if (charset_num == 28) {
            sock->charset_name = "gbk";
            sock->charset_num = 28;
        } else if (charset_num == 33) {
            sock->charset_name = "utf8";
            sock->charset_num = 33;
        } else {
            TLOG_TRACE("{}, unknown charset num: {}, charset will be set as gbk.",*sock,
                            charset_num);
            sock->charset_name = "gbk";
            sock->charset_num = 28;
        }
        off += 23;

        // Get user name.
        std::string username;
        if (0 != _wrapper->protocol_get_string(packet,
                                               sock->packet_len + PACKET_HEADER_LEN, off, username)) {
            TLOG_ERROR("{}, Username is null", *sock);
            return RET_AUTH_FAILED;
        }
        //需要修改成权限类
        SchemaFactory *factory = SchemaFactory::get_instance();
        sock->user_info = factory->get_user_info(username);

        if (sock->user_info == nullptr) {
            sock->user_info.reset(new UserInfo);
            TLOG_WARN("user name not exist [{}]", username.c_str());
            return RET_AUTH_FAILED;
        }
        if (sock->user_info->username.empty()) {
            TLOG_WARN("{}, user name not exist [{}]",*sock, username.c_str());
            return RET_AUTH_FAILED;
        }
        sock->username = sock->user_info->username;
        if (sock->user_info->max_connection == 0) {
            //use default max_connection
            sock->user_info->max_connection = FLAGS_max_connections_per_user;
        }
        if (sock->user_info->query_quota == 0) {
            //use default query_quota
            sock->user_info->query_quota = FLAGS_query_quota_per_user;
        }
        // Get password.
        if ((unsigned int) (sock->packet_len + PACKET_HEADER_LEN) < off + 1) {
            TLOG_ERROR("{}, packet_len={} + 4 <= off={} + 1", *sock,
                       sock->packet_len, off);
            return RET_ERROR;
        }
        uint8_t len = packet[off];
        off++;
        if (len == '\x00') {
            TLOG_WARN("{}, Password len is:[{}]", *sock,len);
            return RET_AUTH_FAILED;
        } else if (len == '\x14') {
            if ((unsigned int) (sock->packet_len + PACKET_HEADER_LEN) < (20 + off)) {
                TLOG_ERROR("s->packet_len={} + PACKET_HEADER_LEN=4 < 20 + off={}",
                           sock->packet_len, off);
                return RET_ERROR;
            }
            for (int idx = 0; idx < 20; idx++) {
                if (*(packet + off + idx) != *(sock->user_info->scramble_password + idx)) {
                    TLOG_WARN("{}, client connect Baikal with wrong password",*sock);
                    return RET_AUTH_FAILED;
                }
            }
            off += 20;
        } else {
            TLOG_WARN("{}, client connect Baikal with wrong password, "
                                    "client->scramble_len={} should be 0 or 20", *sock,len);
            return RET_AUTH_FAILED;
        }

        if (!sock->user_info->allow_addr(sock->ip)) {
            TLOG_WARN("{}, client connect Baikal with invalid ip", *sock);
            return RET_AUTH_FAILED;
        }

        // set current_db
        sock->current_db.clear();
        if (capability & CLIENT_CONNECT_WITH_DB) {
            if (0 != _wrapper->protocol_get_string(packet,
                                                   sock->packet_len + PACKET_HEADER_LEN, off, sock->current_db)) {
                TLOG_ERROR("{}, current_db is wrong", *sock);
                return RET_AUTH_FAILED;
            }
        }
        return RET_SUCCESS;
    }

    int StateMachine::_read_packet_header(SmartSocket sock) {
        int ret = RET_SUCCESS;
        int read_len = 0;
        ret = _wrapper->real_read_header(sock,
                                         PACKET_HEADER_LEN - sock->header_read_len,
                                         &read_len);

        sock->header_read_len += read_len;
        if (ret == RET_WAIT_FOR_EVENT) {
            TLOG_TRACE("{}, Read is interrupt by event.", *sock);
            return ret;
        } else if (ret != RET_SUCCESS) {
            if (read_len == 0) {
                TLOG_DEBUG("{}, Read length is 0. want_len:[{}],real_len:[{}]", *sock,
                           PACKET_HEADER_LEN - sock->header_read_len, read_len);
            } else {
                TLOG_ERROR("{}, Failed to read head. want_len:[{}],real_len:[{}]", *sock,
                           PACKET_HEADER_LEN - sock->header_read_len, read_len);
            }
            return ret;
        } else if (sock->header_read_len < 4) {
            TLOG_ERROR("{}, Read head wait for event.want_len:[{}],real_len:[{}]", *sock,
                       PACKET_HEADER_LEN - sock->header_read_len, read_len);
            return RET_WAIT_FOR_EVENT;
        }

        uint8_t *header = nullptr;
        header = sock->self_buf->_data;
        sock->current_packet_len = header[0] | header[1] << 8 | header[2] << 16;
        sock->packet_len += sock->current_packet_len;
        sock->packet_id = header[3];
        sock->last_packet_id = header[3];
        memset(sock->self_buf->_data, 0, PACKET_HEADER_LEN);
        if (sock->current_packet_len == (int) PACKET_LEN_MAX) { // if packet >= 16M need read next packet
            sock->has_multi_packet = true;
        } else {
            sock->has_multi_packet = false;
        }
        return RET_SUCCESS;
    }

    int StateMachine::_read_packet(SmartSocket sock) {
        if (!sock || !sock->self_buf) {
            TLOG_ERROR("sock == nullptr || self_buf == nullptr");
            return RET_ERROR;
        }
        int ret = RET_SUCCESS;
        int read_len = 0;
        do {
            if (sock->header_read_len != 4) {
                ret = _read_packet_header(sock);
                if (ret != RET_SUCCESS) {
                    TLOG_TRACE("{}, Read packet header not ok ret:{}.", *sock,ret);
                    return ret;
                }
            }
            read_len = 0;
            ret = _wrapper->real_read(sock, sock->current_packet_len - sock->packet_read_len, &read_len);

            sock->packet_read_len += read_len;
            if (ret == RET_WAIT_FOR_EVENT) {
                TLOG_TRACE("{}, Read is interrupt by event.",*sock);
                return ret;
            } else if (ret != RET_SUCCESS) {
                TLOG_WARN("{}, Failed to read body.want_len:[{}],real_len:[{}]",*sock,
                                  sock->current_packet_len - sock->packet_read_len, read_len);
                return ret;
            } else if (sock->current_packet_len > sock->packet_read_len) {
                TLOG_WARN("{}, Read body wait for event.want_len:[{}],real_len:[{}]",
                                  sock->current_packet_len - sock->packet_read_len, read_len);
                return RET_WAIT_FOR_EVENT;
            }
            sock->packet_read_len = 0;
            sock->header_read_len = 0;
        } while (sock->has_multi_packet);

        return RET_SUCCESS;
    }

    int StateMachine::_query_read(SmartSocket sock) {
        if (!sock) {
            TLOG_ERROR("s==nullptr");
            return RET_ERROR;
        }
        sock->reset_query_ctx(new(std::nothrow)QueryContext(sock->user_info, sock->current_db));
        if (!sock->query_ctx) {
            TLOG_ERROR("create query context instance failed");
            return RET_ERROR;
        }
        int ret = _read_packet(sock);
        if (ret == RET_WAIT_FOR_EVENT) {
            TLOG_TRACE("{}, Read packet partly.",*sock);
            return ret;
        } else if (ret != RET_SUCCESS) {
            TLOG_WARN("{}, Failed to read packet.[ret={}]", *sock, ret);
            return ret;
        }
        uint32_t off = PACKET_HEADER_LEN;
        // point to current query.
        uint8_t *packet = sock->self_buf->_data;
        int32_t packet_left = sock->self_buf->_size;

        // get query command
        ret = _wrapper->protocol_get_char(packet, sock->packet_len + PACKET_HEADER_LEN, off,
                                          &(sock->query_ctx->mysql_cmd));
        if (ret != RET_SUCCESS) {
            TLOG_ERROR("{}, protocol_get_char failed off={}, len=1", *sock, off);
            return RET_ERROR;
        }
        packet_left -= 1;

        auto command = sock->query_ctx->mysql_cmd;
        // TLOG_WARN("{}, command[{}]",*sock, command);
        // Check command valid
        if (!_wrapper->is_valid_command(command)) {
            const char *message = "denied command -_-||";
            if (!_wrapper->make_string_packet(sock, message)) {
                TLOG_ERROR("{}, Failed to fill string packet.", *sock);
                return RET_ERROR;
            }
            TLOG_ERROR("{}, invalid command[{}]", *sock, command);
            return RET_CMD_UNSUPPORT;
        }
        if (_wrapper->is_shutdown_command(command)) {
            return RET_COMMAND_SHUTDOWN;
        }

        if (COM_PING == command) {                     // COM_PING
            sock->query_ctx->type = _get_query_type(sock->query_ctx);
            return RET_SUCCESS;
        } else if (COM_STMT_EXECUTE == command) {      // this is COM_EXECUTE Packet
            if (RET_SUCCESS != _query_read_stmt_execute(sock)) {
                _wrapper->make_err_packet(sock, ER_ERROR_COMMON, "prepare statemant execute failed");
                return RET_ERROR;
            }
            return RET_SUCCESS;
        } else if (COM_STMT_SEND_LONG_DATA == command) {
            if (RET_SUCCESS != _query_read_stmt_long_data(sock)) {
                _wrapper->make_err_packet(sock, ER_ERROR_COMMON, "prepare statemant execute failed");
                return RET_ERROR;
            }
            return RET_CMD_DONE;
        } else if (COM_STMT_CLOSE == command || COM_STMT_RESET == command) {
            uint64_t stmt_id = 0;
            if (RET_SUCCESS != _wrapper->protocol_get_length_fixed_int(packet,
                                                                       sock->packet_len + PACKET_HEADER_LEN, off, 4,
                                                                       stmt_id)) {
                TLOG_ERROR("read stmt_id failed");
                return RET_ERROR;
            }
            if (COM_STMT_RESET == command) {
                auto iter = sock->prepared_plans.find(std::to_string(stmt_id));
                if (iter == sock->prepared_plans.end()) {
                    _wrapper->make_err_packet(sock, ER_UNKNOWN_STMT_HANDLER, "prepare stmt not found");
                    return RET_ERROR;
                } else {
                    _wrapper->make_simple_ok_packet(sock);
                    auto prepare_ctx = iter->second;
                    sock->query_ctx->sql = prepare_ctx->sql;
                    prepare_ctx->long_data_vars.clear();
                    return RET_CMD_DONE;
                }
            }
            // TLOG_WARN("stmt_id is: {}", stmt_id);
            sock->query_ctx->prepare_stmt_name = std::to_string(stmt_id);
            return RET_SUCCESS;
        } else {
            // command == (COM_QUERY || COM_STMT_PREPARE || COM_INIT_DB) Read query sql.
            int sql_len = sock->packet_len - 1;
            if (sql_len > 0) {
                // off == 5 now.
                ret = _wrapper->protocol_get_sql_string(packet, packet_left, off, sock->query_ctx->sql, sql_len);
                if (ret != 0) {
                    TLOG_ERROR("{}, protocol_get_sql_string ret={}", *sock, ret);
                    return ret;
                }
                TLOG_DEBUG("sql is {}, {}", command, sock->query_ctx->sql.c_str());
            } else {
                TLOG_ERROR("{}, server is read_only, so it can not "
                           "execute stmt_close statement, command:[{}]", *sock, command);
                _wrapper->make_err_packet(sock, ER_NOT_ALLOWED_COMMAND, "command not supported");
                return RET_CMD_UNSUPPORT;
            }
        }
        sock->query_ctx->type = _get_query_type(sock->query_ctx);
        auto type = sock->query_ctx->type;
        _get_json_attributes(sock->query_ctx);

        // If use charset optimize, then don't support set charset.
        if (type == SQL_SET_CHARSET_NUM || type == SQL_SET_CHARACTER_SET_NUM) {
            TLOG_ERROR("{}, unsupport charset SQL [{}]", *sock, sock->query_ctx->sql.c_str());
            _wrapper->make_err_packet(sock, ER_UNKNOWN_CHARACTER_SET, "unsupport charset");
            return RET_CMD_UNSUPPORT;
        }
        if (SQL_UNKNOWN_NUM == sock->query_ctx->type) {
            TLOG_WARN("{}, Query type is unknow. type=[{}] command=[{:#x}].",*sock,
                              sock->query_ctx->type, command);
            if (!_wrapper->make_simple_ok_packet(sock)) {
                TLOG_ERROR("{}, fill_ok_packet errro.", *sock);
                return RET_CMD_UNSUPPORT;
            }
            return RET_CMD_UNSUPPORT;
        }
        return RET_SUCCESS;
    }

    int StateMachine::_query_read_stmt_long_data(SmartSocket sock) {
        uint8_t *packet = sock->self_buf->_data;
        uint32_t off = PACKET_HEADER_LEN + 1; // packet header(4) + cmd(1)
        // std::string data((char*)packet, sock->packet_len + PACKET_HEADER_LEN);
        // TLOG_WARN("data is: {}", str_to_hex(data).c_str());

        uint64_t stmt_id = 0;
        if (RET_SUCCESS !=
            _wrapper->protocol_get_length_fixed_int(packet, sock->packet_len + PACKET_HEADER_LEN, off, 4, stmt_id)) {
            TLOG_WARN("read stmt_id failed");
            return RET_ERROR;
        }
        //TLOG_WARN("stmt_id is: {}", stmt_id);
        std::string stmt_name = std::to_string(stmt_id);
        auto iter = sock->prepared_plans.find(stmt_name);
        if (iter == sock->prepared_plans.end()) {
            TLOG_WARN("find stmt_id failed stmt_id:{}", stmt_id);
            return RET_ERROR;
        }
        sock->query_ctx->prepare_stmt_name = stmt_name;

        uint64_t param_id = 0;
        if (RET_SUCCESS !=
            _wrapper->protocol_get_length_fixed_int(packet, sock->packet_len + PACKET_HEADER_LEN, off, 2, param_id)) {
            TLOG_WARN("read param_id failed");
            return RET_ERROR;
        }
        //TLOG_WARN("param_id is: {}", param_id);
        auto prepare_ctx = iter->second;
        std::string &long_data = prepare_ctx->long_data_vars[param_id];
        long_data.append((char *) (packet + off), sock->packet_len + PACKET_HEADER_LEN - off);
        //TLOG_WARN("long data: {}, {}", param_id, long_data.c_str());
        return RET_SUCCESS;
    }

    int StateMachine::_query_read_stmt_execute(SmartSocket sock) {
        uint8_t *packet = sock->self_buf->_data;
        uint32_t off = PACKET_HEADER_LEN + 1; // packet header(4) + cmd(1)
        // std::string data((char*)packet, sock->packet_len + PACKET_HEADER_LEN);
        // TLOG_WARN("data is: {}", str_to_hex(data).c_str());

        uint64_t stmt_id = 0;
        if (RET_SUCCESS !=
            _wrapper->protocol_get_length_fixed_int(packet, sock->packet_len + PACKET_HEADER_LEN, off, 4, stmt_id)) {
            TLOG_ERROR("read stmt_id failed");
            return RET_ERROR;
        }
        //TLOG_WARN("stmt_id is: {}", stmt_id);

        std::string stmt_name = std::to_string(stmt_id);
        auto iter = sock->prepared_plans.find(stmt_name);
        if (iter == sock->prepared_plans.end()) {
            sock->query_ctx->stat_info.error_code = ER_UNKNOWN_STMT_HANDLER;
            sock->query_ctx->stat_info.error_msg << "Unknown prepared statement handler (" << stmt_name
                                                 << ") given to EXECUTE";
            TLOG_WARN("Unknown prepared statement handler ({}) given to EXECUTE", stmt_name.c_str());
            return RET_ERROR;
        }
        auto prepare_ctx = iter->second;

        uint8_t flags = 0;
        if (RET_SUCCESS != _wrapper->protocol_get_char(packet, sock->packet_len + PACKET_HEADER_LEN, off, &flags)) {
            TLOG_ERROR("read stmt flags failed");
            return RET_ERROR;
        }
        // todo: flags support
        // https://dev.mysql.com/doc/refman/5.7/en/mysql-stmt-attr-set.html
        // https://dev.mysql.com/doc/internals/en/com-stmt-execute.html
        //TLOG_WARN("stmt_flags is: {}", flags);
        if (flags != 0) {
            TLOG_ERROR("stmt_flags non-zero is not supported: {}", flags);
            return RET_ERROR;
        }
        uint64_t iteration_count = 0;
        if (RET_SUCCESS != _wrapper->protocol_get_length_fixed_int(packet, sock->packet_len + PACKET_HEADER_LEN, off, 4,
                                                                   iteration_count)) {
            TLOG_ERROR("read stmt_id failed");
            return RET_ERROR;
        }
        uint8_t new_parameter_bound_flag = 0;
        int num_params = prepare_ctx->placeholders.size();
        //TLOG_WARN("iteration_count is: {}, param_count: {}", iteration_count, num_params);

        uint8_t *null_bitmap = nullptr;
        if (num_params > 0) {
            int null_bitmap_len = (num_params + 7) / 8;
            // TLOG_WARN("null_bitmap_len: {}, offset: {}", null_bitmap_len, off);
            null_bitmap = packet + off;
            off += null_bitmap_len;
            if (RET_SUCCESS != _wrapper->protocol_get_char(packet, sock->packet_len + PACKET_HEADER_LEN, off,
                                                           &new_parameter_bound_flag)) {
                TLOG_ERROR("read stmt new_parameter_bound_flag failed");
                return RET_ERROR;
            }
        }

        // TLOG_WARN("new_parameter_bound_flag is: {}", new_parameter_bound_flag);
        if (new_parameter_bound_flag == 1) {
            prepare_ctx->param_type.clear();
            uint8_t *type_ptr = packet + off;
            for (int idx = 0; idx < num_params; ++idx) {
                SignedType param_type;
                param_type.mysql_type = static_cast<MysqlType>(*(type_ptr + 2 * idx));
                param_type.is_unsigned = *(type_ptr + 2 * idx + 1) == 0x80;
                prepare_ctx->param_type.push_back(param_type);
                // TLOG_WARN("stmt_name: {}, mysql_type: {}, is_unsigned: {}",
                //     stmt_name.c_str(), param_type.mysql_type, param_type.is_unsigned);
            }
            off += (2 * num_params);
        }

        if (num_params > 0) {
            if (prepare_ctx->param_type.size() <= 0) {
                TLOG_ERROR("empty param_types: {}", stmt_name.c_str());
                return RET_ERROR;
            }
            for (int idx = 0; idx < num_params; ++idx) {
                proto::ExprNode expr_node;
                auto iter = prepare_ctx->long_data_vars.find(idx);
                if (iter != prepare_ctx->long_data_vars.end()) {
                    expr_node.set_node_type(proto::STRING_LITERAL);
                    expr_node.set_col_type(proto::STRING);
                    expr_node.set_num_children(0);
                    proto::DeriveExprNode *str_node = expr_node.mutable_derive_node();
                    str_node->set_string_val(iter->second);
                } else {
                    bool is_null = (null_bitmap[idx / 8] >> (idx % 8)) & 0x01;
                    if (is_null || prepare_ctx->param_type[idx].mysql_type == MYSQL_TYPE_NULL) {
                        // TLOG_WARN("is_null: {}, type: {}", is_null, type_vec[idx].mysql_type);
                        expr_node.set_node_type(proto::NULL_LITERAL);
                        expr_node.set_col_type(proto::NULL_TYPE);
                    } else {
                        if (RET_SUCCESS != _wrapper->decode_binary_protocol_value(
                                packet, sock->packet_len + PACKET_HEADER_LEN, off, prepare_ctx->param_type[idx],
                                expr_node)) {
                            TLOG_WARN("decode_prepared_stmt_param_value failed num_params:{}, idx:{}", num_params,
                                      idx);
                            return RET_ERROR;
                        }
                    }
                }
                //TLOG_WARN("param_value: {}, {}", idx, expr_node.ShortDebugString().c_str());
                sock->query_ctx->param_values.push_back(expr_node);
            }
        }
        sock->query_ctx->prepare_stmt_name = stmt_name;
        return RET_SUCCESS;
    }

    bool StateMachine::_query_process(SmartSocket client) {
        TimeCost cost;
        //gettimeofday(&(client->query_ctx->stat_info.start_stamp), nullptr);

        bool ret = true;
        auto command = client->query_ctx->mysql_cmd;
        int type = client->query_ctx->type;
        if (command == COM_PING) {            // 0x0e command:MYSQL_PING
            _wrapper->make_simple_ok_packet(client);
            client->state = STATE_READ_QUERY_RESULT;
            return true;
        }
        auto sql_len = client->query_ctx->sql.size();
        if (command != COM_STMT_EXECUTE && command != COM_STMT_CLOSE && sql_len == 0) {
            TLOG_ERROR("SQL size is 0. command: {}", command);
            return false;
        }
        if (command == COM_INIT_DB) {     // 0x02 command: use database, set names, set charset...
            if (type == SQL_USE_NUM || type == SQL_USE_IN_QUERY_NUM) {
                ret = _handle_client_query_use_database(client);
            } else {
                // Other query return ok package.
                _wrapper->make_simple_ok_packet(client);
                client->state = STATE_READ_QUERY_RESULT;
            }
        } else if (command == COM_QUERY) { // 0x03 command:COM_QUERY
            if (type == SQL_SET_CHARSET_NUM
                || type == SQL_SET_CHARACTER_SET_NUM) {
                _wrapper->make_simple_ok_packet(client);
                client->state = STATE_READ_QUERY_RESULT;
            } else if (type == SQL_SET_NAMES_NUM
                       || type == SQL_SET_CHARACTER_SET_CLIENT_NUM
                       || type == SQL_SET_CHARACTER_SET_CONNECTION_NUM) {
                re2::RE2::Options option;
                option.set_encoding(RE2::Options::EncodingLatin1);
                option.set_case_sensitive(false);
                re2::RE2 reg(".*gbk.*", option);
                if (RE2::FullMatch(client->query_ctx->sql, reg)) {
                    client->charset_name = "gbk";
                    client->charset_num = 28;
                } else {
                    client->charset_name = "utf8";
                    client->charset_num = 33;
                }
                if (reg.error_code() != 0) {
                    TLOG_WARN("charset regex match error.");
                }
                _wrapper->make_simple_ok_packet(client);
                client->state = STATE_READ_QUERY_RESULT;
            } else if (type == SQL_SET_CHARACTER_SET_RESULTS_NUM) {
                // jdbc连接设置GBK，也会设置成character_set_results=null/utf8
                // 先忽略character_set_results
                _wrapper->make_simple_ok_packet(client);
                client->state = STATE_READ_QUERY_RESULT;
            } else if (turbo::EqualsIgnoreCase(client->query_ctx->sql, SQL_SELECT_DATABASE)) {
                ret = _handle_client_query_select_database(client);
            } else if (turbo::EqualsIgnoreCase(client->query_ctx->sql, SQL_SELECT_CONNECTION_ID)) {
                ret = _handle_client_query_select_connection_id(client);
            } else if (turbo::StartsWithIgnoreCase(client->query_ctx->sql, SQL_HANDLE)) {
                size_t pos = 0;
                std::string sql = client->query_ctx->sql;
                while ((pos = sql.find("  ")) != std::string::npos) {
                    sql = sql.replace(pos, 2, " ");
                }
                client->query_ctx->sql = sql;
                // handle sql like "handle xxx"
                ret = HandleHelper::get_instance()->execute(client);
            } else if (turbo::StartsWithIgnoreCase(client->query_ctx->sql, SQL_SHOW)) {
                size_t pos = 0;
                std::string sql = client->query_ctx->sql;
                while ((pos = sql.find("  ")) != std::string::npos) {
                    sql = sql.replace(pos, 2, " ");
                }
                client->query_ctx->sql = sql;
                ret = ShowHelper::get_instance()->execute(client);
            } else if (type == SQL_USE_IN_QUERY_NUM
                       && turbo::StartsWithIgnoreCase(client->query_ctx->sql, SQL_USE)) {
                ret = _handle_client_query_use_database(client);
            } else if (type == SQL_DESC_NUM) {
                ret = _handle_client_query_desc_table(client);
            } else if (type == SQL_SHOW_NUM) {
                _wrapper->make_simple_ok_packet(client);
                client->state = STATE_READ_QUERY_RESULT;
            } else {
                //对于正常的请求做限制
                if (client->user_info->is_exceed_quota()) {
                    _wrapper->make_err_packet(client, ER_QUERY_EXCEED_QUOTA, "query exceed quota(qps)");
                    TLOG_WARN("query exceed quota, user:{}, query:{}, quota:{}, time:{}",
                              client->username.c_str(),
                              client->user_info->query_count.load(),
                              client->user_info->query_quota,
                              client->user_info->query_cost.get_time());
                    client->state = STATE_READ_QUERY_RESULT;
                    return true;
                }
                // 防止超大sql文本
                if (sql_len > std::max(FLAGS_baikal_max_allowed_packet, (int64_t) 1024)) {
                    TLOG_WARN("sql too big sql_len: {}", sql_len);
                    _wrapper->make_err_packet(client, ER_NET_PACKET_TOO_LARGE,
                                              "Packets larger than max_allowed_packet are not allowed");
                    client->state = STATE_ERROR_REUSE;
                    return true;
                }
                //TLOG_DEBUG_CLIENT(client, "Choose common handle cost time:[{}(ms)]", cost.get_time());
                ret = _handle_client_query_common_query(client);
                client->state = (client->state == STATE_ERROR) ? STATE_ERROR : STATE_READ_QUERY_RESULT;
            }
        } else if (command == COM_FIELD_LIST) {   // 0x04 command:COM_FIELD_LIST
            TLOG_WARN("{}, Unsupport command[{}]", *client, client->query_ctx->sql.c_str());
            _wrapper->make_err_packet(client, ER_NOT_ALLOWED_COMMAND, "command not supported");
            client->state = STATE_ERROR_REUSE;
        } else if (command == COM_STMT_PREPARE || command == COM_STMT_EXECUTE || command == COM_STMT_CLOSE) {
            // 0x16 command: mysql_stmt_prepare
            // 0x17 command: mysql_stmt_execute
            // 0x19 command: mysql_stmt_close
            ret = _handle_client_query_common_query(client);
            client->state = STATE_READ_QUERY_RESULT;
        } else {                                 // Unsupport command.
            TLOG_ERROR("{}, unsupport command[{}]", *client, client->query_ctx->sql.c_str());
            _wrapper->make_err_packet(client, ER_NOT_ALLOWED_COMMAND, "command not supported");
            client->state = STATE_ERROR_REUSE;
        }
        return ret;
    }

    void StateMachine::_parse_comment(std::shared_ptr<QueryContext> ctx) {
        // Remove comments.
        re2::RE2::Options option;
        option.set_encoding(RE2::Options::EncodingLatin1);
        option.set_case_sensitive(false);
        option.set_perl_classes(true);
        re2::RE2 reg("^\\/\\*(.*?)\\*\\/", option);

        // Remove ignore character.
        turbo::TrimRight(ctx->sql, turbo::ByAnyOf(" \t\n\r\x0B;"));
        turbo::TrimLeft(ctx->sql, turbo::ByAnyOf(" \t\n\r\x0B"));

        while (turbo::StartsWith(ctx->sql, "/*")) {
            size_t len = ctx->sql.size();
            std::string comment;
            if (!RE2::Extract(ctx->sql, reg, "\\1", &comment)) {
                TLOG_WARN("extract commit error.");
            }
            if (comment.size() != 0) {
                ctx->comments.push_back(comment);
                ctx->sql = ctx->sql.substr(comment.size() + 4);
            }
            if (ctx->sql.size() == len) {
                break;
            }
            // Remove ignore character.
            turbo::TrimLeft(ctx->sql, turbo::ByAnyOf(" \t\n\r\x0B"));
        }
    }

    int StateMachine::_get_json_attributes(std::shared_ptr<QueryContext> ctx) {
        for (auto &json_str: ctx->comments) {
            rapidjson::Document root;
            try {
                root.Parse<0>(json_str.c_str());
                if (root.HasParseError()) {
                    //rapidjson::ParseErrorCode code = root.GetParseError();
                    //TLOG_WARN("parse extra file error [code:{}][{}]", code, json_str.c_str());
                    continue;
                }
                auto json_iter = root.FindMember("region_id");
                if (json_iter != root.MemberEnd()) {
                    ctx->debug_region_id = json_iter->value.GetInt64();
                    TLOG_WARN("debug_region_id: {}", ctx->debug_region_id);
                }
                json_iter = root.FindMember("enable_2pc");
                if (json_iter != root.MemberEnd()) {
                    ctx->enable_2pc = json_iter->value.GetInt64();
                    TLOG_WARN("enable_2pc: {}", ctx->enable_2pc);
                }
                json_iter = root.FindMember("full_export");
                if (json_iter != root.MemberEnd()) {
                    ctx->is_full_export = json_iter->value.GetBool();
                    TLOG_WARN("full_export: {}", ctx->is_full_export);
                }
                json_iter = root.FindMember("single_store_concurrency");
                if (json_iter != root.MemberEnd()) {
                    ctx->single_store_concurrency = json_iter->value.GetInt();
                    TLOG_WARN("single_store_concurrency: {}", ctx->single_store_concurrency);
                }
                json_iter = root.FindMember("ttl_duration");
                if (json_iter != root.MemberEnd()) {
                    ctx->row_ttl_duration = json_iter->value.GetInt64();
                    TLOG_DEBUG("row_ttl_duration: {}", ctx->row_ttl_duration);
                }
                json_iter = root.FindMember("X-B3-TraceId");
                if (json_iter != root.MemberEnd() && json_iter->value.IsString()) {
                    ctx->stat_info.trace_id = json_iter->value.GetString();
                }
                json_iter = root.FindMember("peer_index");
                if (json_iter != root.MemberEnd()) {
                    ctx->peer_index = json_iter->value.GetInt64();
                    TLOG_WARN("peer_index: {}", ctx->peer_index);
                }
            } catch (...) {
                TLOG_WARN("parse extra file error [{}]", json_str.c_str());
                continue;
            }
        }
        return 0;
    }

    bool StateMachine::_handle_client_query_use_database(SmartSocket client) {
        if (client == nullptr) {
            return false;
        }
        std::string sql = client->query_ctx->sql;
        // Find databases.
        SchemaFactory *factory = SchemaFactory::get_instance();
        std::vector<std::string> dbs = factory->get_db_list(client->user_info->all_database);
        int type = client->query_ctx->type;
        std::string db;
        if (type == SQL_USE_NUM) {
            turbo::TrimLeft(sql, turbo::ByAnyOf(" `"));
            db = sql;
        } else if (type == SQL_USE_IN_QUERY_NUM) {
            std::vector<std::string> split_vec = turbo::StrSplit(client->query_ctx->sql,
                                                                 turbo::ByAnyChar(" \t\n\r."),
                                                                 turbo::SkipEmpty());
            if (split_vec.size() < 2) {
                TLOG_ERROR("use db fail, {}", sql.c_str());
                return false;
            }
            db = remove_quote(split_vec[1].c_str(), '`');
        } else {
            TLOG_ERROR("use db fail, {}", sql.c_str());
            return false;
        }
        std::string db_name = db;
        std::transform(db_name.begin(), db_name.end(), db_name.begin(), ::tolower);
        if (db_name != "information_schema") {
            db_name = try_to_lower(db);
        }
        auto iter = std::find_if(dbs.begin(), dbs.end(), [db_name](std::string &db) {
            return db_name == try_to_lower(db);
        });
        if (iter == dbs.end()) {
            _wrapper->make_err_packet(client, ER_DBACCESS_DENIED_ERROR,
                                      "Access denied for user '{}' to database '{}'",
                                      client->user_info->username.c_str(), db.c_str());
            client->state = STATE_READ_QUERY_RESULT;
            return false;
        } else {
            db_name = *iter;
        }
        // Set current database.
        client->query_ctx->cur_db = db_name;
        client->current_db = db_name;
        // Set ok package.
        _wrapper->make_simple_ok_packet(client);
        client->state = STATE_READ_QUERY_RESULT;
        return true;
    }

    bool StateMachine::_handle_client_query_select_database(SmartSocket client) {
        return ShowHelper::get_instance()->_handle_client_query_template(client,
                                                                         "database()", MYSQL_TYPE_VARCHAR,
                                                                         {client->current_db});
    }

    bool StateMachine::_handle_client_query_select_connection_id(SmartSocket client) {
        return ShowHelper::get_instance()->_handle_client_query_template(client,
                                                                         "CONNECTION_ID()", MYSQL_TYPE_LONGLONG,
                                                                         {std::to_string(client->conn_id)});
    }

    bool StateMachine::_handle_client_query_desc_table(SmartSocket client) {
        if (client == nullptr) {
            TLOG_ERROR("param invalid");
            //client->state = STATE_ERROR;
            return false;
        }

        // Make fields.
        std::vector<ResultField> fields;
        do {
            ResultField field;
            field.name = "Field";
            field.type = MYSQL_TYPE_VARCHAR;
            field.length = 1024;
            fields.push_back(field);
        } while (0);
        do {
            ResultField field;
            field.name = "Type";
            field.type = MYSQL_TYPE_VARCHAR;
            field.length = 1024;
            fields.push_back(field);
        } while (0);
        do {
            ResultField field;
            field.name = "Null";
            field.type = MYSQL_TYPE_VARCHAR;
            field.length = 1024;
            fields.push_back(field);
        } while (0);
        do {
            ResultField field;
            field.name = "Key";
            field.type = MYSQL_TYPE_VARCHAR;
            field.length = 1024;
            fields.push_back(field);
        } while (0);
        do {
            ResultField field;
            field.name = "default";
            field.type = MYSQL_TYPE_VARCHAR;
            field.length = 1024;
            fields.push_back(field);
        } while (0);
        do {
            ResultField field;
            field.name = "Extra";
            field.type = MYSQL_TYPE_VARCHAR;
            field.length = 1024;
            fields.push_back(field);
        } while (0);

        std::vector<std::string> split_vec = turbo::StrSplit(client->query_ctx->sql,
                                                             turbo::ByAnyChar(" \t\n\r."), turbo::SkipEmpty());
        std::string db = client->current_db;
        std::string table;
        if (split_vec.size() == 2) {
            table = remove_quote(split_vec[1].c_str(), '`');
        } else if (split_vec.size() == 3) {
            db = remove_quote(split_vec[1].c_str(), '`');
            table = remove_quote(split_vec[2].c_str(), '`');
        } else {
            client->state = STATE_ERROR;
            return false;
        }
        SchemaFactory *factory = SchemaFactory::get_instance();
        std::string namespace_ = client->user_info->namespace_;
        if (db == "information_schema") {
            namespace_ = "INTERNAL";
        }
        std::string full_name = namespace_ + "." + db + "." + table;
        int64_t table_id = -1;
        if (factory->get_table_id(full_name, table_id) != 0) {
            client->state = STATE_ERROR;
            return false;
        }
        TableInfo info = factory->get_table_info(table_id);
        std::multimap<int32_t, IndexInfo> field_index;
        for (auto &index_id: info.indices) {
            IndexInfo index_info = factory->get_index_info(index_id);
            for (auto &field: index_info.fields) {
                field_index.insert(std::make_pair(field.id, index_info));
            }
        }
        // Make rows.
        std::vector<std::vector<std::string> > rows;
        for (auto &field: info.fields) {
            if (field.deleted) {
                continue;
            }
            std::vector<std::string> row;
            std::vector<std::string> split_vec = turbo::StrSplit(field.name,
                                                                 turbo::ByAnyChar(" \t\n\r."), turbo::SkipEmpty());
            row.push_back(split_vec[split_vec.size() - 1]);
            row.push_back(PrimitiveType_Name(field.type));
            row.push_back(field.can_null ? "YES" : "NO");

            std::vector<std::string> extra_vec;
            if (field_index.count(field.id) == 0) {
                row.push_back(" ");
            } else {

                std::vector<std::string> index_types;
                index_types.reserve(4);
                auto range = field_index.equal_range(field.id);
                for (auto index_iter = range.first; index_iter != range.second; ++index_iter) {
                    auto &index_info = index_iter->second;
                    std::string index = proto::IndexType_Name(index_info.type);
                    if (index_info.type == proto::I_FULLTEXT) {
                        index += "(" + proto::SegmentType_Name(index_info.segment_type) + ")";
                    }
                    index += "(" + proto::IndexHintStatus_Name(index_info.index_hint_status) + ")";
                    index_types.push_back(index);
                    extra_vec.push_back(proto::IndexState_Name(index_info.state));
                }
                row.push_back(turbo::FormatRange("{}", index_types, "|"));
            }
            row.push_back(field.default_value);

            if (info.auto_inc_field_id == field.id) {
                extra_vec.push_back("auto_increment");
            } else {
                //extra_vec.push_back(" ");
            }

            row.push_back(turbo::FormatRange("{}", extra_vec, "|"));

            rows.push_back(row);
        }

        // Make mysql packet.
        if (_make_common_resultset_packet(client, fields, rows) != 0) {
            TLOG_ERROR("{}, Failed to make result packet.", *client);
            _wrapper->make_err_packet(client, ER_MAKE_RESULT_PACKET, "Failed to make result packet.");
            client->state = STATE_ERROR;
            return false;
        }
        client->state = STATE_READ_QUERY_RESULT;
        return true;
    }

    int StateMachine::_make_common_resultset_packet(
            SmartSocket sock,
            std::vector<ResultField> &fields,
            std::vector<std::vector<std::string> > &rows) {
        if (!sock) {
            TLOG_ERROR("sock == nullptr.");
            return RET_ERROR;
        }
        if (fields.size() == 0) {
            TLOG_ERROR("Field size is 0.");
            return RET_ERROR;
        }

        //Result Set Header Packet
        int start_pos = sock->send_buf->_size;
        if (!sock->send_buf->byte_array_append_len((const uint8_t *) "\x01\x00\x00\x01", 4)) {
            TLOG_ERROR("byte_array_append_len failed.");
            return RET_ERROR;
        }
        if (!sock->send_buf->byte_array_append_length_coded_binary(fields.size())) {
            TLOG_ERROR("byte_array_append_len failed. len:[{}]", fields.size());
            return RET_ERROR;
        }
        int packet_body_len = sock->send_buf->_size - start_pos - 4;
        sock->send_buf->_data[start_pos] = packet_body_len & 0xFF;
        sock->send_buf->_data[start_pos + 1] = (packet_body_len >> 8) & 0xFF;
        sock->send_buf->_data[start_pos + 2] = (packet_body_len >> 16) & 0xFF;
        sock->send_buf->_data[start_pos + 3] = (++sock->packet_id) & 0xFF;
        // Make field packets
        for (uint32_t cnt = 0; cnt < fields.size(); ++cnt) {
            fields[cnt].catalog = "baikal";
            fields[cnt].db = sock->query_ctx->cur_db;
            fields[cnt].table.clear();
            fields[cnt].org_table.clear();
            fields[cnt].org_name = fields[cnt].name;
            _wrapper->make_field_packet(sock->send_buf, &fields[cnt], ++sock->packet_id);
        }

        // Make EOF packet
        _wrapper->make_eof_packet(sock->send_buf, ++sock->packet_id);

        // Make row packets
        for (uint32_t cnt = 0; cnt < rows.size(); ++cnt) {
            // Make row data packet
            if (!_wrapper->make_row_packet(sock->send_buf, rows[cnt], ++sock->packet_id)) {
                TLOG_ERROR("make_row_packet failed");
                return RET_ERROR;
            }
        }
        // Make EOF packet
        _wrapper->make_eof_packet(sock->send_buf, ++sock->packet_id);
        return 0;
    }

    int StateMachine::_query_result_send(SmartSocket sock) {
        if (!sock || sock->is_free) {
            TLOG_ERROR("s==nullptr");
            return RET_ERROR;
        }
        return _wrapper->real_write(sock);
    }

    int StateMachine::_send_result_to_client_and_reset_status(EpollInfo *epoll_info,
                                                              SmartSocket client) {
        if (epoll_info == nullptr || client == nullptr) {
            TLOG_ERROR("send_QueryStato_client param client null");
            return -1;
        }
        int ret = 0;
        switch (ret = _query_result_send(client)) {
            case RET_SUCCESS:
                if (_has_more_result(client)) {
                    client->send_buf->byte_array_clear();
                    client->state = STATE_READ_QUERY_RESULT_MORE;
                    //epoll_info->poll_events_mod(client, EPOLLOUT);
                    break;
                }
                //reset client
                _reset_network_socket_client_resource(client);

                //reuse again
                client->state = STATE_SEND_AUTH_RESULT;
                epoll_info->poll_events_mod(client, EPOLLIN);
                break;
            case RET_WAIT_FOR_EVENT:
                epoll_info->poll_events_mod(client, EPOLLOUT);
                break;
            default:
                TLOG_ERROR("{}, Failed to send result: state={}, ret={}, errno={}", *client,
                           static_cast<int>(client->state), ret, errno);
                client_free(client, epoll_info);
                break;
        }
        return ret;
    }

    bool StateMachine::_has_more_result(SmartSocket client) {
        RuntimeState &state = *client->query_ctx->get_runtime_state();
        if (client->query_ctx->is_full_export && !state.is_eos()) {
            return true;
        }
        return false;
    }

    int StateMachine::_reset_network_socket_client_resource(SmartSocket client) {
        client->send_buf->byte_array_clear();
        client->self_buf->byte_array_clear();
        client->send_buf_offset = 0;
        client->packet_len = 0;
        return 0;
    }

    void StateMachine::client_free(SmartSocket sock, EpollInfo *epoll_info) {
        if (!sock) {
            TLOG_ERROR("s==nullptr");
            return;
        }
        TLOG_WARN("{}, client_free, cmd={}", *sock,sock->query_ctx->mysql_cmd);
        if (sock->fd == -1 || sock->is_free) {
            TLOG_WARN("{}, sock is already free.", *sock);
            return;
        }
        if (sock->txn_id != 0) {
            sock->reset_query_ctx(new(std::nothrow)QueryContext(sock->user_info, sock->current_db));
            sock->query_ctx->sql = "rollback";
            TLOG_WARN("client free txn_id:{} seq_id:{} need rollback", sock->txn_id, sock->seq_id);
            _handle_client_query_common_query(sock);
        }
        if (sock->is_counted) {
            sock->user_info->connection_dec();
        }
        _print_query_time(sock);
        sock->reset_query_ctx(new QueryContext);
        if (sock->fd > 0 && sock->fd < (int) CONFIG_MPL_EPOLL_MAX_SIZE) {
            epoll_info->delete_fd_mapping(sock->fd);
        }
        epoll_info->poll_events_delete(sock);
        sock->is_free = true;
    }

    int StateMachine::_get_query_type(std::shared_ptr<QueryContext> ctx) {
        _parse_comment(ctx);

        // Get query type by command number.
        switch (ctx->mysql_cmd) {
            case '\x02':
                return SQL_USE_NUM;
            case '\x04':
                return SQL_FIELD_LIST_NUM;
            case '\x05':
                return SQL_CREATE_DB_NUM;
            case '\x06':
                return SQL_DROPD_DB_NUM;
            case '\x07':
                return SQL_REFRESH_NUM;
            case '\x09':
                return SQL_STAT_NUM;
            case '\x0a':
                return SQL_PROCESS_INFO_NUM;
            case '\x0d':
                return SQL_DEBUG_NUM;
            case '\x11':
                return SQL_CHANGEUSER_NUM;
            case '\x0e':
                return SQL_PING_NUM;
            default:
                break;
        }
        if (ctx->mysql_cmd != '\x03' && ctx->mysql_cmd != '\x16' && ctx->mysql_cmd != '\x17'
            && ctx->mysql_cmd != '\x19' && ctx->mysql_cmd != '\x1c') {
            return SQL_UNKNOWN_NUM;
        }
        // Unknow number.
        if (ctx->sql.size() <= 0) {
            TLOG_WARN("query->sql is nullptr, command={}", ctx->mysql_cmd);
            return SQL_UNKNOWN_NUM;
        }
        // Get sql type.
        if (turbo::StartsWithIgnoreCase(ctx->sql, SQL_SELECT)) {
            return SQL_SELECT_NUM;
        }
        if (turbo::StartsWithIgnoreCase(ctx->sql, SQL_SHOW)) {
            return SQL_SHOW_NUM;
        }
        if (turbo::StartsWithIgnoreCase(ctx->sql, SQL_EXPLAIN)) {
            return SQL_EXPLAIN_NUM;
        }
        if (turbo::StartsWithIgnoreCase(ctx->sql, SQL_KILL)) {
            return SQL_KILL_NUM;
        }
        if (turbo::StartsWithIgnoreCase(ctx->sql, SQL_USE)) {
            return SQL_USE_IN_QUERY_NUM;
        }
        if (turbo::StartsWithIgnoreCase(ctx->sql, SQL_DESC)) {
            return SQL_DESC_NUM;
        }
        if (turbo::StartsWithIgnoreCase(ctx->sql, SQL_CALL)) {
            return SQL_CALL_NUM;
        }
        if (turbo::StartsWithIgnoreCase(ctx->sql, SQL_SET)) {
            auto value_str = turbo::TrimLeft(
                    ctx->sql, turbo::ByAnyOf(" SETset"));
            if (turbo::StartsWithIgnoreCase(value_str, "names")) {
                return SQL_SET_NAMES_NUM;
            }
            if (turbo::StartsWithIgnoreCase(value_str, "charset")) {
                return SQL_SET_CHARSET_NUM;
            }
            // do not support "set [global | session | local | @@] ..."
            if (turbo::StartsWithIgnoreCase(value_str, "character_set_client")) {
                return SQL_SET_CHARACTER_SET_CLIENT_NUM;
            }
            // get character_set_connection query
            if (turbo::StartsWithIgnoreCase(value_str, "character_set_connection")) {
                return SQL_SET_CHARACTER_SET_CONNECTION_NUM;
            }
            // get character_set_results query
            if (turbo::StartsWithIgnoreCase(value_str, "character_set_results")) {
                return SQL_SET_CHARACTER_SET_RESULTS_NUM;
            }
            // get set character set.
            if (turbo::StartsWithIgnoreCase(value_str, "character set")) {
                return SQL_SET_CHARACTER_SET_NUM;
            }
            // get autocommit.
            // if (turbo::StartsWithIgnoreCase(value_str, "autocommit")) {
            //     std::string tmp = turbo::TrimLeft(
            //                                 ctx->sql, turbo::ByAnyChar(" autocommit="));
            //     return tmp == "0" ? SQL_AUTOCOMMIT_0_NUM : SQL_AUTOCOMMIT_1_NUM;
            // }
            return SQL_SET_NUM;
        }
        return SQL_WRITE_NUM;
    }

    bool StateMachine::_handle_client_query_common_query(SmartSocket client) {
        if (client == nullptr) {
            TLOG_ERROR("param invalid: socket==nullptr");
            //client->state = STATE_ERROR;
            return false;
        }
        client->query_ctx->client_conn = client.get();
        client->query_ctx->stat_info.sql_length = client->query_ctx->sql.size();
        client->query_ctx->charset = client->charset_name;

        if (SchemaFactory::get_instance()->is_big_sql(client->query_ctx->sql)) {
            _wrapper->make_err_packet(client,
                                      ER_SQL_TOO_BIG, "{}",
                                      "sql too big");
            return false;
        }

        // sql planner.
        TimeCost cost;
        TimeCost cost1;

        int ret = 0;
        ret = LogicalPlanner::analyze(client->query_ctx.get());
        if (ret < 0) {
            TLOG_WARN("{}, Failed to LogicalPlanner::analyze: {}",*client,
                              client->query_ctx->sql.c_str());
            if (client->query_ctx->stat_info.error_code == ER_ERROR_FIRST) {
                client->query_ctx->stat_info.error_code = ER_GEN_PLAN_FAILED;
                client->query_ctx->stat_info.error_msg << "get logical plan failed";
            }
            _wrapper->make_err_packet(client,
                                      client->query_ctx->stat_info.error_code, "{}",
                                      client->query_ctx->stat_info.error_msg.str().c_str());
            return false;
        }
        // DDL query need to interact with metaserver.
        if (client->query_ctx->succ_after_logical_plan) {
            if (client->query_ctx->mysql_cmd == COM_STMT_PREPARE) {
                _wrapper->make_stmt_prepare_ok_packet(client);
            } else if (client->query_ctx->mysql_cmd != COM_STMT_CLOSE) {
                _wrapper->make_simple_ok_packet(client);
            }
            client->query_ctx->stat_info.old_txn_id = client->txn_id;
            client->query_ctx->stat_info.old_seq_id = client->seq_id;
            return true;
        }
        // const std::vector<proto::TupleDescriptor>& tuples = ctx->tuple_descs();
        // for (uint32_t idx = 0; idx < tuples.size(); ++idx) {
        //     TLOG_WARN("TupleDescriptor: {}", pb2json(tuples[idx]).c_str());
        // }
        if (client->query_ctx->exec_prepared == false) {
            ret = client->query_ctx->create_plan_tree();
            if (ret < 0) {
                TLOG_ERROR("{}, Failed to pb_plan to execnode: {}", *client,
                           client->query_ctx->sql.c_str());
                return false;
            }
        }
        cost1.reset();

        // set txn_id and txn seq_id
        if (client->query_ctx->root != nullptr) {
            // TODO runtime_state裸用的地方太多容易出错
            client->query_ctx->get_runtime_state()->txn_id = client->txn_id;
            //为了不改动老逻辑。对于新逻辑 runtime_state的seq_id不起任何作用
            client->query_ctx->get_runtime_state()->seq_id = client->seq_id + 1;
        }
        //TLOG_WARN("client: {} ,seq_id: {}", client.get(), client->seq_id);
        ON_SCOPE_EXIT([client]() {
            if (client->txn_id == 0) {
                client->on_commit_rollback();
            } else {
                // for print log
                client->update_old_txn_info();
            }
        });

        //TLOG_WARN("create_plan_tree success, {}", client->query_ctx->sql.c_str());
        ret = PhysicalPlanner::analyze(client->query_ctx.get());
        if (ret < 0) {
            TLOG_ERROR("{}, Failed to PhysicalPlanner::analyze: {}", *client,
                       client->query_ctx->sql.c_str());
            // single SQL transaction need to reset connection transaction status
            if (client->query_ctx->get_runtime_state()->single_sql_autocommit()) {
                client->on_commit_rollback();
            }
            if (client->query_ctx->stat_info.error_code == ER_ERROR_FIRST) {
                client->query_ctx->stat_info.error_code = ER_GEN_PLAN_FAILED;
                client->query_ctx->stat_info.error_msg << "get physical plan failed";
            }
            _wrapper->make_err_packet(client,
                                      client->query_ctx->stat_info.error_code, "{}",
                                      client->query_ctx->stat_info.error_msg.str().c_str());
            return false;
        }
        client->query_ctx->stat_info.query_plan_time = cost.get_time();
        if (client->query_ctx->explain_type == SHOW_PLAN) {
            client->on_commit_rollback();
            proto::Plan plan;
            ExecNode::create_pb_plan(0, &plan, client->query_ctx->root);
            std::string plan_str = "logical_plan:" + client->query_ctx->plan.DebugString() + "\n" +
                                   "physical_plan:" + plan.DebugString();
            std::vector<ResultField> fields;
            ResultField field;
            field.name = "Plan";
            field.type = MYSQL_TYPE_VARCHAR;
            field.length = 1024 * 1024;
            fields.push_back(field);

            std::vector<std::vector<std::string> > rows;
            std::vector<std::string> row;
            row.push_back(plan_str);
            rows.push_back(row);
            if (_make_common_resultset_packet(client, fields, rows) != 0) {
                TLOG_ERROR("{}, Failed to make result packet.", *client);
                _wrapper->make_err_packet(client, ER_MAKE_RESULT_PACKET, "Failed to make result packet.");
                return false;
            }
            if (client->query_ctx->is_full_export) {
                // full_export packet_node必须open
                ret = PhysicalPlanner::full_export_start(client->query_ctx.get(), client->send_buf);
                client->query_ctx->stat_info.query_exec_time += cost.get_time();
                client->query_ctx->stat_info.send_buf_size += client->send_buf->_size;
            }
            return true;
        } else if (client->query_ctx->explain_type == SHOW_SIGN) {
            //用户获取sql对应的签名，获取方式为explain format = 'sign'+ sql_format 返回给用户sql签名
            std::vector<ResultField> fields;
            ResultField field;
            field.name = "sign";
            field.type = MYSQL_TYPE_STRING;
            field.length = 1024 * 1024;
            fields.reserve(1);
            fields.emplace_back(field);
            uint64_t sign = client->query_ctx->stat_info.sign;
            std::vector<std::vector<std::string>> rows;
            rows.reserve(3);
            std::vector<std::string> row = {std::to_string(sign)};
            rows.emplace_back(row);

            auto subquery_signs_set = client->get_subquery_signs();
            for (auto &subquery_sign: subquery_signs_set) {
                std::vector<std::string> row = {std::to_string(subquery_sign)};
                rows.emplace_back(row);
            }

            if (_make_common_resultset_packet(client, fields, rows) != 0) {
                TLOG_ERROR("{}, Failed to make sql sign result packet.", *client);
                _wrapper->make_err_packet(client, ER_MAKE_RESULT_PACKET, "Failed to make result packet.");
                return false;
            }
            return true;
        }

        if (client->query_ctx->succ_after_physical_plan) {
            _wrapper->make_simple_ok_packet(client);
            return true;
        }

        //TLOG_WARN("client: {} ,seq_id: {}", client.get(), client->seq_id);
        // 不会有fether那一层，重构
        if (!client->query_ctx->is_full_export) {
            ret = PhysicalPlanner::execute(client->query_ctx.get(), client->send_buf);
            //TLOG_WARN("client: {} ,seq_id: {}", client.get(), client->seq_id);
            // 空值优化时可能执行不到TransactionNode
            // 单语句事务需要回退状态
            if (client->query_ctx->get_runtime_state()->single_sql_autocommit()) {
                client->on_commit_rollback();
            }
            client->query_ctx->stat_info.query_exec_time = cost.get_time();
            client->query_ctx->stat_info.send_buf_size = client->send_buf->_size;
        } else {
            ret = PhysicalPlanner::full_export_start(client->query_ctx.get(), client->send_buf);
            client->query_ctx->stat_info.query_exec_time += cost.get_time();
            client->query_ctx->stat_info.send_buf_size += client->send_buf->_size;
        }
        if (ret < 0) {
            if (client->query_ctx->stat_info.error_code == ER_SQL_TOO_BIG) {
                SchemaFactory::get_instance()->update_big_sql(client->query_ctx->sql);
            }
            TLOG_WARN("{}, Failed to PhysicalPlanner::execute: {}",*client,
                              client->query_ctx->sql.c_str());
            if (client->query_ctx->stat_info.error_code == ER_ERROR_FIRST) {
                client->query_ctx->stat_info.error_code = ER_EXEC_PLAN_FAILED;
                client->query_ctx->stat_info.error_msg << "exec physical plan failed";
            }
            _wrapper->make_err_packet(client,
                                      client->query_ctx->stat_info.error_code, "{}",
                                      client->query_ctx->stat_info.error_msg.str().c_str());
            return false;
        }
        return true;
    }
} // namespace EA
