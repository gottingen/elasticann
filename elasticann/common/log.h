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

#include <stdarg.h>
#include <gflags/gflags.h>
#include "turbo/log/logging.h"

#undef CHECK_LOG_FORMAT
#ifdef CHECK_LOG_FORMAT
void DB_WARNING(const char *fmt, ...) __attribute__((format(printf,1,2)));
inline void DB_WARNING(const char *fmt, ...) {}
void DB_FATAL(const char *fmt, ...) __attribute__((format(printf,1,2)));
inline void DB_FATAL(const char *fmt, ...) {}
void DB_DEBUG(const char *fmt, ...) __attribute__((format(printf,1,2)));
inline void DB_DEBUG(const char *fmt, ...) {}
void DB_TRACE(const char *fmt, ...) __attribute__((format(printf,1,2)));
inline void DB_TRACE(const char *fmt, ...) {}
void DB_NOTICE(const char *fmt, ...) __attribute__((format(printf,1,2)));
inline void DB_NOTICE(const char *fmt, ...) {}
#define DB_NOTICE_LONG DB_NOTICE

void SQL_TRACE(const char *fmt, ...) __attribute__((format(printf,1,2)));
inline void SQL_TRACE(const char *fmt, ...) {}

template <typename T>
void DB_WARNING_CLIENT(T sock, const char *fmt, ...) __attribute__((format(printf,2,3)));
template <typename T>
void DB_WARNING_CLIENT(T sock, const char *fmt, ...) {}
template <typename T>
void DB_FATAL_CLIENT(T sock, const char *fmt, ...) __attribute__((format(printf,2,3)));
template <typename T>
void DB_FATAL_CLIENT(T sock, const char *fmt, ...) {}
template <typename T>
void DB_DEBUG_CLIENT(T sock, const char *fmt, ...) __attribute__((format(printf,2,3)));
template <typename T>
void DB_DEBUG_CLIENT(T sock, const char *fmt, ...) {}
template <typename T>
void DB_TRACE_CLIENT(T sock, const char *fmt, ...) __attribute__((format(printf,2,3)));
template <typename T> 
void DB_TRACE_CLIENT(T sock, const char *fmt, ...) {}
template <typename T>
void DB_NOTICE_CLIENT(const char *fmt, ...) __attribute__((format(printf,1,2)));
template <typename T>
void DB_NOTICE_CLIENT(const char *fmt, ...) {}

#endif //CHECK_LOG_FORMAT

namespace EA {
    DECLARE_bool(enable_debug);
    DECLARE_bool(enable_self_trace);
    DECLARE_bool(servitysinglelog);

    const int MAX_LOG_LEN = 2048;

    inline void tlog_info_writelog(const char *fmt, ...) {
        char buf[MAX_LOG_LEN];
        va_list args;
        va_start(args, fmt);
        vsnprintf(buf, sizeof(buf), fmt, args);
        va_end(args);
        TLOG_INFO(buf);
    }

    const int MAX_LOG_LEN_LONG = 20480;

    inline void tlog_info_writelog_long(const char *fmt, ...) {
        char buf[MAX_LOG_LEN_LONG];
        va_list args;
        va_start(args, fmt);
        vsnprintf(buf, sizeof(buf), fmt, args);
        va_end(args);
        TLOG_INFO(buf);
    }

#define DB_NOTICE_LONG(_fmt_, args...) \
    do {\
        ::EA::tlog_info_writelog_long("[%s:%d][%s][%llu]" _fmt_, \
                strrchr(__FILE__, '/') + 1, __LINE__, __FUNCTION__, bthread_self(), ##args);\
    } while (0);

    inline void tlog_warning_writelog(const char *fmt, ...) {
        char buf[MAX_LOG_LEN];
        va_list args;
        va_start(args, fmt);
        vsnprintf(buf, sizeof(buf), fmt, args);
        va_end(args);
        TLOG_WARN(buf);
    }

    inline void tlog_error_writelog(const char *fmt, ...) {
        char buf[MAX_LOG_LEN];
        va_list args;
        va_start(args, fmt);
        vsnprintf(buf, sizeof(buf), fmt, args);
        va_end(args);
        TLOG_ERROR(buf);
    }

#ifndef CHECK_LOG_FORMAT

#ifndef NDEBUG
#define DB_DEBUG(_fmt_, args...) \
    do {\
        if (!FLAGS_enable_debug) break; \
        ::EA::tlog_info_writelog("[%s:%d][%s][%lu]" _fmt_, \
                strrchr(__FILE__, '/') + 1, __LINE__, __FUNCTION__, bthread_self(), ##args);\
    } while (0);
#else
#define DB_DEBUG(_fmt_, args...) 
#endif

#define DB_TRACE(_fmt_, args...) \
    do {\
        if (!FLAGS_enable_self_trace) break; \
        ::EA::tlog_info_writelog("[%s:%d][%s][%lu]" _fmt_, \
                strrchr(__FILE__, '/') + 1, __LINE__, __FUNCTION__, bthread_self(), ##args);\
    } while (0);

#define DB_NOTICE(_fmt_, args...) \
    do {\
        ::EA::tlog_info_writelog("[%s:%d][%s][%lu]" _fmt_, \
                strrchr(__FILE__, '/') + 1, __LINE__, __FUNCTION__, bthread_self(), ##args);\
    } while (0);

#define DB_WARNING(_fmt_, args...) \
    do {\
        ::EA::tlog_warning_writelog("[%s:%d][%s][%lu]" _fmt_, \
                strrchr(__FILE__, '/') + 1, __LINE__, __FUNCTION__, bthread_self(), ##args);\
    } while (0);

#define DB_FATAL(_fmt_, args...) \
    do {\
        ::EA::tlog_error_writelog("[%s:%d][%s][%lu]" _fmt_, \
                strrchr(__FILE__, '/') + 1, __LINE__, __FUNCTION__, bthread_self(), ##args);\
    } while (0);

#define SQL_TRACE(_fmt_, args...) \
    do {\
        if (!FLAGS_enable_self_trace) break; \
        ::EA::tlog_info_writelog_long("[%s:%d][%s][%lu]" _fmt_, \
                strrchr(__FILE__, '/') + 1, __LINE__, __FUNCTION__, bthread_self(), ##args);\
    } while (0);

#endif //CHECK_LOG_FORMAT

#ifndef CHECK_LOG_FORMAT
#define DB_DEBUG_CLIENT(sock, _fmt_, args...) \
    do {\
        DB_DEBUG("user=%s fd=%d ip=%s port=%d errno=%d:" _fmt_, \
            sock->username.c_str(), sock->fd, sock->ip.c_str(), sock->port, \
            sock->query_ctx->stat_info.error_code, ##args);\
    } while (0);

#define DB_TRACE_CLIENT(sock, _fmt_, args...) \
    do {\
        DB_TRACE("user=%s fd=%d ip=%s port=%d errno=%d:" _fmt_, \
            sock->username.c_str(), sock->fd, sock->ip.c_str(), sock->port, \
            sock->query_ctx->stat_info.error_code, ##args);\
    } while (0);

#define DB_NOTICE_CLIENT(sock, _fmt_, args...) \
    do {\
        DB_NOTICE("user=%s fd=%d ip=%s port=%d errno=%d:" _fmt_, \
            sock->username.c_str(), sock->fd, sock->ip.c_str(), sock->port, \
            sock->query_ctx->stat_info.error_code, ##args);\
    } while (0);

#define DB_WARNING_CLIENT(sock, _fmt_, args...) \
    do {\
        DB_WARNING("user=%s fd=%d ip=%s port=%d errno=%d log_id=%lu:" _fmt_, \
            sock->username.c_str(), sock->fd, sock->ip.c_str(), sock->port, \
            sock->query_ctx->stat_info.error_code, \
            sock->query_ctx->stat_info.log_id, ##args);\
    } while (0);

#define DB_FATAL_CLIENT(sock, _fmt_, args...) \
    do {\
        DB_FATAL("user=%s fd=%d ip=%s port=%d errno=%d:" _fmt_, \
            sock->username.c_str(), sock->fd, sock->ip.c_str(), sock->port, \
            sock->query_ctx->stat_info.error_code, ##args);\
    } while (0);

#endif //CHECK_LOG_FORMAT

    inline int init_log(const char *bin_name) {
        /*
        ::google::InitGoogleLogging(bin_name);
        FLAGS_max_log_size = MAX_LOG_LEN;
        FLAGS_stop_logging_if_full_disk = true;
        FLAGS_logbufsecs = 0;
        FLAGS_logtostderr = false;
        FLAGS_alsologtostderr = false;
        FLAGS_log_dir = "";
        ::google::SetLogDestination(google::GLOG_INFO, "log/task_info_log.");
        ::google::SetLogDestination(google::GLOG_WARNING, "log/task_warning_log.");
        ::google::SetLogDestination(google::GLOG_ERROR, "log/task_error_log.");

        if (FLAGS_servitysinglelog) {
            auto old_logger1 = google::base::GetLogger(google::GLOG_INFO);
            auto my_logger1 = new SingleLogFileObject(old_logger1, google::GLOG_INFO);
            google::base::SetLogger(google::GLOG_INFO, my_logger1);

            auto old_logger2 = google::base::GetLogger(google::GLOG_WARNING);
            auto my_logger2 = new SingleLogFileObject(old_logger2, google::GLOG_WARNING);
            google::base::SetLogger(google::GLOG_WARNING, my_logger2);

            auto old_logger3 = google::base::GetLogger(google::GLOG_ERROR);
            auto my_logger3 = new SingleLogFileObject(old_logger3, google::GLOG_ERROR);
            google::base::SetLogger(google::GLOG_ERROR, my_logger3);
        }
         */
        return 0;
    }

} //namespace EA

