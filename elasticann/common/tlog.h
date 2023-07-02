//
// Created by jeff on 23-7-2.
//

#ifndef ELASTICANN_COMMON_TLOG_H_
#define ELASTICANN_COMMON_TLOG_H_

#include "elasticann/config/gflags_defines.h"
#include "turbo/log/sinks/rotating_file_sink.h"
#include "turbo/log/sinks/daily_file_sink.h"
#include "turbo/log/sinks/daily_file_sink.h"
#include "turbo/files/filesystem.h"
#include "turbo/log/logging.h"

namespace EA {
    inline bool init_tlog() {
        if(!FLAGS_ea_console_log) {
            if (!turbo::filesystem::exists(FLAGS_ea_log_root)) {
                if (!turbo::filesystem::create_directories(FLAGS_ea_log_root)) {
                    return false;
                }
            }
            turbo::filesystem::path lpath(FLAGS_ea_log_root);
            lpath /= FLAGS_ea_log_base_name;
            auto file_sink = std::make_shared<turbo::tlog::sinks::daily_file_sink_mt>(lpath.string(),
                                                                                      FLAGS_ea_rotation_hour,
                                                                                      FLAGS_ea_rotation_minute,
                                                                                      false, FLAGS_ea_log_save_days);
            file_sink->set_level(turbo::tlog::level::trace);

            auto logger = std::make_shared<turbo::tlog::logger>("ea-logger", file_sink);
            logger->set_level(turbo::tlog::level::debug);
            turbo::tlog::set_default_logger(logger);
        }
    }
}
#endif  // ELASTICANN_COMMON_TLOG_H_
