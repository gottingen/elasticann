// Copyright 2023 The Turbo Authors.
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

#ifndef ELASTICANN_CONFIG_GFLAGS_DEFINES_H_
#define ELASTICANN_CONFIG_GFLAGS_DEFINES_H_

#include "gflags/gflags_declare.h"

// for log
DECLARE_bool(ea_console_log);
DECLARE_string(ea_log_root);
DECLARE_int32(ea_rotation_hour);
DECLARE_int32(ea_rotation_minute);
DECLARE_string(ea_log_base_name);
DECLARE_int32(ea_log_save_days);

DECLARE_string(ea_plugin_root);

DECLARE_int32(ea_file_server_election_timeout_ms);
DECLARE_int32(ea_file_server_snapshot_interval_s);


#endif  // ELASTICANN_CONFIG_GFLAGS_DEFINES_H_
