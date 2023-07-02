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
#include "elasticann/config/gflags_defines.h"
#include "gflags/gflags.h"

DEFINE_bool(ea_console_log, true, "console or file log");
DEFINE_string(ea_log_root, "./logs", "ea flags log root");
DEFINE_int32(ea_rotation_hour, 2, "rotation hour");
DEFINE_int32(ea_rotation_minute, 30, "rotation minutes");
DEFINE_string(ea_log_base_name, "ea_log.txt", "base name for EA");
DEFINE_int32(ea_log_save_days, 7, "ea log save days");