// Copyright 2023 The Elastic-AI Authors.
// part of Elastic AI Search
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

#ifndef ELASTICANN_CLIENT_META_H_
#define ELASTICANN_CLIENT_META_H_

#include "turbo/base/status.h"
#include <butil/endpoint.h>
#include <brpc/channel.h>
#include <brpc/server.h>
#include <brpc/controller.h>
#include <string>
#include "elasticann/base/tlog.h"
#include "elasticann/flags/meta.h"
#include <google/protobuf/descriptor.h>
#include "eaproto/meta/meta.interface.pb.h"
#include "turbo/module/module_version.h"
#include "elasticann/client/base_message_sender.h"


namespace EA::client {

    class MetaClient {
    public:
        static MetaClient *get_instance() {
            static MetaClient ins;
            return &ins;
        }

        MetaClient() = default;

    public:
        turbo::Status init(BaseMessageSender *sender);

        ///
        /// \param path
        /// \param message
        /// \return
        static turbo::Status load_proto_from_file(const std::string &path, google::protobuf::Message &message);

        ///
        /// \param path
        /// \param message
        /// \return
        static turbo::Status dump_proto_to_file(const std::string &path, const google::protobuf::Message &message);

        ///
        /// \param content json format content
        /// \param message
        /// \return
        static turbo::Status load_proto(const std::string &content, google::protobuf::Message &message);

        ///
        /// \param messages
        /// \param content json format content
        /// \return
        static turbo::Status dump_proto(const google::protobuf::Message &message, std::string &content);

        ///
        /// \param json_content
        /// \return
        static turbo::Status check_config(const std::string &json_content);

        ///
        /// \param config_path
        /// \return
        static turbo::Status check_config_file(const std::string &config_path);

        ///
        /// \param config_path
        /// \param config
        /// \return
        static turbo::Status dump_config_file(const std::string &config_path, const EA::proto::ConfigInfo &config);

        ///
        /// \param config_name
        /// \param content
        /// \param config_type
        /// \param version
        /// \param retry_time
        /// \return
        turbo::Status create_config(const std::string &config_name,
                                    const std::string &content,
                                    const std::string &version,
                                    const std::string &config_type = "json",
                                    int *retry_time = nullptr);

        ///
        /// \param request
        /// \param response
        /// \param retry_time
        /// \return
        turbo::Status create_config(const EA::proto::ConfigInfo &request, int *retry_times = nullptr);

        ///
        /// \param config_name
        /// \param path
        /// \param config_type
        /// \param version
        /// \param retry_time
        /// \return
        turbo::Status create_config_by_file(const std::string &config_name,
                                            const std::string &path,
                                            const std::string &config_type = "json", const std::string &version = "",
                                            int *retry_time = nullptr);

        ///
        /// \param json_path
        /// \param retry_time
        /// \return
        turbo::Status create_config_by_json(const std::string &json_path, int *retry_time = nullptr);

        ///
        /// \param configs
        /// \param retry_time
        /// \return
        turbo::Status list_config(std::vector<std::string> &configs, int *retry_time = nullptr);

        ///
        /// \param config_name
        /// \param versions
        /// \param retry_time
        /// \return
        turbo::Status list_config_version(const std::string &config_name, std::vector<std::string> &versions,
                                          int *retry_time = nullptr);

        ///
        /// \param config_name
        /// \param versions
        /// \param retry_time
        /// \return
        turbo::Status list_config_version(const std::string &config_name, std::vector<turbo::ModuleVersion> &versions,
                                          int *retry_time = nullptr);

        ///
        /// \param config_name
        /// \param version
        /// \param config
        /// \return
        turbo::Status
        get_config(const std::string &config_name, const std::string &version, EA::proto::ConfigInfo &config,
                   int *retry_time = nullptr);

        ///
        /// \param config_name
        /// \param version
        /// \param config
        /// \param retry_time
        /// \param type
        /// \param time
        /// \return
        turbo::Status get_config(const std::string &config_name, const std::string &version, std::string &config,
                                 int *retry_time = nullptr,
                                 std::string *type = nullptr, uint32_t *time = nullptr);

        ///
        /// \param config_name
        /// \param version
        /// \param path
        /// \param retry_time
        /// \return
        turbo::Status save_config(const std::string &config_name, const std::string &version, std::string &path,
                                  int *retry_time = nullptr);

        ///
        /// \brief auto save to current directory with filename config.type
        /// \param config_name
        /// \param version
        /// \param retry_time
        /// \return
        turbo::Status
        save_config(const std::string &config_name, const std::string &version, int *retry_time = nullptr);

        ///
        /// \param config_name
        /// \param config
        /// \param retry_time
        /// \return
        turbo::Status
        get_config_latest(const std::string &config_name, EA::proto::ConfigInfo &config,
                          int *retry_time = nullptr);

        ///
        /// \param config_name
        /// \param config
        /// \param version
        /// \param retry_time
        /// \return
        turbo::Status
        get_config_latest(const std::string &config_name, std::string &config, std::string &version,
                          int *retry_time = nullptr);

        ///
        /// \param config_name
        /// \param config
        /// \param version
        /// \param type
        /// \param retry_time
        /// \return
        turbo::Status
        get_config_latest(const std::string &config_name, std::string &config, std::string &version, std::string &type,
                          int *retry_time = nullptr);


        ///
        /// \param config_name
        /// \param config
        /// \param version
        /// \param retry_time
        /// \return
        turbo::Status
        get_config_latest(const std::string &config_name, std::string &config, turbo::ModuleVersion &version,
                          int *retry_time = nullptr);

        ///
        /// \param config_name
        /// \param config
        /// \param version
        /// \param retry_time
        /// \return
        turbo::Status
        get_config_latest(const std::string &config_name, std::string &config, turbo::ModuleVersion &version,
                          std::string &type,
                          int *retry_time = nullptr);

        ///
        /// \param config_name
        /// \param config
        /// \param retry_time
        /// \return
        turbo::Status
        get_config_latest(const std::string &config_name, std::string &config, int *retry_time = nullptr);

        ///
        /// \param config_name
        /// \param version
        /// \param retry_time
        /// \return
        turbo::Status
        remove_config(const std::string &config_name, const std::string &version, int *retry_time = nullptr);

        ///
        /// \param config_name
        /// \param version
        /// \param retry_time
        /// \return
        turbo::Status
        remove_config(const std::string &config_name, const turbo::ModuleVersion &version, int *retry_time = nullptr);

        ///
        /// \param config_name
        /// \param version
        /// \param retry_time
        /// \return
        turbo::Status
        remove_config_all_version(const std::string &config_name, int *retry_time = nullptr);

        ///
        /// \param info
        /// \param retry_time
        /// \return
        turbo::Status create_namespace(EA::proto::NameSpaceInfo &info, int *retry_time = nullptr);

        ///
        /// \param ns
        /// \param quota
        /// \param retry_time
        /// \return
        turbo::Status create_namespace(const std::string &ns, int64_t quota = 0, int *retry_time = nullptr);

        ///
        /// \param json_str
        /// \param retry_time
        /// \return
        turbo::Status create_namespace_by_json(const std::string &json_str, int *retry_time = nullptr);

        ///
        /// \param path
        /// \param retry_time
        /// \return
        turbo::Status create_namespace_by_file(const std::string &path, int *retry_time = nullptr);

        ///
        /// \param ns
        /// \param retry_time
        /// \return
        turbo::Status remove_namespace(const std::string &ns, int *retry_time = nullptr);

        ///
        /// \param ns
        /// \param retry_time
        /// \return
        turbo::Status modify_namespace(EA::proto::NameSpaceInfo &ns_info, int *retry_time = nullptr);

        ///
        /// \param json_str
        /// \param retry_time
        /// \return
        turbo::Status modify_namespace_by_json(const std::string &json_str, int *retry_time = nullptr);

        ///
        /// \param path
        /// \param retry_time
        /// \return
        turbo::Status modify_namespace_by_file(const std::string &path, int *retry_time = nullptr);

        ///
        /// \param ns_name
        /// \param retry_time
        /// \return
        turbo::Status list_namespace(std::vector<std::string> &ns_list, int *retry_time = nullptr);

        ///
        /// \param ns_list
        /// \param retry_time
        /// \return
        turbo::Status list_namespace(std::vector<EA::proto::NameSpaceInfo> &ns_list, int *retry_time = nullptr);

        ///
        /// \param ns_list
        /// \param retry_time
        /// \return
        turbo::Status list_namespace_to_json(std::vector<std::string> &ns_list, int *retry_time = nullptr);

        ///
        /// \param save_path
        /// \param retry_time
        /// \return
        turbo::Status list_namespace_to_file(const std::string &save_path, int *retry_time = nullptr);

        ///
        /// \param ns_name
        /// \param ns_list
        /// \param retry_time
        /// \return
        turbo::Status
        get_namespace(const std::string &ns_name, EA::proto::NameSpaceInfo &ns_pb, int *retry_time = nullptr);

        ///
        /// \param ns_name
        /// \param json_str
        /// \param retry_time
        /// \return
        turbo::Status get_namespace_json(const std::string &ns_name, std::string &json_str, int *retry_time = nullptr);

        ///
        /// \param ns_name
        /// \param json_path
        /// \param retry_time
        /// \return
        turbo::Status
        save_namespace_json(const std::string &ns_name, const std::string &json_path, int *retry_time = nullptr);

        ///
        /// \param info
        /// \param retry_time
        /// \return
        turbo::Status create_database(EA::proto::DataBaseInfo &info, int *retry_time = nullptr);

        ///
        /// \param ns
        /// \param database
        /// \param quota
        /// \param retry_time
        /// \return
        turbo::Status create_database(const std::string &ns, const std::string &database, int64_t quota = 0,
                                      int *retry_time = nullptr);

        ///
        /// \param json_str
        /// \param retry_time
        /// \return
        turbo::Status create_database_by_json(const std::string &json_str, int *retry_time = nullptr);

        ///
        /// \param path
        /// \param retry_time
        /// \return
        turbo::Status create_database_by_file(const std::string &path, int *retry_time = nullptr);

        ///
        /// \param ns
        /// \param database
        /// \param retry_time
        /// \return
        turbo::Status remove_database(const std::string &ns, const std::string &database, int *retry_time = nullptr);

        ///
        /// \param db_info
        /// \param retry_time
        /// \return
        turbo::Status modify_database(EA::proto::DataBaseInfo &db_info, int *retry_time = nullptr);

        ///
        /// \param json_str
        /// \param retry_time
        /// \return
        turbo::Status modify_database_by_json(const std::string &json_str, int *retry_time = nullptr);

        ///
        /// \param path
        /// \param retry_time
        /// \return
        turbo::Status modify_database_by_file(const std::string &path, int *retry_time = nullptr);

        ///
        /// \param db_list
        /// \param retry_time
        /// \return
        turbo::Status list_database(std::vector<EA::proto::DataBaseInfo> &db_list, int *retry_time = nullptr);

        ///
        /// \param ns
        /// \param db_list
        /// \param retry_time
        /// \return
        turbo::Status
        list_database(const std::string &ns, std::vector<EA::proto::DataBaseInfo> &db_list, int *retry_time = nullptr);

        ///
        /// \param db_list
        /// \param retry_time
        /// \return
        turbo::Status list_database(std::vector<std::string> &db_list, int *retry_time = nullptr);

        ///
        /// \param ns
        /// \param db_list
        /// \param retry_time
        /// \return
        turbo::Status list_database(std::string &ns, std::vector<std::string> &db_list, int *retry_time = nullptr);

        ///
        /// \param ns_list
        /// \param retry_time
        /// \return
        turbo::Status list_database_to_json(std::vector<std::string> &db_list, int *retry_time = nullptr);

        ///
        /// \param ns
        /// \param ns_list
        /// \param retry_time
        /// \return
        turbo::Status
        list_database_to_json(const std::string &ns, std::vector<std::string> &ns_list, int *retry_time = nullptr);

        ///
        /// \param save_path
        /// \param retry_time
        /// \return
        turbo::Status list_database_to_file(const std::string &save_path, int *retry_time = nullptr);

        ///
        /// \param ns
        /// \param save_path
        /// \param retry_time
        /// \return
        turbo::Status
        list_database_to_file(const std::string &ns, const std::string &save_path, int *retry_time = nullptr);

        ///
        /// \param ns_name
        /// \param db_pb
        /// \param retry_time
        /// \return
        turbo::Status
        get_database(const std::string &ns_name, const std::string &db_name, EA::proto::DataBaseInfo &db_pb,
                     int *retry_time = nullptr);

        ///
        /// \param ns_name
        /// \param json_str
        /// \param retry_time
        /// \return
        turbo::Status get_database_json(const std::string &ns_name, const std::string &db_name, std::string &json_str,
                                        int *retry_time = nullptr);

        ///
        /// \param ns_name
        /// \param json_path
        /// \param retry_time
        /// \return
        turbo::Status
        save_database_json(const std::string &ns_name, const std::string &db_name, const std::string &json_path,
                           int *retry_time = nullptr);

        ///
        /// \param info
        /// \param retry_time
        /// \return
        turbo::Status create_zone(EA::proto::ZoneInfo &info, int *retry_time = nullptr);

        ///
        /// \param ns
        /// \param zone
        /// \param quota
        /// \param retry_time
        /// \return
        turbo::Status
        create_zone(const std::string &ns, const std::string &zone, int64_t quota = 0, int *retry_time = nullptr);

        ///
        /// \param json_str
        /// \param retry_time
        /// \return
        turbo::Status create_zone_by_json(const std::string &json_str, int *retry_time = nullptr);

        ///
        /// \param path
        /// \param retry_time
        /// \return
        turbo::Status create_zone_by_file(const std::string &path, int *retry_time = nullptr);

        ///
        /// \param ns
        /// \param zone
        /// \param retry_time
        /// \return
        turbo::Status remove_zone(const std::string &ns, const std::string &zone, int *retry_time = nullptr);

        ///
        /// \param zone_info
        /// \param retry_time
        /// \return
        turbo::Status modify_zone(EA::proto::ZoneInfo &zone_info, int *retry_time = nullptr);

        ///
        /// \param json_str
        /// \param retry_time
        /// \return
        turbo::Status modify_zone_by_json(const std::string &json_str, int *retry_time = nullptr);

        ///
        /// \param path
        /// \param retry_time
        /// \return
        turbo::Status modify_zone_by_file(const std::string &path, int *retry_time = nullptr);

        ///
        /// \param zone_list
        /// \param retry_time
        /// \return
        turbo::Status list_zone(std::vector<EA::proto::ZoneInfo> &zone_list, int *retry_time = nullptr);

        ///
        /// \param ns
        /// \param zone_list
        /// \param retry_time
        /// \return
        turbo::Status
        list_zone(const std::string &ns, std::vector<EA::proto::ZoneInfo> &zone_list, int *retry_time = nullptr);

        ///
        /// \param zone_list
        /// \param retry_time
        /// \return
        turbo::Status list_zone(std::vector<std::string> &zone_list, int *retry_time = nullptr);

        ///
        /// \param ns
        /// \param zone_list
        /// \param retry_time
        /// \return
        turbo::Status list_zone(std::string &ns, std::vector<std::string> &zone_list, int *retry_time = nullptr);

        ///
        /// \param ns_list
        /// \param retry_time
        /// \return
        turbo::Status list_zone_to_json(std::vector<std::string> &zone_list, int *retry_time = nullptr);

        ///
        /// \param ns
        /// \param ns_list
        /// \param retry_time
        /// \return
        turbo::Status
        list_zone_to_json(const std::string &ns, std::vector<std::string> &zone_list, int *retry_time = nullptr);

        ///
        /// \param save_path
        /// \param retry_time
        /// \return
        turbo::Status list_zone_to_file(const std::string &save_path, int *retry_time = nullptr);

        ///
        /// \param ns
        /// \param save_path
        /// \param retry_time
        /// \return
        turbo::Status list_zone_to_file(const std::string &ns, const std::string &save_path, int *retry_time = nullptr);

        ///
        /// \param ns_name
        /// \param zone_name
        /// \param zone_pb
        /// \param retry_time
        /// \return
        turbo::Status get_zone(const std::string &ns_name, const std::string &zone_name, EA::proto::ZoneInfo &zone_pb,
                               int *retry_time = nullptr);

        ///
        /// \param ns_name
        /// \param json_str
        /// \param retry_time
        /// \return
        turbo::Status get_zone_json(const std::string &ns_name, const std::string &zone_name, std::string &json_str,
                                    int *retry_time = nullptr);

        ///
        /// \param ns_name
        /// \param json_path
        /// \param retry_time
        /// \return
        turbo::Status
        save_zone_json(const std::string &ns_name, const std::string &zone_name, const std::string &json_path,
                       int *retry_time = nullptr);

        ///
        /// \param servlet_info
        /// \param retry_time
        /// \return
        turbo::Status create_servlet(EA::proto::ServletInfo &servlet_info, int *retry_time = nullptr);

        ///
        /// \param ns
        /// \param zone
        /// \param servlet
        /// \param retry_time
        /// \return
        turbo::Status
        create_servlet(const std::string &ns, const std::string &zone, const std::string &servlet, int *retry_time = nullptr);

        ///
        /// \param json_str
        /// \param retry_time
        /// \return
        turbo::Status create_servlet_by_json(const std::string &json_str, int *retry_time = nullptr);

        ///
        /// \param path
        /// \param retry_time
        /// \return
        turbo::Status create_servlet_by_file(const std::string &path, int *retry_time = nullptr);

        ///
        /// \param ns
        /// \param zone
        /// \param servlet
        /// \param retry_time
        /// \return
        turbo::Status remove_servlet(const std::string &ns, const std::string &zone, const std::string &servlet, int *retry_time = nullptr);

        ///
        /// \param zone_info
        /// \param retry_time
        /// \return
        turbo::Status modify_servlet(EA::proto::ServletInfo &servlet_info, int *retry_time = nullptr);

        ///
        /// \param json_str
        /// \param retry_time
        /// \return
        turbo::Status modify_servlet_by_json(const std::string &json_str, int *retry_time = nullptr);

        ///
        /// \param path
        /// \param retry_time
        /// \return
        turbo::Status modify_servlet_by_file(const std::string &path, int *retry_time = nullptr);

        ///
        /// \param servlet_list
        /// \param retry_time
        /// \return
        turbo::Status list_servlet(std::vector<EA::proto::ServletInfo> &servlet_list, int *retry_time = nullptr);

        ///
        /// \param ns
        /// \param servlet_list
        /// \param retry_time
        /// \return
        turbo::Status
        list_servlet(const std::string &ns, std::vector<EA::proto::ServletInfo> &servlet_list, int *retry_time = nullptr);

        ///
        /// \param ns
        /// \param zone
        /// \param servlet_list
        /// \param retry_time
        /// \return
        turbo::Status
        list_servlet(const std::string &ns, const std::string &zone, std::vector<EA::proto::ServletInfo> &servlet_list, int *retry_time = nullptr);

        ///
        /// \param servlet_list
        /// \param retry_time
        /// \return
        turbo::Status list_servlet(std::vector<std::string> &servlet_list, int *retry_time = nullptr);

        ///
        /// \param ns
        /// \param servlet_list
        /// \param retry_time
        /// \return
        turbo::Status list_servlet(const std::string &ns, std::vector<std::string> &servlet_list, int *retry_time = nullptr);

        ///
        /// \param ns
        /// \param zone
        /// \param servlet_list
        /// \param retry_time
        /// \return
        turbo::Status list_servlet(const std::string &ns, const std::string &zone, std::vector<std::string> &servlet_list, int *retry_time = nullptr);

        ///
        /// \param servlet_list
        /// \param retry_time
        /// \return
        turbo::Status list_servlet_to_json(std::vector<std::string> &servlet_list, int *retry_time = nullptr);

        ///
        /// \param ns
        /// \param servlet_list
        /// \param retry_time
        /// \return
        turbo::Status list_servlet_to_json(const std::string &ns, std::vector<std::string> &servlet_list, int *retry_time = nullptr);

        ///
        /// \param ns
        /// \param zone
        /// \param servlet_list
        /// \param retry_time
        /// \return
        turbo::Status list_servlet_to_json(const std::string &ns, const std::string &zone, std::vector<std::string> &servlet_list, int *retry_time = nullptr);

        ///
        /// \param save_path
        /// \param retry_time
        /// \return
        turbo::Status list_servlet_to_file(const std::string &save_path, int *retry_time = nullptr);

        ///
        /// \param ns
        /// \param save_path
        /// \param retry_time
        /// \return
        turbo::Status list_servlet_to_file(const std::string & ns, const std::string &save_path, int *retry_time = nullptr);

        ///
        /// \param ns
        /// \param zone
        /// \param save_path
        /// \param retry_time
        /// \return
        turbo::Status list_servlet_to_file(const std::string & ns, const std::string &zone, const std::string &save_path, int *retry_time = nullptr);

        ///
        /// \param ns_name
        /// \param zone_name
        /// \param servlet
        /// \param servlet_pb
        /// \param retry_time
        /// \return
        turbo::Status get_servlet(const std::string &ns_name, const std::string &zone_name, const std::string &servlet, EA::proto::ServletInfo &servlet_pb,
                               int *retry_time = nullptr);

        ///
        /// \param ns_name
        /// \param zone_name
        /// \param servlet
        /// \param json_str
        /// \param retry_time
        /// \return
        turbo::Status get_servlet_json(const std::string &ns_name, const std::string &zone_name, const std::string &servlet, std::string &json_str,
                                    int *retry_time = nullptr);

        ///
        /// \param ns_name
        /// \param zone_name
        /// \param servlet
        /// \param json_path
        /// \param retry_time
        /// \return
        turbo::Status
        save_servlet_json(const std::string &ns_name, const std::string &zone_name, const std::string &servlet,const std::string &json_path,
                       int *retry_time = nullptr);

        ///
        /// \param logical_list
        /// \param retry_time
        /// \return
        turbo::Status add_logical(const std::vector<std::string> &logical_list, int *retry_time = nullptr);

        ///
        /// \param logical
        /// \param retry_time
        /// \return
        turbo::Status add_logical(const std::string &logical, int *retry_time = nullptr);

        ///
        /// \param logical
        /// \param retry_time
        /// \return
        turbo::Status remove_logical(const std::vector<std::string> &logical_list, int *retry_time = nullptr);

        ///
        /// \param logical
        /// \param retry_time
        /// \return
        turbo::Status remove_logical(const std::string &logical, int *retry_time = nullptr);

        ///
        /// \param logical
        /// \param physical
        /// \param retry_time
        /// \return
        turbo::Status add_physical(const std::string &logical, const std::vector<std::string> &physicals, int *retry_time = nullptr);

        ///
        /// \param logical
        /// \param physical
        /// \param retry_time
        /// \return
        turbo::Status add_physical(const std::string &logical, const std::string &physical, int *retry_time = nullptr);

        ///
        /// \param logical
        /// \param physicals
        /// \param retry_time
        /// \return
        turbo::Status remove_physical(const std::string &logical, const std::vector<std::string> &physicals, int *retry_time = nullptr);

        ///
        /// \param logical
        /// \param physical
        /// \param retry_time
        /// \return
        turbo::Status remove_physical(const std::string &logical, const std::string &physical, int *retry_time = nullptr);

        ///
        /// \param logical_from
        /// \param logical_to
        /// \param physical
        /// \param retry_time
        /// \return
        turbo::Status move_physical(const std::string &logical_from, const std::string &logical_to, const std::string &physical, int *retry_time = nullptr);

        ///
        /// \param retry_time
        /// \return
        turbo::Status list_logical(std::vector<std::string> &logicals, int *retry_time = nullptr);

        ///
        /// \param logical
        /// \param rooms
        /// \param retry_time
        /// \return
        turbo::Status get_logical(const std::string &logical, EA::proto::PhysicalRoom &rooms, int *retry_time = nullptr);

        ///
        /// \param logical
        /// \param physicals
        /// \param retry_time
        /// \return
        turbo::Status get_logical(const std::string &logical, std::vector<std::string> &physicals, int *retry_time = nullptr);

        ///
        /// \param rooms
        /// \param retry_time
        /// \return
        turbo::Status list_physical(std::vector<EA::proto::PhysicalRoom> &rooms, int *retry_time = nullptr);

        turbo::Status list_instance(std::vector<EA::proto::InstanceInfo> &instance_list, int *retry_time = nullptr);

        ///
        /// \param request
        /// \param response
        /// \param retry_time
        /// \return
        turbo::Status meta_manager(const EA::proto::MetaManagerRequest &request,
                                   EA::proto::MetaManagerResponse &response, int *retry_time);

        ///
        /// \param request
        /// \param response
        /// \param retry_times
        /// \return
        turbo::Status meta_query(const EA::proto::QueryRequest &request,
                                 EA::proto::QueryResponse &response, int *retry_time);

    private:
        BaseMessageSender *_sender;
    };

    inline turbo::Status MetaClient::meta_manager(const EA::proto::MetaManagerRequest &request,
                               EA::proto::MetaManagerResponse &response, int *retry_time) {
        if(!retry_time) {
            return _sender->meta_manager(request, response);
        }
        return _sender->meta_manager(request, response, *retry_time);
    }

    inline turbo::Status MetaClient::meta_query(const EA::proto::QueryRequest &request,
                             EA::proto::QueryResponse &response, int *retry_time) {
        if(!retry_time) {
            return _sender->meta_query(request, response);
        }
        return _sender->meta_query(request, response, *retry_time);
    }

}  // namespace EA::client

#endif // ELASTICANN_CLIENT_META_H_
