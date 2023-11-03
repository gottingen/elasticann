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


#include "elasticann/common/common.h"
#include <unordered_map>
#include <cstdlib>
#include <cctype>
#include <sstream>
#include <json2pb/pb_to_json.h>
#include <json2pb/json_to_pb.h>
#include <butil/endpoint.h>
#include <brpc/channel.h>
#include <brpc/server.h>
#include <brpc/controller.h>
#include <butil/file_util.h>
#include "rocksdb/slice.h"
#include <google/protobuf/descriptor.pb.h>
#include "rocksdb/slice.h"
#include "elasticann/common/expr_value.h"
#include "re2/re2.h"
#include "turbo/strings/str_split.h"

using google::protobuf::FieldDescriptorProto;

namespace EA {
DEFINE_int32(raft_write_concurrency, 40, "raft_write concurrency, default:40");
DEFINE_int32(service_write_concurrency, 40, "service_write concurrency, default:40");
DEFINE_int32(snapshot_load_num, 4, "snapshot load concurrency, default 4");
DEFINE_int32(baikal_heartbeat_concurrency, 10, "baikal heartbeat concurrency, default:10");
DEFINE_int64(incremental_info_gc_time, 600 * 1000 * 1000, "time interval to clear incremental info");
DECLARE_string(default_physical_room);
DEFINE_bool(enable_self_trace, true, "open SELF_TRACE log");
DEFINE_bool(servitysinglelog, true, "diff servity message in seperate logfile");
DEFINE_bool(open_service_write_concurrency, true, "open service_write_concurrency, default: true");
DEFINE_int32(baikal_heartbeat_interval_us, 10 * 1000 * 1000, "baikal_heartbeat_interval(us)");
DEFINE_bool(schema_ignore_case, false, "whether ignore case when match db/table name");
DEFINE_bool(disambiguate_select_name, false, "whether use the first when select name is ambiguous, default false");
DEFINE_int32(new_sign_read_concurrency, 10, "new_sign_read concurrency, default:20");
DEFINE_bool(open_new_sign_read_concurrency, false, "open new_sign_read concurrency, default: false");
DEFINE_bool(need_verify_ddl_permission, false, "default true");

int64_t timestamp_diff(timeval _start, timeval _end) {
    return (_end.tv_sec - _start.tv_sec) * 1000000 
        + (_end.tv_usec-_start.tv_usec); //macro second
}

std::string pb2json(const google::protobuf::Message& message) {
    std::string json;
    std::string error;

    if (json2pb::ProtoMessageToJson(message, &json, &error)) {
        return json;
    }
    return error;
}

std::string json2pb(const std::string& json, google::protobuf::Message* message) {
    std::string error;

    if (json2pb::JsonToProtoMessage(json, message, &error)) {
        return "";
    }
    return error;
}

// STMPS_SUCCESS,
// STMPS_FAIL,
// STMPS_NEED_RESIZE
SerializeStatus to_string (int32_t number, char *buf, size_t size, size_t& len) {
    if (number == 0U) {
        len = 1;
        if (size < 1) {
            return STMPS_NEED_RESIZE;
        }
        buf[0] = '0';
        return STMPS_SUCCESS;
    }
    if (number == INT32_MIN) {
        len = 11;
        if (size < len) {
            return STMPS_NEED_RESIZE;
        }
        memcpy(buf, "-2147483648", len);
        return STMPS_SUCCESS;
    }
    len = 0;
    bool negtive = false;
    if (number < 0) {
        number = -number;
        negtive = true;
        len++;
    }

    int32_t n = number;
    while (n > 0) {
        n /= 10;
        len++;
    }
    if (len > size) {
        return STMPS_NEED_RESIZE;
    }
    if (negtive) {
        buf[0] = '-';
    }
    int length = len;
    while (number > 0) {
        buf[--length] = '0' + (number % 10);
        number /= 10;
    }
    return STMPS_SUCCESS;
}

std::string to_string(int32_t number)
{
    char buffer[16];
    size_t len = 0;
    SerializeStatus ret = to_string(number, buffer, 16, len);
    if (ret == STMPS_SUCCESS) {
        buffer[len] = '\0';
        return std::string(buffer);
    }
    return "";
}

SerializeStatus to_string (uint32_t number, char *buf, size_t size, size_t& len) {

    if (number == 0U) {
        len = 1;
        if (size < len) {
            return STMPS_NEED_RESIZE;
        }
        buf[0] = '0';
        return STMPS_SUCCESS;
    }
    len = 0;
    uint32_t n = number;

    while (n > 0) {
        n /= 10;
        len++;
    }
    if (len > size) {
        return STMPS_NEED_RESIZE;
    }
    int length = len;
    while (number > 0) {
        buf[--length] = '0' + (number % 10);
        number /= 10;
    }

    return STMPS_SUCCESS;
}

std::string to_string(uint32_t number)
{
    char buffer[16];
    size_t len = 0;
    SerializeStatus ret = to_string(number, buffer, 16, len);
    if (ret == STMPS_SUCCESS) {
        buffer[len] = '\0';
        return std::string(buffer);
    }
    return "";
}

SerializeStatus to_string (int64_t number, char *buf, size_t size, size_t& len) {
    if (number == 0UL) {
        len = 1;
        if (size < len) {
            return STMPS_NEED_RESIZE;
        }
        buf[0] = '0';
        return STMPS_SUCCESS;
    }

    if (number == INT64_MIN) {
        len = 20;
        if (size < len) {
            return STMPS_NEED_RESIZE;
        }
        memcpy(buf, "-9223372036854775808", len);
        return STMPS_SUCCESS;
    }
    len = 0;
    bool negtive = false;
    if (number < 0) {
        number = -number;
        negtive = true;
        len++;
    }

    int64_t n = number;
    while (n > 0) {
        n /= 10;
        len++;
    }
    if (len > size) {
        return STMPS_NEED_RESIZE;
    }
    if (negtive) {
        buf[0] = '-';
    }
    int length = len;
    while (number > 0) {
        buf[--length] = '0' + (number % 10);
        number /= 10;
    }

    return STMPS_SUCCESS;
}

std::string to_string(int64_t number)
{
    char buffer[32];
    size_t len = 0;
    SerializeStatus ret = to_string(number, buffer, 32, len);
    if (ret == STMPS_SUCCESS) {
        buffer[len] = '\0';
        return std::string(buffer);
    }
    return "";
}

SerializeStatus to_string (uint64_t number, char *buf, size_t size, size_t& len) {
    if (number == 0UL) {
        len = 1;
        if (size < len) {
            return STMPS_NEED_RESIZE;
        }
        buf[0] = '0';
        return STMPS_SUCCESS;
    }
    len = 0;
    uint64_t n = number;

    while (n > 0) {
        n /= 10;
        len++;
    }
    if (len > size) {
        return STMPS_NEED_RESIZE;
    }

    int length = len;
    while (number > 0) {
        buf[--length] = '0' + (number % 10);
        number /= 10;
    }
    return STMPS_SUCCESS;
}

std::string to_string(uint64_t number)
{
    char buffer[32];
    size_t len = 0;
    SerializeStatus ret = to_string(number, buffer, 32, len);
    if (ret == STMPS_SUCCESS) {
        buffer[len] = '\0';
        return std::string(buffer);
    }
    return "";
}

std::string remove_quote(const char* str, char quote) {
    uint32_t len = strlen(str);
    if (len > 2 && str[0] == quote && str[len-1] == quote) {
        return std::string(str + 1, len - 2);
    } else {
        return std::string(str);
    }
}

std::string str_to_hex(const std::string& str) {
    return rocksdb::Slice(str).ToString(true);
}

bool is_digits(const std::string& str) {
    return std::all_of(str.begin(), str.end(), ::isdigit);
}

void stripslashes(std::string& str, bool is_gbk) {
    size_t slow = 0;
    size_t fast = 0;
    bool has_slash = false;
    static std::unordered_map<char, char> trans_map = {
        {'\\', '\\'},
        {'\"', '\"'},
        {'\'', '\''},
        {'r', '\r'},
        {'t', '\t'},
        {'n', '\n'},
        {'b', '\b'},
        {'Z', '\x1A'},
    };
    while (fast < str.size()) {
        if (has_slash) {
            if (trans_map.count(str[fast]) == 1) {
                str[slow++] = trans_map[str[fast++]];
            } else if (str[fast] == '%' || str[fast] == '_') {
                // like中的特殊符号，需要补全'\'
                str[slow++] = '\\';
                str[slow++] = str[fast++];
            }
            has_slash = false;
        } else {
            if (str[fast] == '\\') {
                has_slash = true;
                fast++;
            } else if (is_gbk && (str[fast] & 0x80) != 0) {
                //gbk中文字符处理
                str[slow++] = str[fast++];
                if (fast >= str.size()) {
                    // 去除最后半个gbk中文
                    //--slow;
                    break;
                }
                str[slow++] = str[fast++];
            } else {
                str[slow++] = str[fast++];
            }
        }
    }
    str.resize(slow);
}

void update_op_version(proto::SchemaConf* p_conf, const std::string& desc) {
    auto version = p_conf->has_op_version() ? p_conf->op_version() : 0;
    p_conf->set_op_version(version + 1);
    p_conf->set_op_desc(desc);
}

void update_schema_conf_common(const std::string& table_name, const proto::SchemaConf& schema_conf, proto::SchemaConf* p_conf) {
        const google::protobuf::Reflection* src_reflection = schema_conf.GetReflection();
        //const google::protobuf::Descriptor* src_descriptor = schema_conf.GetDescriptor();
        const google::protobuf::Reflection* dst_reflection = p_conf->GetReflection();
        const google::protobuf::Descriptor* dst_descriptor = p_conf->GetDescriptor();
        const google::protobuf::FieldDescriptor* src_field = nullptr;
        const google::protobuf::FieldDescriptor* dst_field = nullptr;

        std::vector<const google::protobuf::FieldDescriptor*> src_field_list;
        src_reflection->ListFields(schema_conf, &src_field_list);
        for (int i = 0; i < (int)src_field_list.size(); ++i) {
            src_field = src_field_list[i];
            if (src_field == nullptr) {
                continue;
            }

            dst_field = dst_descriptor->FindFieldByName(src_field->name());
            if (dst_field == nullptr) {
                continue;
            }

            if (src_field->cpp_type() != dst_field->cpp_type()) {
                continue;
            }

            auto type = src_field->cpp_type();
            switch (type) {
                case google::protobuf::FieldDescriptor::CPPTYPE_INT32: {
                    auto src_value = src_reflection->GetInt32(schema_conf, src_field);
                    dst_reflection->SetInt32(p_conf, dst_field, src_value);
                } break;
                case google::protobuf::FieldDescriptor::CPPTYPE_UINT32: {
                    auto src_value = src_reflection->GetUInt32(schema_conf, src_field);
                    dst_reflection->SetUInt32(p_conf, dst_field, src_value);
                } break;
                case google::protobuf::FieldDescriptor::CPPTYPE_INT64: {
                    auto src_value = src_reflection->GetInt64(schema_conf, src_field);
                    dst_reflection->SetInt64(p_conf, dst_field, src_value);
                } break;
                case google::protobuf::FieldDescriptor::CPPTYPE_UINT64: {
                    auto src_value = src_reflection->GetUInt64(schema_conf, src_field);
                    dst_reflection->SetUInt64(p_conf, dst_field, src_value);
                } break;
                case google::protobuf::FieldDescriptor::CPPTYPE_FLOAT: {
                    auto src_value = src_reflection->GetFloat(schema_conf, src_field);
                    dst_reflection->SetFloat(p_conf, dst_field, src_value);
                } break;
                case google::protobuf::FieldDescriptor::CPPTYPE_DOUBLE: {
                    auto src_value = src_reflection->GetDouble(schema_conf, src_field);
                    dst_reflection->SetDouble(p_conf, dst_field, src_value);
                } break;
                case google::protobuf::FieldDescriptor::CPPTYPE_BOOL: {
                    auto src_value = src_reflection->GetBool(schema_conf, src_field);
                    dst_reflection->SetBool(p_conf, dst_field, src_value);
                } break;
                case google::protobuf::FieldDescriptor::CPPTYPE_STRING: {
                    auto src_value = src_reflection->GetString(schema_conf, src_field);
                    dst_reflection->SetString(p_conf, dst_field, src_value);
                } break;
                case google::protobuf::FieldDescriptor::CPPTYPE_ENUM: {
                    auto src_value = src_reflection->GetEnum(schema_conf, src_field);
                    dst_reflection->SetEnum(p_conf, dst_field, src_value);
                } break;
                default: {
                    break;
                }
            }

        }
        TLOG_WARN("{} schema conf UPDATE TO : {}", table_name, schema_conf.ShortDebugString());
}

int primitive_to_proto_type(proto::PrimitiveType type) {
    using google::protobuf::FieldDescriptorProto;
    static std::unordered_map<int32_t, int32_t> _mysql_pb_type_mapping = {
        { proto::INT8,         FieldDescriptorProto::TYPE_SINT32 },
        { proto::INT16,        FieldDescriptorProto::TYPE_SINT32 },
        { proto::INT32,        FieldDescriptorProto::TYPE_SINT32 },
        { proto::INT64,        FieldDescriptorProto::TYPE_SINT64 },
        { proto::UINT8,        FieldDescriptorProto::TYPE_UINT32 },
        { proto::UINT16,       FieldDescriptorProto::TYPE_UINT32 },
        { proto::UINT32,       FieldDescriptorProto::TYPE_UINT32 },
        { proto::UINT64,       FieldDescriptorProto::TYPE_UINT64 },
        { proto::FLOAT,        FieldDescriptorProto::TYPE_FLOAT  },
        { proto::DOUBLE,       FieldDescriptorProto::TYPE_DOUBLE },
        { proto::STRING,       FieldDescriptorProto::TYPE_BYTES  },
        { proto::DATETIME,     FieldDescriptorProto::TYPE_FIXED64},
        { proto::TIMESTAMP,    FieldDescriptorProto::TYPE_FIXED32},
        { proto::DATE,         FieldDescriptorProto::TYPE_FIXED32},
        { proto::TIME,         FieldDescriptorProto::TYPE_SFIXED32},
        { proto::HLL,          FieldDescriptorProto::TYPE_BYTES},
        { proto::BOOL,         FieldDescriptorProto::TYPE_BOOL},
        { proto::BITMAP,       FieldDescriptorProto::TYPE_BYTES},
        { proto::TDIGEST,      FieldDescriptorProto::TYPE_BYTES},
        { proto::NULL_TYPE,    FieldDescriptorProto::TYPE_BOOL}
    };
    if (_mysql_pb_type_mapping.count(type) == 0) {
        TLOG_WARN("mysql_type {} not supported.", type);
        return -1;
    }
    return _mysql_pb_type_mapping[type];
}
int get_physical_room(const std::string& ip_and_port_str, std::string& physical_room) {
    physical_room = FLAGS_default_physical_room;
    return 0;
}

int get_instance_from_bns(int* ret,
                          const std::string& bns_name,
                          std::vector<std::string>& instances,
                          bool need_alive,
                          bool white_list) {
    return -1;
}

bool same_with_container_id_and_address(const std::string& container_id, const std::string& address) {
    return true;
}

int get_multi_port_from_bns(int* ret,
                          const std::string& bns_name,
                          std::vector<std::string>& instances,
                          bool need_alive) {
    return -1;
}

static unsigned char to_hex(unsigned char x)   {   
    return  x > 9 ? x + 55 : x + 48;   
}

static unsigned char from_hex(unsigned char x) {   
    unsigned char y = '\0';  
    if (x >= 'A' && x <= 'Z') { 
        y = x - 'A' + 10;  
    } else if (x >= 'a' && x <= 'z') { 
        y = x - 'a' + 10;  
    } else if (x >= '0' && x <= '9') {
        y = x - '0';  
    }
    return y;  
}  

std::string url_decode(const std::string& str) {
    std::string strTemp = "";  
    size_t length = str.length();  
    for (size_t i = 0; i < length; i++)  {  
        if (str[i] == '+') {
            strTemp += ' ';
        }  else if (str[i] == '%')  {  
            unsigned char high = from_hex((unsigned char)str[++i]);  
            unsigned char low = from_hex((unsigned char)str[++i]);  
            strTemp += high * 16 + low;  
        }  
        else strTemp += str[i];  
    }  
    return strTemp;  
}

std::vector<std::string> string_split(const std::string &s, char delim) {
    std::stringstream ss(s);
    std::string item;
    std::vector<std::string> elems;
    while (std::getline(ss, item, delim)) {
        elems.emplace_back(item);
        // elems.push_back(std::move(item));
    }
    return elems;
}

int64_t parse_snapshot_index_from_path(const std::string& snapshot_path, bool use_dirname) {
    butil::FilePath path(snapshot_path);
    std::string tmp_path;
    if (use_dirname) {
        tmp_path = path.DirName().BaseName().value();
    } else {
        tmp_path = path.BaseName().value();
    }
    std::vector<std::string> split_vec;
    std::vector<std::string> snapshot_index_vec;
    split_vec =  turbo::StrSplit(tmp_path, '/', turbo::SkipEmpty());
    snapshot_index_vec = turbo::StrSplit(split_vec.back(), '_', turbo::SkipEmpty());
    int64_t snapshot_index = 0;
    if (snapshot_index_vec.size() == 2) {
        snapshot_index = atoll(snapshot_index_vec[1].c_str());
    }
    return snapshot_index;
}

bool ends_with(const std::string &str, const std::string &ending) {
    if (str.length() < ending.length()) {
        return false;
    }
    return str.compare(str.length() - ending.length(), ending.length(), ending) == 0;
}

std::string string_trim(std::string& str) {
    size_t first = str.find_first_not_of(' ');
    if (first == std::string::npos)
        return "";
    size_t last = str.find_last_not_of(' ');
    return str.substr(first, (last-first+1));
}

const std::string& rand_peer(proto::RegionInfo& info) {
    if (info.peers_size() == 0) {
        return info.leader();
    }
    uint32_t i = butil::fast_rand() % info.peers_size();
    return info.peers(i);
}

void other_peer_to_leader(proto::RegionInfo& info) {
    auto peer = rand_peer(info);
    if (peer != info.leader()) {
        info.set_leader(peer);
        return;
    }
    for (auto& peer : info.peers()) {
        if (peer != info.leader()) {
            info.set_leader(peer);
            break;
        }
    }
}

std::string url_encode(const std::string& str) {
    std::string strTemp = "";  
    size_t length = str.length();  
    for (size_t i = 0; i < length; i++) {  
        if (isalnum((unsigned char)str[i]) ||   
                (str[i] == '-') ||  
                (str[i] == '_') ||   
                (str[i] == '.') ||   
                (str[i] == '~')) {
            strTemp += str[i];  
        } else if (str[i] == ' ') {
            strTemp += "+";  
        } else  {  
            strTemp += '%';  
            strTemp += to_hex((unsigned char)str[i] >> 4);  
            strTemp += to_hex((unsigned char)str[i] % 16);  
        }  
    }  
    return strTemp; 
}

int brpc_with_http(const std::string& host, const std::string& url, std::string& response) {
    brpc::Channel channel;
    brpc::ChannelOptions options;
    options.protocol = brpc::PROTOCOL_HTTP;
    if (channel.Init(host.c_str() /*any url*/, &options) != 0) {
        TLOG_WARN("Fail to initialize channel, host: {}, url: {}", host, url);
        return -1;
    }

    brpc::Controller cntl;
    cntl.http_request().uri() = url;  // 设置为待访问的URL
    channel.CallMethod(nullptr, &cntl, nullptr, nullptr, nullptr/*done*/);
    TLOG_DEBUG("http status code : {}",cntl.http_response().status_code());
    response = cntl.response_attachment().to_string();
    TLOG_WARN("host: {}, url: {}, response: {}", host, url, response);
    return 0;
}

std::string store_or_db_bns_to_meta_bns(const std::string& bns) {
    auto serv_pos = bns.find(".serv");
    std::string bns_group = bns;
    if (serv_pos != bns.npos) {
        bns_group = bns.substr(0, serv_pos);
    }
    std::string bns_group2 = bns_group;
    auto pos1 = bns_group.find_first_of(".");
    auto pos2 = bns_group.find_last_of(".");
    if (pos1 == bns_group.npos || pos2 == bns_group.npos || pos2 <= pos1) {
        return "";
    }
    bns_group = "group" + bns_group.substr(pos1, pos2 - pos1 + 1) + "all";
    bns_group2 = bns_group2.substr(pos1 + 1);
    bool is_store_bns = false;
    if (bns_group.find("baikalStore") != bns_group.npos || bns_group.find("baikalBinlog") != bns_group.npos) {
        is_store_bns = true;
    }

    std::vector<std::string> instances;
    int retry_times = 0;
    while (true) {
        instances.clear();
        int ret2 = 0;
        int ret = 0;
        if (is_store_bns) {
            ret = get_instance_from_bns(&ret2, bns_group, instances);
        } else {
            ret = get_multi_port_from_bns(&ret2, bns_group, instances);
        }
        if (ret != 0 || instances.empty()) {
            if (++retry_times > 5) {
                return "";
            } else {
                if (retry_times > 3) {
                    bns_group = bns_group2;
                }
                continue;
            }
        } else {
            break;
        }
    }

    std::string meta_bns = "";
    for (const std::string& instance : instances) {
        std::string response;
        int ret = brpc_with_http(instance, instance + "/flags/meta_server_bns", response);
        if (ret != 0) {
            continue;
        }
        auto pos = response.find("(default");
        if (pos != response.npos) {
            response = response.substr(0, pos + 1);
        }
        re2::RE2::Options option;
        option.set_encoding(RE2::Options::EncodingLatin1);
        option.set_case_sensitive(false);
        option.set_perl_classes(true);

        re2::RE2 reg(".*(group.*baikalMeta.*all).*", option);
        meta_bns.clear();
        if (!RE2::Extract(response, reg, "\\1", &meta_bns)) {
            TLOG_WARN("extract commit error. response: {}", response);
            continue;
        }

        if (meta_bns.empty()) {
            continue;
        } else {
            break;
        }
    }

    TLOG_WARN("bns_group: {}; store_bns to meta bns : {} => {}", bns_group, bns, meta_bns);

    return meta_bns;
}
void parse_sample_sql(const std::string& sample_sql, std::string& database, std::string& table, std::string& sql) {
    // Remove comments.
    re2::RE2::Options option;
    option.set_encoding(RE2::Options::EncodingLatin1);
    option.set_case_sensitive(false);
    option.set_perl_classes(true);

    re2::RE2 reg("family_table_tag_optype_plat=\\[(.*)\t(.*)\t.*\t.*\t.*sql=\\[(.*)\\]", option);

    if (!RE2::Extract(sample_sql, reg, "\\1", &database)) {
        TLOG_WARN("extract commit error.");
    }
    if (!RE2::Extract(sample_sql, reg, "\\2", &table)) {
        TLOG_WARN("extract commit error.");
    }
    if (!RE2::Extract(sample_sql, reg, "\\3", &sql)) {
        TLOG_WARN("extract commit error.");
    }
    TLOG_WARN("sample_sql: {}, database: {}, table: {}, sql: {}", sample_sql, database, table, sql);
}

}  // EA
