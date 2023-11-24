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
#include "elasticann/client/utility.h"
#include "turbo/strings/utility.h"
#include "turbo/container/flat_hash_set.h"

namespace EA::client {

    std::string config_type_to_string(EA::proto::ConfigType type) {
        switch (type) {
            case EA::proto::CF_JSON:
                return "json";
            case EA::proto::CF_TEXT:
                return "text";
            case EA::proto::CF_INI:
                return "ini";
            case EA::proto::CF_YAML:
                return "yaml";
            case EA::proto::CF_XML:
                return "xml";
            case EA::proto::CF_GFLAGS:
                return "gflags";
            case EA::proto::CF_TOML:
                return "toml";
            default:
                return "unknown format";
        }
    }

    turbo::ResultStatus<EA::proto::ConfigType> string_to_config_type(const std::string &str) {
        auto lc = turbo::StrToLower(str);
        if (lc == "json") {
            return EA::proto::CF_JSON;
        } else if (lc == "text") {
            return EA::proto::CF_TEXT;
        } else if (lc == "ini") {
            return EA::proto::CF_INI;
        } else if (lc == "yaml") {
            return EA::proto::CF_YAML;
        } else if (lc == "xml") {
            return EA::proto::CF_XML;
        } else if (lc == "gflags") {
            return EA::proto::CF_GFLAGS;
        } else if (lc == "toml") {
            return EA::proto::CF_TOML;
        }
        return turbo::InvalidArgumentError("unknown format '{}'", str);
    }

    turbo::Status string_to_version(const std::string &str, EA::proto::Version *v) {
        std::vector<std::string> vs = turbo::StrSplit(str, ".");
        if (vs.size() != 3)
            return turbo::InvalidArgumentError("version error, should be like 1.2.3");
        int64_t m;
        if (!turbo::SimpleAtoi(vs[0], &m)) {
            return turbo::InvalidArgumentError("version error, should be like 1.2.3");
        }
        v->set_major(m);
        if (!turbo::SimpleAtoi(vs[1], &m)) {
            return turbo::InvalidArgumentError("version error, should be like 1.2.3");
        }
        v->set_minor(m);
        if (!turbo::SimpleAtoi(vs[2], &m)) {
            return turbo::InvalidArgumentError("version error, should be like 1.2.3");
        }
        v->set_patch(m);
        return turbo::OkStatus();
    }

    std::string version_to_string(const EA::proto::Version &v) {
        return turbo::Format("{}.{}.{}", v.major(), v.minor(), v.patch());
    }

    static turbo::flat_hash_set<char> AllowChar{'a', 'b', 'c', 'd', 'e', 'f', 'g',
                                                'h', 'i', 'j', 'k', 'l', 'm', 'n',
                                                'o', 'p', 'q', 'r', 's', 't',
                                                'u', 'v', 'w', 'x', 'y', 'z',
                                                '0','1','2','3','4','5','6','7','8','9',
                                                '_',
                                                'A', 'B', 'C', 'D', 'E', 'F', 'G',
                                                'H', 'I', 'J', 'K', 'L', 'M', 'N',
                                                'O', 'P', 'Q', 'R', 'S', 'T',
                                                'U', 'V', 'Q', 'X', 'Y', 'Z',
    };

    turbo::Status CheckValidNameType(std::string_view ns) {
        int i = 0;
        for(auto c : ns) {
            if(AllowChar.find(c) == AllowChar.end()) {
                return turbo::InvalidArgumentError("the {} char {} of {} is not allow used in name the valid set is[a-z,A-Z,0-9,_]", i, c, ns);
            }
            ++i;
        }
        return turbo::OkStatus();
    }

}  // namespace EA::client
