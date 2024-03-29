//
// Created by jeff on 23-7-8.
//

#ifndef ELASTICANN_COMMON_PROTO_HELPER_H_
#define ELASTICANN_COMMON_PROTO_HELPER_H_

#include "turbo/format/format.h"
#include "eaproto/db/meta.interface.pb.h"
#include "eaproto/db/plan.pb.h"
#include "rocksdb/db.h"
#include "rocksdb/utilities/memory_util.h"
#include "rapidjson/rapidjson.h"
#include "rapidjson/reader.h"
#include "braft/log_entry.h"
#include "re2/re2.h"

namespace fmt {
    template<>
    struct formatter<EA::proto::PrimitiveType> : public formatter<int> {
        auto format(const EA::proto::PrimitiveType& a, format_context& ctx) const {
            return formatter<int>::format(static_cast<int>(a), ctx);
        }
    };

    template<>
    struct formatter<EA::proto::IndexType> : public formatter<int> {
        auto format(const EA::proto::IndexType& a, format_context& ctx) const {
            return formatter<int>::format(static_cast<int>(a), ctx);
        }
    };

    template<>
    struct formatter<rocksdb::Status::Code> : public formatter<int> {
        auto format(const rocksdb::Status::Code& a, format_context& ctx) const {
            return formatter<int>::format(static_cast<int>(a), ctx);
        }
    };

    template<>
    struct formatter<EA::proto::OpType> : public formatter<int> {
        auto format(const EA::proto::OpType& a, format_context& ctx) const {
            return formatter<int>::format(static_cast<int>(a), ctx);
        }
    };

    template<>
    struct formatter<rapidjson::ParseErrorCode> : public formatter<int> {
        auto format(const rapidjson::ParseErrorCode& a, format_context& ctx) const {
            return formatter<int>::format(static_cast<int>(a), ctx);
        }
    };

    template<>
    struct formatter<braft::EntryType> : public formatter<int> {
        auto format(const braft::EntryType& a, format_context& ctx) const {
            return formatter<int>::format(static_cast<int>(a), ctx);
        }
    };

    template<>
    struct formatter<::EA::proto::RaftControlOp> : public formatter<int> {
        auto format(const ::EA::proto::RaftControlOp& a, format_context& ctx) const {
            return formatter<int>::format(static_cast<int>(a), ctx);
        }
    };

    template<>
    struct formatter<::EA::proto::ExprNodeType> : public formatter<int> {
        auto format(const ::EA::proto::ExprNodeType& a, format_context& ctx) const {
            return formatter<int>::format(static_cast<int>(a), ctx);
        }
    };

    template<>
    struct formatter<::EA::proto::PlanNodeType> : public formatter<int> {
        auto format(const ::EA::proto::PlanNodeType& a, format_context& ctx) const {
            return formatter<int>::format(static_cast<int>(a), ctx);
        }
    };

    template<>
    struct formatter<::re2::RE2::ErrorCode> : public formatter<int> {
        auto format(const ::re2::RE2::ErrorCode& a, format_context& ctx) const {
            return formatter<int>::format(static_cast<int>(a), ctx);
        }
    };

    template<>
    struct formatter<rocksdb::MemoryUtil::UsageType> : public formatter<int> {
        auto format(const rocksdb::MemoryUtil::UsageType& a, format_context& ctx) const {
            return formatter<int>::format(static_cast<int>(a), ctx);
        }
    };
}
#endif  // ELASTICANN_COMMON_PROTO_HELPER_H_
