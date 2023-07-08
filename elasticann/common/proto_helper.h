//
// Created by jeff on 23-7-8.
//

#ifndef ELASTICANN_COMMON_PROTO_HELPER_H_
#define ELASTICANN_COMMON_PROTO_HELPER_H_

#include "turbo/format/format.h"
#include "elasticann/proto/meta.interface.pb.h"
#include "rocksdb/db.h"

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
}
#endif  // ELASTICANN_COMMON_PROTO_HELPER_H_
