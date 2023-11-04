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

#pragma once

#include "elasticann/logical_plan/logical_planner.h"
#include "elasticann/logical_plan/query_context.h"
#include "elasticann/sqlparser/parser.h"

namespace EA {

    class DDLPlanner : public LogicalPlanner {
    public:

        DDLPlanner(QueryContext *ctx) : LogicalPlanner(ctx) {}

        virtual ~DDLPlanner() {}

        virtual int plan();

    private:
        int parse_create_table(proto::SchemaInfo &table);

        int parse_drop_table(proto::SchemaInfo &table);

        int parse_restore_table(proto::SchemaInfo &table);

        int parse_create_database(proto::DataBaseInfo &database);

        int parse_drop_database(proto::DataBaseInfo &database);

        int parse_alter_table(proto::MetaManagerRequest &alter_request);

        int check_partition_key_constraint(proto::SchemaInfo &table, const std::string &field_name);

        int add_column_def(proto::SchemaInfo &table, parser::ColumnDef *column);

        int add_constraint_def(proto::SchemaInfo &table, parser::Constraint *constraint, parser::AlterTableSpec *spec);

        bool
        is_fulltext_type_constraint(proto::StorageType pb_storage_type, bool &has_arrow_type, bool &has_pb_type) const;

        proto::PrimitiveType to_baikal_type(parser::FieldType *field_type);

        int parse_pre_split_keys(std::string split_start_key,
                                 std::string split_end_key,
                                 std::string global_start_key,
                                 std::string global_end_key,
                                 int32_t split_region_num, proto::SchemaInfo &table);

        int pre_split_index(const std::string &start_key,
                            const std::string &end_key,
                            int32_t region_num,
                            proto::SchemaInfo &table,
                            const proto::IndexInfo *pk_index,
                            const proto::IndexInfo *index,
                            const std::vector<const proto::FieldInfo *> &pk_fields,
                            const std::vector<const proto::FieldInfo *> &index_fields);

        int parse_modify_column(proto::MetaManagerRequest &alter_request,
                                const parser::TableName *table_name,
                                const parser::AlterTableSpec *alter_spec);

        std::map<std::string, bool> _column_can_null;
    };
} //namespace EA
