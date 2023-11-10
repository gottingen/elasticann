//
// Created by jeff on 23-7-6.
//

#include "elasticann/client/table_cmd.h"
#include "elasticann/client/cluster_cmd.h"
#include "elasticann/client/option_context.h"
#include "elasticann/common/tlog.h"
#include "elasticann/client/router_interact.h"
#include "eaproto/router/router.interface.pb.h"
#include "elasticann/client/proto_builder.h"
#include "turbo/format/print.h"
#include "elasticann/client/show_help.h"

namespace EA::client {
    void setup_table_cmd(turbo::App &app) {
        // Create the option and subcommand objects.
        auto opt = OptionContext::get_instance();
        auto *tb = app.add_subcommand("table", "table operations");
        tb->callback([tb]() { run_table_cmd(tb); });

        auto *ctb = tb->add_subcommand("create", "table create operations");
        ctb->add_option("-n, namespace", OptionContext::get_instance()->namespace_name, "table belong to namespace")->required(true);
        ctb->add_option("-d, database", OptionContext::get_instance()->db_name, "table belong to database")->required(true);
        ctb->add_option("-t, table", OptionContext::get_instance()->table_name, "table name")->required(true);
        ctb->add_option("-f, field", OptionContext::get_instance()->table_fields, "fields name and type, format: field_name:field_type")->required(true);
        ctb->add_option("-i, index", OptionContext::get_instance()->table_indexes, "indexes, format: index_name:index_type:field1,field2...fieldn");
    }

    void run_table_cmd(turbo::App *app) {
        if(app->get_subcommands().empty()) {
            turbo::Println("{}", app->help());
        }
    }

    void run_table_create() {

    }
}