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
#include "elasticann/client/namespace_cmd.h"
#include <iostream>
#include <memory>

namespace EA::schema {
    /// Set up a subcommand and capture a shared_ptr to a struct that holds all its options.
    /// The variables of the struct are bound to the CLI options.
    /// We use a shared ptr so that the addresses of the variables remain for binding,
    /// You could return the shared pointer if you wanted to access the values in main.
    void setup_namespace_cmd(turbo::App &app) {
        // Create the option and subcommand objects.
        auto opt = std::make_shared<NamespaceCmdAOptions>();
        auto *ns = app.add_subcommand("namespace", "namespace operations");
        ns->add_option("-n,--name", opt->name, "namespace name")->required();
        ns->callback([ns]() { run_namespace_cmd(ns); });
        // Add options to sub, binding them to opt.
        //ns->require_subcommand();
        // add sub cmd
        auto cns = ns->add_subcommand("create", " create namespace");
        cns->callback([opt]() { run_ns_create_cmd(*opt); });
        auto rns = ns->add_subcommand("remove", " remove namespace");
        rns->callback([opt]() { run_ns_remove_cmd(*opt); });
        auto mns = ns->add_subcommand("modify", " modify namespace");
        mns->add_option("-q, --quota", opt->quota, "new namespace quota")->required();
        mns->callback([opt]() { run_ns_modify_cmd(*opt); });

    }

    /// The function that runs our code.
    /// This could also simply be in the callback lambda itself,
    /// but having a separate function is cleaner.
    void run_namespace_cmd(turbo::App* app) {
        // Do stuff...
        if(app->get_subcommands().empty()) {
            std::cout<<app->help()<<std::endl;
        }
    }

    void run_ns_create_cmd(const NamespaceCmdAOptions &option) {
        std::cout<<"create namespace: "<< option.name<<std::endl;
    }
    void run_ns_remove_cmd(const NamespaceCmdAOptions &option) {
        std::cout<<"remove namespace: "<< option.name<<std::endl;
    }
    void run_ns_modify_cmd(const NamespaceCmdAOptions &option) {
        std::cout<<"modify namespace: "<< option.name<<std::endl;
    }

}  // namespace EA::schema
