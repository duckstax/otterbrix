#include <rocketjoe/services/process_pool/process_pool.hpp>
#include <iostream>

namespace rocketjoe { namespace services {

        process_pool_t::process_pool_t(goblin_engineer::root_manager * ptr, goblin_engineer::dynamic_config &): abstract_service(ptr, "process_pool_t") {

        }

        void process_pool_t::add_worker_process() {
            auto path = boost::filesystem::path("/home/kotbegemot/CLionProjects/RocketJoe/cmake-build-debug/apps/debug_worker");
            boost::process::spawn(path, g_);
        }

        process_pool_t::~process_pool_t() {
            g_.wait();
        }

    }}
