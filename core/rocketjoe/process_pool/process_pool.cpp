#include <rocketjoe/services/process_pool/process_pool.hpp>
#include <iostream>

namespace rocketjoe { namespace services {

        process_pool_t::process_pool_t(goblin_engineer::root_manager * ptr, goblin_engineer::dynamic_config &cfg)
        : abstract_service(ptr, "process_pool_t")
        , worker_counter_(0){
            add_handler(
                "add_worker",
                [this](actor_zeta::context&ctx){
                    add_worker_process();
                }
            );

        }

        void process_pool_t::add_worker_process() {
            boost::process::spawn("debug_worker","-f", g_);
        }

        process_pool_t::~process_pool_t() {
            g_.wait();
        }

        void process_pool_t::created_workers(std::size_t size) {
            for(std::size_t i = 0 ; i < size;++i ) {
                actor_zeta::send(actor_zeta::actor_address(this),actor_zeta::actor_address(this),"add_worker",int(0));
            }

        }


    }}
