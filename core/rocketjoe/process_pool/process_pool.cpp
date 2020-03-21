#include <rocketjoe/process_pool/process_pool.hpp>
#include <iostream>

namespace rocketjoe {

        void process_pool_t::add_worker_process() {
            boost::process::spawn("debug_worker","-f", g_);
            log_t_.info("created worker process");
        }

        process_pool_t::~process_pool_t() {
            g_.wait();
            log_t_.info("destroy process pool");
        }

        void process_pool_t::add_worker_process(std::size_t size) {
            for(std::size_t i = 0 ; i < size;++i ) {
              add_worker_process();
            }
        }

        process_pool_t::process_pool_t(log_t log)
            : worker_counter_(0)
            , log_t_(std::move(log)) {

            log_t_.info("construct process pool");
        }

}
