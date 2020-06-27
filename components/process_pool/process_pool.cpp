#include <components/process_pool/process_pool.hpp>
#include <iostream>

namespace components {

    void process_pool_t::add_worker_process() {
        boost::process::spawn(executable_file_, boost::process::args(args_), g_);
        log_t_.info("created worker process");
    }

    process_pool_t::~process_pool_t() {
        std::error_code ec;
        g_.wait(ec);
        if (ec) {
            log_t_.error(fmt::format("Error Process Pool {0}", ec.message()));
        }
        log_t_.info("Destroy process pool");
    }

    void process_pool_t::add_worker_process(std::size_t size) {
        for (std::size_t i = 0; i < size; ++i) {
            add_worker_process();
        }
    }

    process_pool_t::process_pool_t(const std::string& executable_file, const std::vector<std::string>& args, log_t log)
        : executable_file_(executable_file)
        , args_(args)
        , worker_counter_(0)
        , log_t_(std::move(log)) {
        log_t_.info("construct process pool");
    }

} // namespace components
