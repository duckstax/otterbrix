#pragma once

#include <boost/process.hpp>
#include <goblin-engineer.hpp>

namespace rocketjoe { namespace services {

        class process_pool_t final : goblin_engineer::abstract_service {
        public:
            process_pool_t(goblin_engineer::root_manager *, goblin_engineer::dynamic_config &);

            ~process_pool_t() override;

            void created_workers(std::size_t);

            void add_worker_process();

        private:
            std::uint64_t worker_counter_;
            boost::process::group g_;
        };
}}