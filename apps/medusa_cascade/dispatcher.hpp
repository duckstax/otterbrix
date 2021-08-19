#pragma once
#include "forward.hpp"
#include "network_service/network_service.hpp"
#include "protocol.hpp"
#include "route.hpp"
#include <goblin-engineer/core.hpp>
#include <log/log.hpp>
#include <boost/uuid/uuid_generators.hpp>

namespace kv {
    class manager_dispatcher_t final : public goblin_engineer::abstract_manager_service {
    public:
        manager_dispatcher_t(log_t& log, size_t num_workers, size_t max_throughput);
        ~manager_dispatcher_t() override;

        auto executor() noexcept -> goblin_engineer::abstract_executor* final override;
        auto get_executor() noexcept -> goblin_engineer::abstract_executor* final override;
        auto enqueue_base(goblin_engineer::message_ptr msg, actor_zeta::execution_device*) -> void override;

    private:
        log_t log_;
        goblin_engineer::executor_ptr e_;
    };

    ///using manager_dispatcher_ptr = goblin_engineer::intrusive_ptr<manager_dispatcher_t>;
    using manager_dispatcher_ptr = goblin_engineer::intrusive_ptr<network_service_t>;

    class dispatcher_t final : public goblin_engineer::abstract_service {
    public:
        dispatcher_t(manager_dispatcher_ptr manager_database, log_t& log);

        void ws_dispatch(session_id id, std::string& request, size_t size);

        void create_database() {
        }

        void create_collection() {
        }

        void select(session_t session, const select_t& value);

        void insert(session_t session, const insert_t& value);

        void erase(session_t session, const erase_t& value);

    private:
        log_t log_;
        boost::uuids::string_generator string_generator_;
        msgpack::zone zone_;
    };
} // namespace kv