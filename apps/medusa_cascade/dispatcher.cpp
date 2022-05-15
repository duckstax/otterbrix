#include "dispatcher.hpp"
#include "components/protocol/protocol.hpp"
#include "protocol/insert.hpp"
#include "protocol/request_select.hpp"
#include "tracy/tracy.hpp"
#include <msgpack.hpp>
#include <services/storage/route.hpp>

namespace kv {

    manager_dispatcher_t::manager_dispatcher_t(log_t& log, size_t num_workers, size_t max_throughput)
        : goblin_engineer::abstract_manager_service("manager_dispatcher")
        , log_(log.clone())
        , e_(new goblin_engineer::shared_work(num_workers, max_throughput), goblin_engineer::detail::thread_pool_deleter()) {
        ZoneScoped;
        e_->start();
    }

    manager_dispatcher_t::~manager_dispatcher_t() {
        ZoneScoped;
    }

    auto manager_dispatcher_t::executor() noexcept -> actor_zeta::scheduler_abstract_t* {
        return e_.get();
    }

    //NOTE: behold thread-safety!
    auto manager_dispatcher_t::enqueue_base(goblin_engineer::message_ptr msg, actor_zeta::execution_device*) -> void {
        ZoneScoped;
        set_current_message(std::move(msg));
        execute(*this);
    }

    dispatcher_t::dispatcher_t(manager_dispatcher_ptr manager_database, log_t& log)
        : goblin_engineer::abstract_service(manager_database, "dispatcher")
        , log_(log.clone()) {
        add_handler(dispatcher::create_collection, &dispatcher_t::create_collection);
        add_handler(dispatcher::create_database, &dispatcher_t::create_database);
        add_handler(dispatcher::select, &dispatcher_t::select);
        add_handler(dispatcher::insert, &dispatcher_t::insert);
        add_handler(dispatcher::erase, &dispatcher_t::erase);
        add_handler(dispatcher::ws_dispatch, &dispatcher_t::ws_dispatch);
    }

    void dispatcher_t::ws_dispatch(session_id id, std::string& request, size_t size) {
        auto req = std::move(request);
        auto obj = msgpack::unpack(zone_, req.data(), size);
        auto protocol = std::move(obj.as<protocol_t>());
        session_t session;
        session.id_ = id;
        session.uid_ = string_generator_(protocol.uid_);

        switch (protocol.op_type) {
            case protocol_op::create_collection:
                break;
            case protocol_op::create_database:
                break;
            case protocol_op::select: {
                return actor_zeta::send(self(), self(), "select", std::move(session), std::move(std::get<select_t>(protocol.data_)));
            }

            case protocol_op::insert: {
                return actor_zeta::send(self(), self(), "insert", std::move(session), std::move(std::get<insert_t>(protocol.data_)));
            }
            case protocol_op::erase: {
                return actor_zeta::send(self(), self(), "erase", std::move(session), std::move(std::get<erase_t>(protocol.data_)));
            }
        }
    }
    void dispatcher_t::erase(session_t session, const erase_t& value) {
        actor_zeta::send(addresses("collection"), self(), "erase", std::move(session), std::move(value));
    }

    void dispatcher_t::insert(session_t session, const insert_t& value) {
        actor_zeta::send(addresses("collection"), self(), "insert", std::move(session), std::move(value));
    }

    void dispatcher_t::select(session_t session, const select_t& value) {
        actor_zeta::send(addresses("collection"), self(), "select", std::move(session), std::move(value));
    }

} // namespace kv