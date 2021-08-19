#include "dispatcher.hpp"
#include "apps/medusa_cascade/protocol/dto.hpp"
#include "protocol.hpp"
#include "tracy/tracy.hpp"
#include <msgpack.hpp>

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
        e_->stop();
    }

    auto manager_dispatcher_t::executor() noexcept -> goblin_engineer::abstract_executor* {
        return e_.get();
    }
    auto manager_dispatcher_t::get_executor() noexcept -> goblin_engineer::abstract_executor* {
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
        auto o = msgpack::unpack(zone_, req.data(), size);

        session_t session;
        session.id_ = id;
        session.uid_ = string_generator_(o.convert_if_not_nil());

        ///msgpack::object obj(my, z);
        switch (static_cast<protocol_op>(proto.op)) {
            case protocol_op::create_collection:
                break;
            case protocol_op::create_database:
                break;
            case protocol_op::select: {
                msgpack::object_handle oh = msgpack::unpack(proto.body.data(), proto.body.size());
                msgpack::object obj = oh.get();
                auto query = std::move(obj.as<select_t>());
                return goblin_engineer::send(self(), self(), "select", std::move(session), std::move(query));
            }

            case protocol_op::insert: {
                msgpack::object_handle oh = msgpack::unpack(proto.body.data(), proto.body.size());
                msgpack::object obj = oh.get();
                auto query = std::move(obj.as<insert_t>());
                return goblin_engineer::send(self(), self(), "insert", std::move(session), std::move(query));
            }
            case protocol_op::erase: {
                msgpack::object_handle oh = msgpack::unpack(proto.body.data(), proto.body.size());
                msgpack::object obj = oh.get();
                auto query = std::move(obj.as<erase_t>());
                return goblin_engineer::send(self(), self(), "erase", std::move(session), std::move(query));
            }
        }
    }
    void dispatcher_t::erase(session_t session, const erase_t& value) {
        goblin_engineer::send(addresses("collection"), self(), "erase", std::move(session), std::move(value));
    }

    void dispatcher_t::insert(session_t session, const insert_t& value) {
        goblin_engineer::send(addresses("collection"), self(), "insert", std::move(session), std::move(value));
    }

    void dispatcher_t::select(session_t session, const select_t& value) {
        goblin_engineer::send(addresses("collection"), self(), "select", std::move(session), std::move(value));
    }

} // namespace kv