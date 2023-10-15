#include "manager_disk.hpp"
#include <core/system_command.hpp>
#include <components/index/disk/route.hpp>
#include <services/collection/route.hpp>
#include "route.hpp"
#include "result.hpp"

namespace services::disk {

    using components::document::document_id_t;

    manager_disk_t::manager_disk_t(std::pmr::memory_resource* mr, actor_zeta::scheduler_raw scheduler, configuration::config_disk config, log_t& log)
        : actor_zeta::cooperative_supervisor<manager_disk_t>(mr)
        , core_sync_(actor_zeta::make_behavior(resource(), core::handler_id(core::route::sync), this, &manager_disk_t::sync))
        , create_agent_(actor_zeta::make_behavior(resource(), handler_id(route::create_agent), this, &manager_disk_t::create_agent))
        , load_(actor_zeta::make_behavior(resource(), handler_id(route::load), this, &manager_disk_t::load))
        , load_indexes_(actor_zeta::make_behavior(resource(), handler_id(route::load_indexes), this, &manager_disk_t::load_indexes))
        , append_database_(actor_zeta::make_behavior(resource(), handler_id(route::append_database), this, &manager_disk_t::append_database))
        , remove_database_(actor_zeta::make_behavior(resource(), handler_id(route::remove_database), this, &manager_disk_t::remove_database))
        , append_collection_(actor_zeta::make_behavior(resource(), handler_id(route::append_collection), this, &manager_disk_t::append_collection))
        , remove_collection_(actor_zeta::make_behavior(resource(), handler_id(route::remove_collection), this, &manager_disk_t::remove_collection))
        , write_documents_(actor_zeta::make_behavior(resource(), handler_id(route::write_documents), this, &manager_disk_t::write_documents))
        , remove_documents_(actor_zeta::make_behavior(resource(), handler_id(route::remove_documents), this, &manager_disk_t::remove_documents))
        , flush_(actor_zeta::make_behavior(resource(), handler_id(route::flush), this, &manager_disk_t::flush))
        , create_(actor_zeta::make_behavior(resource(), handler_id(index::route::create), this, &manager_disk_t::create_index_agent))
        , drop_(actor_zeta::make_behavior(resource(), handler_id(index::route::drop), this, &manager_disk_t::drop_index_agent))
        , success_(actor_zeta::make_behavior(resource(), handler_id(index::route::success), this, &manager_disk_t::drop_index_agent_success))
        , log_(log.clone())
        , config_(std::move(config))
        , metafile_indexes_(nullptr)
        , removed_indexes_(mr)
        , e_(scheduler) {
        trace(log_, "manager_disk start");
        if (!config_.path.empty()) {
            if (!std::filesystem::is_directory(config_.path / "indexes")) {
                std::filesystem::create_directories(config_.path / "indexes");
            }
            metafile_indexes_ = std::make_unique<core::file::file_t>(config_.path / "indexes/METADATA");
        }
        trace(log_, "manager_disk finish");
    }

    actor_zeta::behavior_t manager_disk_t::behavior() {
        return actor_zeta::make_behavior(
            resource(),
            [this](actor_zeta::message* msg) -> void {
                switch (msg->command()) {
                    case core::handler_id(core::route::sync): {
                        core_sync_(msg);
                        break;
                    }
                    case handler_id(route::create_agent): {
                        create_agent_(msg);
                        break;
                    }
                    case handler_id(route::load): {
                        load_(msg);
                        break;
                    }
                    case handler_id(route::load_indexes): {
                        load_indexes_(msg);
                        break;
                    }
                    case handler_id(route::append_database): {
                        append_database_(msg);
                        break;
                    }
                    case handler_id(route::remove_database): {
                        remove_database_(msg);
                        break;
                    }
                    case handler_id(route::append_collection): {
                        append_collection_(msg);
                        break;
                    }
                    case handler_id(route::remove_collection): {
                        remove_collection_(msg);
                        break;
                    }
                    case handler_id(route::write_documents): {
                        write_documents_(msg);
                        break;
                    }
                    case handler_id(route::remove_documents): {
                        remove_documents_(msg);
                        break;
                    }
                    case handler_id(route::flush): {
                        flush_(msg);
                        break;
                    }
                    case index::handler_id(index::route::create): {
                        create_(msg);
                        break;
                    }
                    case index::handler_id(index::route::drop): {
                        drop_(msg);
                        break;
                    }
                    case index::handler_id(index::route::success): {
                        success_(msg);
                        break;
                    }
                }
            });
    }

    auto manager_disk_t::enqueue_impl(actor_zeta::message_ptr msg, actor_zeta::execution_unit*) -> void {
        set_current_message(std::move(msg));
        behavior()(current_message());
    }

    auto manager_disk_t::make_scheduler() noexcept -> actor_zeta::scheduler_abstract_t*{
        return e_;
    }

    manager_disk_t::~manager_disk_t() {
        trace(log_, "delete manager_disk_t");
    }

    void manager_disk_t::create_agent() {
        auto name_agent = "agent_disk_" + std::to_string(agents_.size() + 1);
        trace(log_, "manager_disk create_agent : {}", name_agent);
        auto address = spawn_actor(
            [this](agent_disk_t* ptr) {
                agents_.emplace_back(agent_disk_ptr(ptr));
            },
            config_.path, log_);
    }

    auto manager_disk_t::load(session_id_t& session) -> void {
        trace(log_, "manager_disk_t::load , session : {}", session.data());
        actor_zeta::send(agent(), address(), handler_id(route::load), session, current_message()->sender());
    }

    auto manager_disk_t::load_indexes(session_id_t& session) -> void {
        trace(log_, "manager_disk_t::load_indexes , session : {}", session.data());
        load_session_ = session;
        load_indexes_private(session, current_message()->sender());
    }

    auto manager_disk_t::append_database(session_id_t& session, const database_name_t& database) -> void {
        trace(log_, "manager_disk_t::append_database , session : {} , database : {}", session.data(), database);
        command_append_database_t command{database};
        append_command(commands_, session, command_t(command));
    }

    auto manager_disk_t::remove_database(session_id_t& session, const database_name_t& database) -> void {
        trace(log_, "manager_disk_t::remove_database , session : {} , database : {}", session.data(), database);
        command_remove_database_t command{database};
        append_command(commands_, session, command_t(command));
    }

    auto manager_disk_t::append_collection(session_id_t& session, const database_name_t& database, const collection_name_t& collection) -> void {
        trace(log_, "manager_disk_t::append_collection , session : {} , database : {} , collection : {}", session.data(), database, collection);
        command_append_collection_t command{database, collection};
        append_command(commands_, session, command_t(command));
    }

    auto manager_disk_t::remove_collection(session_id_t& session, const database_name_t& database, const collection_name_t& collection) -> void {
        trace(log_, "manager_disk_t::remove_collection , session : {} , database : {} , collection : {}", session.data(), database, collection);
        command_remove_collection_t command{database, collection};
        append_command(commands_, session, command_t(command));
    }

    auto manager_disk_t::write_documents(session_id_t& session, const database_name_t& database, const collection_name_t& collection, const std::pmr::vector<document_ptr>& documents) -> void {
        trace(log_, "manager_disk_t::write_documents , session : {} , database : {} , collection : {}", session.data(), database, collection);
        command_write_documents_t command{database, collection, documents};
        append_command(commands_, session, command_t(command));
    }

    auto manager_disk_t::remove_documents(session_id_t& session, const database_name_t& database, const collection_name_t& collection, const std::pmr::vector<document_id_t>& documents) -> void {
        trace(log_, "manager_disk_t::remove_documents , session : {} , database : {} , collection : {}", session.data(), database, collection);
        command_remove_documents_t command{database, collection, documents};
        append_command(commands_, session, command_t(command));
    }

    auto manager_disk_t::flush(session_id_t& session, wal::id_t wal_id) -> void {
        trace(log_, "manager_disk_t::flush , session : {} , wal_id : {}", session.data(), wal_id);
        auto it = commands_.find(session);
        if (it != commands_.end()) {
            for (const auto& command : commands_.at(session)) {
                if (command.name() == handler_id(route::remove_collection)) {
                    const auto& drop_collection = command.get<command_remove_collection_t>();
                    std::vector<index_agent_disk_t*> indexes;
                    for (const auto& index : index_agents_) {
                        if (index.second->collection_name() == drop_collection.collection) {
                            indexes.push_back(index.second.get());
                        }
                    }
                    if (indexes.empty()) {
                        actor_zeta::send(agent(), address(), command.name(), command);
                        actor_zeta::send(current_message()->sender(), address(), handler_id(route::remove_collection_finish), session, drop_collection.collection);
                    } else {
                        removed_indexes_.emplace(session, removed_index_t{indexes.size(), command, current_message()->sender()});
                        for (auto *index : indexes) {
                            actor_zeta::send(index->address(), address(), index::handler_id(index::route::drop), session);
                        }
                    }
                } else {
                    actor_zeta::send(agent(), address(), command.name(), command);
                }
            }
            commands_.erase(session);
        }
        actor_zeta::send(agent(), address(), handler_id(route::fix_wal_id), wal_id);
    }

    void manager_disk_t::create_index_agent(session_id_t& session, const components::ql::create_index_t &index) {
        auto name = index.name();
        if (index_agents_.contains(name) && !index_agents_.at(name)->is_dropped()) {
            error(log_, "manager_disk: index {} already exists", name);
            actor_zeta::send(current_message()->sender(), address(), index::handler_id(index::route::error), session);
        } else {
            trace(log_, "manager_disk: create_index_agent : {}", name);
            index_agents_.erase(name);
            auto address_agent = spawn_actor(
                [&](index_agent_disk_t* ptr) {
                    index_agents_.insert_or_assign(name, index_agent_disk_ptr(ptr));
                },
                config_.path, index.collection_, name, index.index_compare_, log_);
            if (session.data() != load_session_.data()) {
                write_index_private(index);
            }
            actor_zeta::send(current_message()->sender(), address(), index::handler_id(index::route::success_create), session, name, address_agent);
        }
    }

    void manager_disk_t::drop_index_agent(session_id_t& session, const index_name_t &index_name) {
        if (index_agents_.contains(index_name)) {
            trace(log_, "manager_disk: drop_index_agent : {}", index_name);
            command_drop_index_t command{index_name, current_message()->sender()};
            append_command(commands_, session, command_t(command));
            actor_zeta::send(index_agents_.at(index_name)->address(), address(), index::handler_id(index::route::drop), session);
            remove_index_private(index_name);
        } else {
            error(log_, "manager_disk: index {} not exists", index_name);
            //actor_zeta::send(current_message()->sender(), address(), index::handler_id(index::route::error), session);
        }
    }

    void manager_disk_t::drop_index_agent_success(session_id_t& session) {
        auto it = commands_.find(session);
        if (it != commands_.end()) {
            for (const auto& command : commands_.at(session)) {
                auto command_drop = command.get<command_drop_index_t>();
                trace(log_, "manager_disk: drop_index_agent : {} : success", command_drop.index_name);
                //actor_zeta::send(command_drop.address, address(), index::handler_id(index::route::success), session);
            }
            commands_.erase(session);
        } else {
            auto it_all_drop = removed_indexes_.find(session);
            if (it_all_drop != removed_indexes_.end()) {
                if (--it_all_drop->second.size == 0) {
                    actor_zeta::send(agent(), address(), it_all_drop->second.command.name(), it_all_drop->second.command);
                    const auto& drop_collection = it_all_drop->second.command.get<command_remove_collection_t>();
                    remove_all_indexes_from_collection_private(drop_collection.collection);
                    actor_zeta::send(it_all_drop->second.sender, address(), handler_id(route::remove_collection_finish), session, drop_collection.collection);
                }
            }
        }
    }

    auto manager_disk_t::agent() -> actor_zeta::address_t {
        return agents_[0]->address();
    }

    void manager_disk_t::write_index_private(const components::ql::create_index_t &index) {
        if (metafile_indexes_) {
            msgpack::sbuffer buf;
            msgpack::pack(buf, index);
            auto size = buf.size();
            metafile_indexes_->append(reinterpret_cast<void*>(&size), sizeof(size));
            metafile_indexes_->append(buf.data(), buf.size());
        }
    }

    void manager_disk_t::load_indexes_private(session_id_t& session, const actor_zeta::address_t& dispatcher) {
        auto indexes = read_indexes_private();
        metafile_indexes_->seek_eof();
        for (const auto &index : indexes) {
            actor_zeta::send(dispatcher, address(), collection::handler_id(collection::route::create_index), session, index, dispatcher);
        }
    }

    std::vector<components::ql::create_index_t> manager_disk_t::read_indexes_private(const collection_name_t& collection_name) const {
        std::vector<components::ql::create_index_t> res;
        if (metafile_indexes_) {
            constexpr auto count_byte_by_size = sizeof(size_t);
            size_t size;
            __off64_t offset = 0;
            while (true) {
                auto size_str = metafile_indexes_->read(count_byte_by_size, offset);
                if (size_str.size() == count_byte_by_size) {
                    offset += count_byte_by_size;
                    std::memcpy(&size, size_str.data(), count_byte_by_size);
                    auto buf = metafile_indexes_->read(size, offset);
                    offset += __off64_t(size);
                    msgpack::unpacked msg;
                    msgpack::unpack(msg, buf.data(), buf.size());
                    auto index = msg.get().as<components::ql::create_index_t>();
                    if (collection_name.empty() || index.collection_ == collection_name) {
                        res.push_back(index);
                    }
                } else {
                    break;
                }
            }
        }
        return res;
    }

    std::vector<components::ql::create_index_t> manager_disk_t::read_indexes_private() const {
        return read_indexes_private("");
    }

    void manager_disk_t::remove_index_private(const index_name_t& index_name) {
        if (metafile_indexes_) {
            auto indexes = read_indexes_private();
            indexes.erase(std::remove_if(indexes.begin(), indexes.end(), [&index_name](const components::ql::create_index_t& index) {
                              return index.name() == index_name;
                          }),
                          indexes.end());
            metafile_indexes_->clear();
            for (const auto &index : indexes) {
                write_index_private(index);
            }
        }
    }

    void manager_disk_t::remove_all_indexes_from_collection_private(const collection_name_t& collection_name) {
        if (metafile_indexes_) {
            auto indexes = read_indexes_private();
            indexes.erase(std::remove_if(indexes.begin(), indexes.end(), [&collection_name](const components::ql::create_index_t& index) {
                              return index.collection_ == collection_name;
                          }),
                          indexes.end());
            metafile_indexes_->clear();
            for (const auto &index : indexes) {
                write_index_private(index);
            }
        }
    }


    manager_disk_empty_t::manager_disk_empty_t(std::pmr::memory_resource* mr, actor_zeta::scheduler_raw scheduler)
        : actor_zeta::cooperative_supervisor<manager_disk_empty_t>(mr)
        , e_(scheduler){}

    auto manager_disk_empty_t::load(session_id_t& session) -> void {
        auto result = result_load_t::empty();
        actor_zeta::send(current_message()->sender(), address(), handler_id(route::load_finish), session, result);
    }

    void manager_disk_empty_t::create_index_agent(session_id_t& session, const collection_name_t&, const index_name_t&, components::ql::index_compare) {
        actor_zeta::send(current_message()->sender(), address(), handler_id(index::route::success_create), session, actor_zeta::address_t::empty_address());
    }

    auto manager_disk_empty_t::make_scheduler() noexcept -> actor_zeta::scheduler_abstract_t*{
        return e_;
    }

    auto manager_disk_empty_t::make_type() const noexcept -> const char* const {
        return "manager_disk";
    }

    actor_zeta::behavior_t manager_disk_empty_t::behavior() {
        return actor_zeta::make_behavior(
            resource(),
            [this](actor_zeta::message* msg) -> void {
                switch (msg->command()) {
                }
            });
    }

    auto manager_disk_empty_t::enqueue_impl(actor_zeta::message_ptr msg, actor_zeta::execution_unit*) -> void {
        set_current_message(std::move(msg));
        behavior()(current_message());
    }

} //namespace services::disk