#include "ottergon.hpp"

namespace {
    static const auto system_database = "system_database";
    static const auto system_collection = "system_collection";

    auto base_make_ottergon(configuration::config cfg) -> ottergon::ottergon_ptr {
        auto ptr = std::make_unique<ottergon::ottergon_t>(configuration::config::default_config());
        auto id = ottergon::session_id_t();
        static_cast<ottergon::base_ottergon_t*>(ptr.get())->dispatcher()->create_database(id, system_database);
        auto id_1 = ottergon::session_id_t();
        static_cast<ottergon::base_ottergon_t*>(ptr.get())->dispatcher()->create_collection(id_1, system_database, system_collection);
        return ptr;
    }

    auto base_execute_sql(ottergon::base_ottergon_t* ptr, const std::string& query) -> components::result::result_t {
        assert(ptr != nullptr);
        assert(!query.empty());
        auto session = ottergon::session_id_t();
        return ptr->dispatcher()->execute_sql(session, query);
    }

} // namespace

namespace ottergon {

    auto make_ottergon() -> ottergon_ptr { return base_make_ottergon(configuration::config::default_config()); }

    auto make_ottergon(configuration::config cfg) -> ottergon_ptr { return base_make_ottergon(cfg); }

    auto execute_sql(const ottergon_ptr& ptr, const std::string& query) -> components::result::result_t {
        return base_execute_sql(ptr.get(), query);
    }
} // namespace ottergon