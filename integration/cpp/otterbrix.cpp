#include "otterbrix.hpp"

namespace {
    static const auto system_database = "system_database";
    static const auto system_collection = "system_collection";

    auto base_make_otterbrix(configuration::config cfg = configuration::config::default_config())
        -> otterbrix::otterbrix_ptr {
        auto* ptr = new otterbrix::otterbrix_t(cfg);
        auto id = otterbrix::session_id_t();
        ptr->dispatcher()->create_database(id, system_database);
        auto id_1 = otterbrix::session_id_t();
        ptr->dispatcher()->create_collection(id_1, system_database, system_collection);
        return ptr;
    }

    auto base_execute_sql(otterbrix::base_otterbrix_t* ptr, const std::string& query)
        -> components::cursor::cursor_t_ptr {
        assert(ptr != nullptr);
        assert(!query.empty());
        auto session = otterbrix::session_id_t();
        return ptr->dispatcher()->execute_sql(session, query);
    }

} // namespace

namespace otterbrix {

    auto make_otterbrix() -> otterbrix_ptr { return base_make_otterbrix(configuration::config::default_config()); }

    auto make_otterbrix(configuration::config cfg) -> otterbrix_ptr { return base_make_otterbrix(std::move(cfg)); }

    auto execute_sql(const otterbrix_ptr& ptr, const std::string& query) -> components::cursor::cursor_t_ptr {
        return base_execute_sql(ptr.get(), query);
    }
} // namespace otterbrix