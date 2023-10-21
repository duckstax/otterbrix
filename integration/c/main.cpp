#include "ottergon.h"

#include <integration/cpp/base_spaces.hpp>

namespace {
    using result_type_t = components::result::result_type_t;

    configuration::config create_config() { return configuration::config::default_config(); }

    struct spaces_t final : public duck_charmer::base_spaces {
    public:
        spaces_t(spaces_t& other) = delete;
        void operator=(const spaces_t&) = delete;
        spaces_t(const configuration::config& config)
            : base_spaces(config) {}
    };
} // namespace

struct pod_space_t {
    state_t state;
    std::unique_ptr<spaces_t> space;
    duck_charmer::wrapper_dispatcher_t* dispatcher;
};

extern "C" ottergon_ptr create(const config_t* cfg) {
    assert(cfg != nullptr);
    auto config = create_config();
    auto pod_space = std::make_unique<pod_space_t>();
    pod_space->space = std::make_unique<spaces_t>(config);
    pod_space->dispatcher = pod_space->space->dispatcher();
    pod_space->state = state_t::created;
    return reinterpret_cast<void*>(pod_space.release());
    ;
}

extern "C" void destroy(ottergon_ptr ptr) {
    assert(ptr != nullptr);
    auto* spaces = reinterpret_cast<pod_space_t*>(ptr);
    assert(spaces->state == state_t::created);
    spaces->dispatcher = nullptr;
    spaces->space.reset();
    spaces->state = state_t::destroyed;
    delete spaces;
}

extern "C" result_set_t* execute_sql(ottergon_ptr ptr, string_view_t query_raw) {
    assert(ptr != nullptr);
    auto pod_space = reinterpret_cast<pod_space_t*>(ptr);
    assert(pod_space->state == state_t::created);
    assert(query_raw.data != nullptr);
    auto session = duck_charmer::session_id_t();
    std::string query(query_raw.data, query_raw.size);
    auto res = pod_space->dispatcher->execute_sql(session, query);
    if (res.is_success()) {
        switch (res.result_type()) {
            case result_type_t::empty: {
                return nullptr;
            }
            case result_type_t::result_address: {
                auto r = res.get<components::result::result_address_t>();
                return nullptr;
            }
            case result_type_t::result_list_addresses: {
                auto r = res.get<components::result::result_list_addresses_t>();
                return nullptr;
            }
            case result_type_t::result: {
                auto r = res.get<components::cursor::cursor_t*>();
                return nullptr;
            }
            case result_type_t::result_insert: {
                auto r = res.get<components::result::result_insert>();
                return nullptr;
            }
            case result_type_t::result_delete: {
                auto r = res.get<components::result::result_delete>();
                return nullptr;
            }
            case result_type_t::result_update: {
                auto r = res.get<components::result::result_update>();
                return nullptr;
            }
            default: {
                assert(false);
            }
        }
    } else {
        assert(false);
    }
}