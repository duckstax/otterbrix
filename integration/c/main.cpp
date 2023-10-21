#include "ottergon.h"

#include <integration/cpp/base_spaces.hpp>

namespace {
    configuration::config create_config() {
        return configuration::config::default_config();
    }

    struct spaces_t final : public duck_charmer::base_spaces {
    public:
        spaces_t(spaces_t& other) = delete;
        void operator=(const spaces_t&) = delete;
        spaces_t(const configuration::config& config):base_spaces(config) {}
    };
}

struct pod_space_t {
    state_t state;
    std::unique_ptr<spaces_t> space;
    duck_charmer::wrapper_dispatcher_t* dispatcher;
};

ottergon_ptr create(const config_t* cfg){
    assert(cfg != nullptr);
    auto config = create_config();
    auto pod_space = std::make_unique<pod_space_t>();
    pod_space->space = std::make_unique<spaces_t>(config);
    pod_space->dispatcher = pod_space->space->dispatcher();
    pod_space->state = state_t::created;
    return reinterpret_cast<void*>(pod_space.release());;
}

void destroy(ottergon_ptr ptr){
    assert(ptr != nullptr);
    auto* spaces = reinterpret_cast<pod_space_t*>(ptr);
    assert(spaces->state == state_t::created);
    spaces->dispatcher = nullptr;
    spaces->space.reset();
    spaces->state = state_t::destroyed;
    delete spaces;

}

struct result_set_t* execute_sql(ottergon_ptr ptr, string_view_t query_raw){
    assert(ptr != nullptr);
    auto pod_space = reinterpret_cast<pod_space_t*>(ptr);
    assert(pod_space->state == state_t::created);
    assert(query_raw.data != nullptr);
    auto session = duck_charmer::session_id_t();
    std::string query(query_raw.data, query_raw.size);
    auto res = pod_space->dispatcher->execute_sql(session, query);
    ///auto r = res.get<result_insert>();
}