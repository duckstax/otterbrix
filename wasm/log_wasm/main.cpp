#include <proxy_wasm_intrinsics.h>

using namespace std;

class hello_world_root_context_t : public RootContext {
public:
    hello_world_root_context_t(uint32_t id, string_view root_id);

    auto onStart(size_t) -> bool override;
};


hello_world_root_context_t::hello_world_root_context_t(uint32_t id, string_view root_id)
    : RootContext(id, root_id) {}

auto hello_world_root_context_t::onStart(size_t) -> bool {
    LOG_INFO("Hello, World!");

    return true;
}

static RegisterContextFactory register_hello_world_root_context(nullptr, ROOT_FACTORY(hello_world_root_context_t), "plugin_id0");
