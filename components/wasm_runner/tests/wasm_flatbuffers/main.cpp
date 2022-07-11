#include <proxy_wasm_intrinsics.h>
#include <proxy_wasm_externs.h>
#include <flatbuffers/flexbuffers.h>

using namespace std;

class wasm_flatbuffers_root_context_t : public RootContext {
public:
    wasm_flatbuffers_root_context_t(uint32_t id, string_view root_id);

    auto onStart(size_t) -> bool override;
};


wasm_flatbuffers_root_context_t::wasm_flatbuffers_root_context_t(uint32_t id, string_view root_id)
    : RootContext(id, root_id) {}

auto wasm_flatbuffers_root_context_t::onStart(size_t) -> bool {
    std::string key = "document";
    const char *value = nullptr;
    size_t value_size = 0;
    uint32_t cas = 0;
    proxy_get_shared_data(key.c_str(), key.size(), &value, &value_size, &cas);
    if (value_size > 0) {
        auto map = flexbuffers::GetRoot(reinterpret_cast<const uint8_t*>(value), value_size).AsMap();
        auto document = "name: " + map["name"].AsString().str() + ", count: " + std::to_string(map["count"].AsInt32());
        LOG_INFO(document);
    }

    return true;
}

static RegisterContextFactory register_hello_world_root_context(nullptr, ROOT_FACTORY(wasm_flatbuffers_root_context_t), "m_plugin_id_1");
