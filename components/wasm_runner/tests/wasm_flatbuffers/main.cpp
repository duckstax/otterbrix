#include <proxy_wasm_intrinsics.h>
#include <proxy_wasm_externs.h>

using namespace std;

class wasm_flatbuffers_root_context_t : public RootContext {
public:
    wasm_flatbuffers_root_context_t(uint32_t id, string_view root_id);

    auto onStart(size_t) -> bool override;
};


wasm_flatbuffers_root_context_t::wasm_flatbuffers_root_context_t(uint32_t id, string_view root_id)
    : RootContext(id, root_id) {}

auto wasm_flatbuffers_root_context_t::onStart(size_t) -> bool {
    std::string blob;
    auto to_char = [](uint8_t value) {
        if (value < 10) {
            return char('0' + value);
        }
        return char('a' + value - 10);
    };

    std::string key = "document";
    const char *value = nullptr;
    size_t value_size = 0;
    uint32_t cas = 0;
    proxy_get_shared_data(key.c_str(), key.size(), &value, &value_size, &cas);
    for (size_t i = 0; i < value_size; ++i) {
        uint8_t byte = static_cast<uint8_t>(value[i]);
        blob.push_back('x');
        blob.push_back(to_char(byte / 16));
        blob.push_back(to_char(byte % 16));
        blob.push_back(' ');
    }
    LOG_INFO(blob);

    return true;
}

static RegisterContextFactory register_hello_world_root_context(nullptr, ROOT_FACTORY(wasm_flatbuffers_root_context_t), "m_plugin_id_1");
