#include <filesystem>
#include <fstream>
#include <sstream>

#include <catch2/catch.hpp>

#include <components/log/log.hpp>

#include "wasm.hpp"

using namespace std;
using namespace proxy_wasm;
using namespace components::wasm_runner;

string read_test_wasm_file(const filesystem::path& base_path) {
    auto path = "tests" / base_path;

    ifstream file(path, ios::binary);
    stringstream file_string_stream;

    file_string_stream << file.rdbuf();

    return file_string_stream.str();
}

class mock_wasm_context_t : public wasm_context_t {
public:
    mock_wasm_context_t(WasmBase* wasm);

    mock_wasm_context_t(WasmBase* wasm, const shared_ptr<PluginBase>& plugin);

    auto log(uint32_t log_level, string_view message) -> WasmResult override;

    auto getLogLevel() -> uint32_t override;
};

class mock_wasm_t : public wasm_t {
public:
    mock_wasm_t(unique_ptr<WasmVm> vm, string_view vm_id,
                string_view vm_configuration, string_view vm_key,
                unordered_map<string, string> envs,
                AllowedCapabilitiesMap allowed_capabilities);

    mock_wasm_t(const shared_ptr<WasmHandleBase>& wasm, const WasmVmFactory& factory);

    auto createVmContext() -> ContextBase* override;

    auto createRootContext(const shared_ptr<PluginBase>& plugin) -> ContextBase* override;

    auto createContext(const shared_ptr<PluginBase>& plugin) -> ContextBase* override;
};

class mock_wasm_manager_t : public wasm_manager_t {
public:
    mock_wasm_manager_t(engine_t engine);

protected:
    auto create_wasm(string_view vm_id, string_view vm_configuration,
                     string_view vm_key, unordered_map<string, string> envs,
                     AllowedCapabilitiesMap allowed_capabilities) const -> unique_ptr<WasmBase> override;

    auto clone_wasm(shared_ptr<WasmHandleBase> wasm) const -> unique_ptr<WasmBase> override;
};

mock_wasm_context_t::mock_wasm_context_t(WasmBase* wasm) : wasm_context_t(wasm) {}

mock_wasm_context_t::mock_wasm_context_t(WasmBase* wasm, const shared_ptr<PluginBase>& plugin)
    : wasm_context_t(wasm, plugin) {}

WasmResult mock_wasm_context_t::log(uint32_t log_level, string_view message) {
    REQUIRE(static_cast<log_t::level>(log_level) == log_t::level::info);
    REQUIRE(message == "Hello, World!");

    return wasm_context_t::log(log_level, message);
}

auto mock_wasm_context_t::getLogLevel() -> uint32_t {
    auto log_level = wasm_context_t::getLogLevel();

    REQUIRE(static_cast<log_t::level>(log_level) == log_t::level::info);
    REQUIRE(static_cast<LogLevel>(log_level) == LogLevel::info);

    return log_level;
}

mock_wasm_t::mock_wasm_t(unique_ptr<WasmVm> vm, string_view vm_id,
                         string_view vm_configuration, string_view vm_key,
                         unordered_map<string, string> envs, AllowedCapabilitiesMap allowed_capabilities)
    : wasm_t(move(vm), vm_id, vm_configuration, vm_key, move(envs), move(allowed_capabilities)) {}

mock_wasm_t::mock_wasm_t(const shared_ptr<WasmHandleBase>& wasm, const WasmVmFactory& factory)
    : wasm_t(wasm, factory) {}

auto mock_wasm_t::createVmContext() -> ContextBase* {
    return new mock_wasm_context_t(this);
}

auto mock_wasm_t::createRootContext(const shared_ptr<PluginBase>& plugin) -> ContextBase* {
    return new mock_wasm_context_t(this, plugin);
}

auto mock_wasm_t::createContext(const shared_ptr<PluginBase>& plugin) -> ContextBase* {
    return new mock_wasm_context_t(this, plugin);
}

mock_wasm_manager_t::mock_wasm_manager_t(engine_t engine) : wasm_manager_t(engine) {}

auto mock_wasm_manager_t::create_wasm(string_view vm_id, string_view vm_configuration,
                                      string_view vm_key, unordered_map<string, string> envs,
                                      AllowedCapabilitiesMap allowed_capabilities) const -> unique_ptr<WasmBase> {
    return make_unique<mock_wasm_t>(new_vm(), vm_id, vm_configuration, vm_key, move(envs), move(allowed_capabilities));
}

auto mock_wasm_manager_t::clone_wasm(shared_ptr<WasmHandleBase> wasm) const -> unique_ptr<WasmBase> {
    return make_unique<mock_wasm_t>(move(wasm), [this]() {
        return new_vm();
    });
}

TEST_CASE("wasm_t", "[API]") {
    string log_dir(".");
    auto log = initialization_logger("wasm_runner", log_dir);
    auto wasm = read_test_wasm_file("log_wasm.wasm");
    mock_wasm_manager_t wasm_manager(engine_t::wamr);

    wasm_manager.initialize("plugin_name", "plugin_id", "plugin_vm_id", "plugin_configiguration",
                            false, "vm_id", "vm_configuration", {}, {}, wasm, false);
    wasm_manager.get_or_create_thread_local_plugin();
}
