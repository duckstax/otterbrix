#include <fstream>
#include <sstream>

#include <boost/filesystem.hpp>

#include <catch2/catch.hpp>

#include <flatbuffers/flexbuffers.h>

#include <components/log/log.hpp>

#include "wasm.hpp"

using namespace std;
using namespace proxy_wasm;
using namespace components::wasm_runner;

string read_test_wasm_file(const boost::filesystem::path& path) {
    ifstream file(path.string(), ios::binary);
    stringstream file_string_stream;

    file_string_stream << file.rdbuf();

    return file_string_stream.str();
}

class mock_wasm_context_t : public wasm_context_t {
public:
    mock_wasm_context_t(string_view id, WasmBase* wasm);

    mock_wasm_context_t(string_view id, WasmBase* wasm, const shared_ptr<PluginBase>& plugin);

    auto log(uint32_t log_level, string_view message) -> WasmResult override;

    auto getLogLevel() -> uint32_t override;

    auto getProperty(string_view path, string* result) -> WasmResult override;

private:
    string_view id_;
};

class mock_wasm_t : public wasm_t {
public:
    mock_wasm_t(string_view id, unique_ptr<WasmVm> vm, string_view vm_id,
                string_view vm_configuration, string_view vm_key,
                unordered_map<string, string> envs,
                AllowedCapabilitiesMap allowed_capabilities);

    mock_wasm_t(string_view id, const shared_ptr<WasmHandleBase>& wasm, const WasmVmFactory& factory);

    auto createVmContext() -> ContextBase* override;

    auto createRootContext(const shared_ptr<PluginBase>& plugin) -> ContextBase* override;

    auto createContext(const shared_ptr<PluginBase>& plugin) -> ContextBase* override;

private:
    string_view id_;
};

class mock_wasm_manager_t : public wasm_manager_t {
public:
    mock_wasm_manager_t(string_view id, engine_t engine);

protected:
    auto create_wasm(string_view vm_id, string_view vm_configuration,
                     string_view vm_key, unordered_map<string, string> envs,
                     AllowedCapabilitiesMap allowed_capabilities) const -> unique_ptr<WasmBase> override;

    auto clone_wasm(shared_ptr<WasmHandleBase> wasm) const -> unique_ptr<WasmBase> override;

private:
    string_view id_;
};

mock_wasm_context_t::mock_wasm_context_t(string_view id, WasmBase* wasm) : wasm_context_t(wasm), id_(id) {}

mock_wasm_context_t::mock_wasm_context_t(string_view id, WasmBase* wasm, const shared_ptr<PluginBase>& plugin)
    : wasm_context_t(wasm, plugin), id_(id) {}

WasmResult mock_wasm_context_t::log(uint32_t log_level, string_view message) {
    REQUIRE(static_cast<log_t::level>(log_level) == log_t::level::info);

    if (id_ == "plugin_id0") {
        //REQUIRE(message == "[/rocketjoe/components/wasm_runner/tests/log_wasm/main.cpp:17]::onStart() Hello, World!");
    }

    if (id_ == "plugin_id1") {
        //REQUIRE(message == "[/rocketjoe/components/wasm_runner/tests/log_quickjs_wasm/main.cpp:159]::js_log() Hello, World!");
    }

    auto status = wasm_context_t::log(log_level, message);

    REQUIRE(status == WasmResult::Ok);

    return status;
}

auto mock_wasm_context_t::getLogLevel() -> uint32_t {
    auto log_level = wasm_context_t::getLogLevel();

    REQUIRE(static_cast<log_t::level>(log_level) == log_t::level::info);
    REQUIRE(static_cast<LogLevel>(log_level) == LogLevel::info);

    return log_level;
}

auto mock_wasm_context_t::getProperty(string_view path, string* result) -> WasmResult {
    REQUIRE(path == "plugin_root_id");

    auto status = wasm_context_t::getProperty(path, result);

    REQUIRE(status == WasmResult::Ok);
    REQUIRE(*result == id_);

    return status;
}

mock_wasm_t::mock_wasm_t(string_view id, unique_ptr<WasmVm> vm, string_view vm_id,
                         string_view vm_configuration, string_view vm_key,
                         unordered_map<string, string> envs, AllowedCapabilitiesMap allowed_capabilities)
    : wasm_t(move(vm), vm_id, vm_configuration, vm_key, move(envs), move(allowed_capabilities)), id_(id) {}

mock_wasm_t::mock_wasm_t(string_view id, const shared_ptr<WasmHandleBase>& wasm, const WasmVmFactory& factory)
    : wasm_t(wasm, factory), id_(id) {}

auto mock_wasm_t::createVmContext() -> ContextBase* {
    return new mock_wasm_context_t(id_, this);
}

auto mock_wasm_t::createRootContext(const shared_ptr<PluginBase>& plugin) -> ContextBase* {
    return new mock_wasm_context_t(id_, this, plugin);
}

auto mock_wasm_t::createContext(const shared_ptr<PluginBase>& plugin) -> ContextBase* {
    return new mock_wasm_context_t(id_, this, plugin);
}

mock_wasm_manager_t::mock_wasm_manager_t(string_view id, engine_t engine) : wasm_manager_t(engine), id_(id) {}

auto mock_wasm_manager_t::create_wasm(string_view vm_id, string_view vm_configuration,
                                      string_view vm_key, unordered_map<string, string> envs,
                                      AllowedCapabilitiesMap allowed_capabilities) const -> unique_ptr<WasmBase> {
    return make_unique<mock_wasm_t>(id_, new_vm(), vm_id, vm_configuration, vm_key, move(envs), move(allowed_capabilities));
}

auto mock_wasm_manager_t::clone_wasm(shared_ptr<WasmHandleBase> wasm) const -> unique_ptr<WasmBase> {
    return make_unique<mock_wasm_t>(id_, move(wasm), [this]() {
        return new_vm();
    });
}

TEST_CASE("wasm_manager_t log", "[API]") {
    string log_dir(".");
    auto log = initialization_logger("wasm_runner", log_dir);
    auto wasm = read_test_wasm_file(boost::filesystem::path("log_wasm") / "log_wasm.wasm");
    string_view plugin_id = "plugin_id0";
    mock_wasm_manager_t wasm_manager(plugin_id, engine_t::wamr);

    wasm_manager.initialize("plugin_name0", plugin_id, "plugin_vm_id0", "plugin_configiguration0",
                            false, "vm_id0", "vm_configuration0", {}, {}, wasm, false);
    wasm_manager.get_or_create_thread_local_plugin();
}

TEST_CASE("wasm_manager_t log_quickjs", "[API]") {
    string log_dir(".");
    auto log = initialization_logger("wasm_runner", log_dir);
    auto wasm = read_test_wasm_file(boost::filesystem::path("log_quickjs_wasm") / "log_quickjs_wasm.wasm");
    string_view plugin_id = "plugin_id1";
    mock_wasm_manager_t wasm_manager(plugin_id, engine_t::wamr);

    wasm_manager.initialize("plugin_name1", plugin_id, "plugin_vm_id1", "plugin_configiguration1",
                            false, "vm_id1", "vm_configuration1", {}, {}, wasm, false);
    wasm_manager.get_or_create_thread_local_plugin();
}

TEST_CASE("wasm_manager_t flatbuffers", "[API]") {
    string log_dir(".");
    auto log = initialization_logger("wasm_runner", log_dir);
    auto wasm = read_test_wasm_file(boost::filesystem::path("wasm_flatbuffers") / "wasm_flatbuffers.wasm");
    string_view plugin_id = "m_plugin_id_1";
    mock_wasm_manager_t wasm_manager(plugin_id, engine_t::wamr);

    flexbuffers::Builder fbb;
    fbb.Map([&]() {
        fbb.String("name", "name_document");
        fbb.Int("count", 1000000000000);
        fbb.Double("value", 1000000000000.0001);
    });
    fbb.Finish();
    auto map = flexbuffers::GetRoot(fbb.GetBuffer()).AsMap();
    info(log, "name: {}, count: {}, value: {}", map["name"].AsString().str(), map["count"].AsInt64(), map["value"].AsDouble());

    wasm_manager.initialize("m_plugin_name_1", plugin_id, "m_plugin_vm_id_1", "m_plugin_configiguration_1",
                            false, "m_vm_id_1", "m_vm_configuration_1", {}, {}, wasm, false);
    wasm_manager.copy_data("document", fbb.GetBuffer());
    wasm_manager.get_or_create_thread_local_plugin();
}
