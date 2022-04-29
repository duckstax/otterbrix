#include "wasm.hpp"

#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/uuid_io.hpp>

#include <proxy-wasm/null.h>
#include <proxy-wasm/wamr.h>

using namespace std;
using namespace proxy_wasm;

namespace components::wasm_runner {

    wasm_vm_integration_t::wasm_vm_integration_t()
        : log_(get_logger()) {}

    auto wasm_vm_integration_t::clone() -> WasmVmIntegration* {
        return new wasm_vm_integration_t;
    }

    auto wasm_vm_integration_t::getLogLevel() -> LogLevel {
        auto log_level = LogLevel::info;

        switch (log_.get_level()) {
            case log_t::level::trace:
                log_level = LogLevel::trace;

                break;
            case log_t::level::debug:
                log_level = LogLevel::debug;

                break;
            case log_t::level::info:
                log_level = LogLevel::info;

                break;
            case log_t::level::warn:
                log_level = LogLevel::warn;

                break;
            case log_t::level::err:
                log_level = LogLevel::error;

                break;
            case log_t::level::critical:
                log_level = LogLevel::critical;

                break;
            default:
                break;
        }

        return log_level;
    }

    auto wasm_vm_integration_t::error(string_view message) -> void {
        log_.error(message);
    }

    auto wasm_vm_integration_t::trace(string_view message) -> void {
        log_.trace(message);
    }

    auto wasm_vm_integration_t::getNullVmFunction(string_view /*function_name*/, bool /*returns_word*/,
                                                  int /*number_of_arguments*/, NullPlugin* /*plugin*/,
                                                  void* /*ptr_to_function_return*/) -> bool {
        return false;
    }

    wasm_context_t::wasm_context_t(WasmBase* wasm)
        : ContextBase(wasm)
        , log_(get_logger()) {}

    wasm_context_t::wasm_context_t(WasmBase* wasm, const shared_ptr<PluginBase>& plugin)
        : ContextBase(wasm, plugin)
        , log_(get_logger()) {}

    WasmResult wasm_context_t::log(uint32_t log_level, string_view message) {
        switch (static_cast<log_t::level>(log_level)) {
            case log_t::level::trace:
                log_.trace(message);

                break;
            case log_t::level::debug:
                log_.debug(message);

                break;
            case log_t::level::info:
                log_.info(message);

                break;
            case log_t::level::warn:
                log_.warn(message);

                break;
            case log_t::level::err:
                log_.error(message);

                break;
            case log_t::level::critical:
                log_.critical(message);

                break;
            default:
                return WasmResult::BadArgument;
        }

        return WasmResult::Ok;
    }

    auto wasm_context_t::getLogLevel() -> uint32_t {
        return static_cast<uint32_t>(log_.get_level());
    }

    wasm_t::wasm_t(unique_ptr<WasmVm> vm, string_view vm_id,
                   string_view vm_configuration, string_view vm_key,
                   unordered_map<string, string> envs, AllowedCapabilitiesMap allowed_capabilities)
        : WasmBase(move(vm), vm_id, vm_configuration, vm_key, move(envs), move(allowed_capabilities)) {}

    wasm_t::wasm_t(const shared_ptr<WasmHandleBase>& wasm, const WasmVmFactory& factory)
        : WasmBase(wasm, factory) {}

    auto wasm_t::createVmContext() -> ContextBase* {
        return new wasm_context_t(this);
    }

    auto wasm_t::createRootContext(const shared_ptr<PluginBase>& plugin) -> ContextBase* {
        return new wasm_context_t(this, plugin);
    }

    auto wasm_t::createContext(const shared_ptr<PluginBase>& plugin) -> ContextBase* {
        return new wasm_context_t(this, plugin);
    }

    static auto to_string(engine_t engine) -> string {
        switch (engine) {
            case engine_t::wamr:
                return "wamr";
            case engine_t::null:
                return "null";
        }

        return "unknown";
    }

    wasm_manager_t::wasm_manager_t(engine_t engine)
        : engine_(engine)
        , plugin_(nullptr)
        , wasm_(nullptr) {}

    auto wasm_manager_t::initialize(string_view plugin_name, string_view plugin_id,
                                    string_view plugin_vm_id, string_view plugin_configuration,
                                    bool fail_open, string_view vm_id,
                                    string_view vm_configuration, unordered_map<string, string> envs,
                                    AllowedCapabilitiesMap allowed_capabilities, const string& code,
                                    bool allow_precompiled) -> void {
        if (!plugin_) {
            plugin_ = create_plugin_and_initialize(plugin_name, plugin_id, plugin_vm_id, plugin_configuration, fail_open);
        }

        if (!wasm_) {
            wasm_ = create_wasm_and_load_code(vm_id, vm_configuration, move(envs), move(allowed_capabilities), code, allow_precompiled);
        }
    }

    auto wasm_manager_t::get_or_create_thread_local_plugin() const -> shared_ptr<PluginHandleBase> {
        if (!plugin_ || !wasm_) {
            return nullptr;
        }

        auto wasm_clone_factory = [this](auto wasm) {
            auto wasm_clone = clone_wasm(move(wasm));

            return make_shared<WasmHandleBase>(move(wasm_clone));
        };

        auto plugin_factory = [](auto wasm, auto plugin) {
            return make_shared<PluginHandleBase>(move(wasm), move(plugin));
        };

        return getOrCreateThreadLocalPlugin(wasm_, plugin_, wasm_clone_factory, plugin_factory);
    }

    auto wasm_manager_t::create_plugin(string_view plugin_name, string_view plugin_id,
                                       string_view plugin_vm_id, string_view plugin_configuration,
                                       string_view plugin_key, string_view engine,
                                       bool fail_open) const -> unique_ptr<PluginBase> {
        return make_unique<PluginBase>(plugin_name, plugin_id, plugin_vm_id, engine, plugin_configuration, fail_open, plugin_key);
    }

    auto wasm_manager_t::create_plugin_and_initialize(string_view plugin_name, string_view plugin_id,
                                                      string_view plugin_vm_id, string_view plugin_configuration,
                                                      bool fail_open) const -> unique_ptr<PluginBase> {
        boost::uuids::random_generator uuid_generator;
        auto plugin_key = to_string(uuid_generator());

        return create_plugin(plugin_name, plugin_id, plugin_vm_id, plugin_configuration, plugin_key, to_string(engine_), fail_open);
    }

    auto wasm_manager_t::create_vm_integration() const -> unique_ptr<WasmVmIntegration> {
        return make_unique<wasm_vm_integration_t>();
    }

    auto wasm_manager_t::new_vm() const -> unique_ptr<WasmVm> {
        unique_ptr<WasmVm> vm;

        switch (engine_) {
            case engine_t::wamr:
                vm = createWamrVm();

                break;
            case engine_t::null:
                vm = createNullVm();

                break;
        }

        vm->integration() = create_vm_integration();

        return vm;
    };

    auto wasm_manager_t::create_wasm(string_view vm_id, string_view vm_configuration,
                                     string_view vm_key, unordered_map<string, string> envs,
                                     AllowedCapabilitiesMap allowed_capabilities) const -> unique_ptr<WasmBase> {
        return make_unique<wasm_t>(new_vm(), vm_id, vm_configuration, vm_key, move(envs), move(allowed_capabilities));
    }

    auto wasm_manager_t::clone_wasm(shared_ptr<WasmHandleBase> wasm) const -> unique_ptr<WasmBase> {
        return make_unique<wasm_t>(move(wasm), [this]() {
            return new_vm();
        });
    }

    auto wasm_manager_t::create_wasm_and_load_code(string_view vm_id, string_view vm_configuration,
                                                   unordered_map<string, string> envs, AllowedCapabilitiesMap allowed_capabilities,
                                                   const string& code, bool allow_precompiled) const -> shared_ptr<WasmHandleBase> {
        boost::uuids::random_generator uuid_generator;
        auto vm_key = to_string(uuid_generator());

        auto wasm_factory = [this, vm_id, vm_configuration, envs = move(envs),
                             allowed_capabilities = move(allowed_capabilities)](auto vm_key) {
            auto wasm = create_wasm(vm_id, vm_configuration, vm_key, move(envs), move(allowed_capabilities));

            return make_shared<WasmHandleBase>(move(wasm));
        };

        auto wasm_clone_factory = [this](auto wasm) {
            auto wasm_clone = clone_wasm(move(wasm));

            return make_shared<WasmHandleBase>(move(wasm_clone));
        };

        return createWasm(vm_key, code, plugin_, wasm_factory, wasm_clone_factory, allow_precompiled);
    }

} // namespace components::wasm_runner
