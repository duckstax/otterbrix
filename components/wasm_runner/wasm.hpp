#pragma once

#include <proxy-wasm/context.h>
#include <proxy-wasm/wasm.h>

#include <components/log/log.hpp>

namespace components::wasm_runner {

    class wasm_vm_integration_t : public proxy_wasm::WasmVmIntegration {
    public:
        wasm_vm_integration_t();

        auto clone() -> proxy_wasm::WasmVmIntegration* override;

        auto getLogLevel() -> proxy_wasm::LogLevel override;

        auto error(std::string_view message) -> void override;

        auto trace(std::string_view message) -> void override;

        auto getNullVmFunction(std::string_view /*function_name*/,
                               bool /*returns_word*/,
                               int /*number_of_arguments*/,
                               proxy_wasm::NullPlugin* /*plugin*/,
                               void* /*ptr_to_function_return*/) -> bool override;

    private:
        log_t log_;
    };

    class wasm_context_t : public proxy_wasm::ContextBase {
    public:
        wasm_context_t(proxy_wasm::WasmBase* wasm);

        wasm_context_t(proxy_wasm::WasmBase* wasm, const std::shared_ptr<proxy_wasm::PluginBase>& plugin);

        auto log(uint32_t log_level, std::string_view message) -> proxy_wasm::WasmResult override;

        auto getLogLevel() -> uint32_t override;

        auto getProperty(std::string_view path, std::string* result) -> proxy_wasm::WasmResult override;

        auto getCurrentTimeNanoseconds() -> uint64_t override;

    private:
        log_t log_;
        std::unordered_map<std::string, std::string> proporties;
    };

    class wasm_t : public proxy_wasm::WasmBase {
    public:
        wasm_t(std::unique_ptr<proxy_wasm::WasmVm> vm,
               std::string_view vm_id,
               std::string_view vm_configuration,
               std::string_view vm_key,
               std::unordered_map<std::string, std::string> envs,
               proxy_wasm::AllowedCapabilitiesMap allowed_capabilities);

        wasm_t(const std::shared_ptr<proxy_wasm::WasmHandleBase>& wasm, const proxy_wasm::WasmVmFactory& factory);

        auto createVmContext() -> proxy_wasm::ContextBase* override;

        auto createRootContext(const std::shared_ptr<proxy_wasm::PluginBase>& plugin)
            -> proxy_wasm::ContextBase* override;

        auto createContext(const std::shared_ptr<proxy_wasm::PluginBase>& plugin) -> proxy_wasm::ContextBase* override;
    };

    enum class engine_t : uint8_t { wamr, null };

    class wasm_manager_t {
    public:
        /**
         * PluginBase is container to hold plugin information which is shared with all Context(s) created
         * for a given plugin. Embedders may extend this class with additional host-specific plugin
         * information as required.
         * @param plugin_name is the name of the plugin.
         * @param plugin_id is an identifier for the in VM handlers for this plugin.
         * @param plugin_vm_id is a string used to differentiate VMs with the same code and VM configuration.
         * @param plugin_configuration is configuration for this plugin.
         * @param fail_open if true the plugin will pass traffic as opposed to close all streams.
         */
        wasm_manager_t(engine_t engine);

        virtual ~wasm_manager_t() = default;

        auto initialize(std::string_view plugin_name,
                        std::string_view plugin_id,
                        std::string_view plugin_vm_id,
                        std::string_view plugin_configuration,
                        bool fail_open,
                        std::string_view vm_id,
                        std::string_view vm_configuration,
                        std::unordered_map<std::string, std::string> envs,
                        proxy_wasm::AllowedCapabilitiesMap allowed_capabilities,
                        const std::string& code,
                        bool allow_precompiled) -> void;

        auto get_or_create_thread_local_plugin() const -> std::shared_ptr<proxy_wasm::PluginHandleBase>;
        auto copy_data(const std::string& key, const std::vector<uint8_t>& data) -> void;

    protected:
        virtual auto create_plugin(std::string_view plugin_name,
                                   std::string_view plugin_id,
                                   std::string_view plugin_vm_id,
                                   std::string_view plugin_configuration,
                                   std::string_view plugin_key,
                                   std::string_view engine,
                                   bool fail_open) const -> std::unique_ptr<proxy_wasm::PluginBase>;

        auto create_plugin_and_initialize(std::string_view plugin_name,
                                          std::string_view plugin_id,
                                          std::string_view plugin_vm_id,
                                          std::string_view plugin_configuration,
                                          bool fail_open) const -> std::unique_ptr<proxy_wasm::PluginBase>;

        virtual auto create_vm_integration() const -> std::unique_ptr<proxy_wasm::WasmVmIntegration>;

        auto new_vm() const -> std::unique_ptr<proxy_wasm::WasmVm>;

        virtual auto create_wasm(std::string_view vm_id,
                                 std::string_view vm_configuration,
                                 std::string_view vm_key,
                                 std::unordered_map<std::string, std::string> envs,
                                 proxy_wasm::AllowedCapabilitiesMap allowed_capabilities) const
            -> std::unique_ptr<proxy_wasm::WasmBase>;

        virtual auto clone_wasm(std::shared_ptr<proxy_wasm::WasmHandleBase> wasm) const
            -> std::unique_ptr<proxy_wasm::WasmBase>;

        auto create_wasm_and_load_code(std::string_view vm_id,
                                       std::string_view vm_configuration,
                                       std::unordered_map<std::string, std::string> envs,
                                       proxy_wasm::AllowedCapabilitiesMap allowed_capabilities,
                                       const std::string& code,
                                       bool allow_precompiled) const -> std::shared_ptr<proxy_wasm::WasmHandleBase>;

    private:
        engine_t engine_;
        std::shared_ptr<proxy_wasm::PluginBase> plugin_;
        std::shared_ptr<proxy_wasm::WasmHandleBase> wasm_;
    };

} // namespace components::wasm_runner
