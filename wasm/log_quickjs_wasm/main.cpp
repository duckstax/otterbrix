#include <proxy_wasm_intrinsics.h>
#include <quickjs.h>
//#include <quickjs-libc.h>

using namespace std;

static auto js_context_deleter(JSContext* ctx) -> void {
    JS_FreeContext(ctx);
}

using js_context_unique_ptr = unique_ptr<JSContext, decltype(&js_context_deleter)>;

static auto js_runtime_deleter(JSRuntime* rt) -> void {
    //js_std_free_handlers(rt);
    JS_FreeRuntime(rt);
}

using js_runtime_unique_ptr = std::unique_ptr<JSRuntime, decltype(&js_runtime_deleter)>;

static auto make_js_runtime() -> js_runtime_unique_ptr {
    return js_runtime_unique_ptr(JS_NewRuntime(), &js_runtime_deleter);
}

class js_value_deleter_t {
public:
    js_value_deleter_t(JSContext* ctx);

    js_value_deleter_t(const js_value_deleter_t&) = delete;

    js_value_deleter_t(js_value_deleter_t&&) = default;

    auto operator=(const js_value_deleter_t&) -> js_value_deleter_t& = delete;

    auto operator=(js_value_deleter_t&&) -> js_value_deleter_t& = default;

    auto operator()(JSValue value) -> void;

private:
    JSContext* ctx_;
};

js_value_deleter_t::js_value_deleter_t(JSContext* ctx) : ctx_(ctx) {}

auto js_value_deleter_t::operator()(JSValue value) -> void {
    JS_FreeValue(ctx_, value);
}

#ifdef CONFIG_CHECK_JSVALUE
using js_value_unique_ptr = std::unique_ptr<std::remove_pointer<JSValue>, js_value_deleter_t>;
#else
class js_value_raii {
public:
    js_value_raii(JSValue value, js_value_deleter_t deleter);

    js_value_raii(const js_value_raii&) = delete;

    js_value_raii(js_value_raii&&) = default;

    ~js_value_raii();

    auto operator=(const js_value_raii&) -> js_value_raii& = delete;

    auto operator=(js_value_raii&&) -> js_value_raii& = default;

    auto get() const -> const JSValue&;

private:
    JSValue value_;
    js_value_deleter_t deleter_;
};

js_value_raii::js_value_raii(JSValue value, js_value_deleter_t deleter) : value_(std::move(value)), deleter_(std::move(deleter)) {}

js_value_raii::~js_value_raii() {
    deleter_(std::move(value_));
}

auto js_value_raii::get() const -> const JSValue& {
    return value_;
}

using js_value_unique_ptr = js_value_raii;
#endif

static auto make_js_value(JSContext* ctx, JSValue value) -> js_value_unique_ptr {
    return js_value_unique_ptr(value, js_value_deleter_t(ctx));
}

class js_cstring_deleter_t {
public:
    js_cstring_deleter_t(JSContext* ctx);

    js_cstring_deleter_t(const js_cstring_deleter_t&) = delete;

    js_cstring_deleter_t(js_cstring_deleter_t&&) = default;

    auto operator=(const js_cstring_deleter_t&) -> js_cstring_deleter_t& = delete;

    auto operator=(js_cstring_deleter_t&&) -> js_cstring_deleter_t& = default;

    auto operator()(const char* cstring) -> void;

private:
    JSContext* ctx_;
};

js_cstring_deleter_t::js_cstring_deleter_t(JSContext* ctx) : ctx_(ctx) {}

auto js_cstring_deleter_t::operator()(const char* cstring) -> void {
    JS_FreeCString(ctx_, cstring);
}

using js_cstring_unique_ptr = std::unique_ptr<const char[], js_cstring_deleter_t>;

static auto make_js_cstring(JSContext* ctx, const char* cstring) -> js_cstring_unique_ptr {
    return js_cstring_unique_ptr(cstring, js_cstring_deleter_t(ctx));
}

static auto js_dump_object(js_context_unique_ptr& ctx, js_value_unique_ptr& value) -> void {
    auto object_string = make_js_cstring(ctx.get(), JS_ToCString(ctx.get(), value.get()));

    if (!object_string) {
        LOG_ERROR("[exception]");

        return;
    }

    auto object_string_view = string_view(&object_string[0]);

    LOG_ERROR(object_string_view.data());
}

static auto js_log(JSContext* ctx, [[maybe_unused]] JSValueConst this_val, int argc, JSValueConst* argv) -> JSValue {
    if (argc != 2) {
        return JS_EXCEPTION;
    }

    auto level = make_js_cstring(ctx, JS_ToCString(ctx, argv[0]));

    if (!level) {
        return JS_EXCEPTION;
    }

    auto level_view = string_view(&level[0]);

    auto message = make_js_cstring(ctx, JS_ToCString(ctx, argv[1]));

    if (!message) {
        return JS_EXCEPTION;
    }

    auto message_view = string_view(&message[0]);

    if (level_view == "trace") {
        LOG_TRACE(message_view.data());
    } else if (level_view == "debug") {
        LOG_DEBUG(message_view.data());
    } else if (level_view == "info") {
        LOG_INFO(message_view.data());
    } else if (level_view == "warn") {
        LOG_WARN(message_view.data());
    } else if (level_view == "error") {
        LOG_ERROR(message_view.data());
    } else {
        return JS_EXCEPTION;
    }

    return JS_UNDEFINED;
}

static const JSCFunctionListEntry js_proxy_wasm_funcs[] = {
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wc99-designator"
    JS_CFUNC_DEF("log", 2, js_log),
#pragma GCC diagnostic pop
};

static auto js_proxy_wasm_funcs_init(JSContext* ctx, JSModuleDef* m) -> int {
    return JS_SetModuleExportList(ctx, m, js_proxy_wasm_funcs, 1);
}

static auto js_new_custom_context(JSRuntime* rt) -> JSContext* {
    auto ctx = JS_NewContext(rt);

    if (!ctx) {
        return nullptr;
    }

    JS_AddIntrinsicBigFloat(ctx);
    JS_AddIntrinsicBigDecimal(ctx);
    JS_AddIntrinsicOperators(ctx);
    JS_EnableBignumExt(ctx, true);

    //js_init_module_std(ctx, "std");
    //js_init_module_os(ctx, "os");

    auto m = JS_NewCModule(ctx, "proxy-wasm", js_proxy_wasm_funcs_init);

    if (!m) {
        JS_FreeContext(ctx);

        return nullptr;
    }

    if (JS_AddModuleExportList(ctx, m, js_proxy_wasm_funcs, 1)) {
        JS_FreeContext(ctx);

        return nullptr;
    }

    return ctx;
}

static auto make_js_context(js_runtime_unique_ptr& rt) -> js_context_unique_ptr {
    auto ctx = js_new_custom_context(rt.get());

    //js_std_set_worker_new_context_func(js_new_custom_context);
    //js_std_init_handlers(rt.get());

    return js_context_unique_ptr(ctx, &js_context_deleter);
}

class hello_world_quick_js_root_context_t : public RootContext {
public:
    hello_world_quick_js_root_context_t(uint32_t id, string_view root_id);

    auto onStart(size_t) -> bool override;

protected:
    js_runtime_unique_ptr rt;
    js_context_unique_ptr ctx;
};


hello_world_quick_js_root_context_t::hello_world_quick_js_root_context_t(uint32_t id, string_view root_id)
    : RootContext(id, root_id)
    , rt(make_js_runtime())
    , ctx(make_js_context(rt)) {}

auto hello_world_quick_js_root_context_t::onStart(size_t) -> bool {
    string_view script =
        "import * as proxy_wasm from 'proxy-wasm';\n"
        "proxy_wasm.log('info', 'Hello, World!');\n";

    if (JS_IsException(JS_Eval(ctx.get(), script.data(), script.size(), "<input>", JS_EVAL_TYPE_MODULE))) {
        auto exception = make_js_value(ctx.get(), JS_GetException(ctx.get()));

        js_dump_object(ctx, exception);

        if (JS_IsError(ctx.get(), exception.get())) {
            auto stack = make_js_value(ctx.get(), JS_GetPropertyStr(ctx.get(), exception.get(), "stack"));

            if (!JS_IsUndefined(stack.get())) {
                js_dump_object(ctx, stack);
            }
        }

        return false;
    }

    return true;
}

static RegisterContextFactory register_hello_world_quick_js_root_context(nullptr, ROOT_FACTORY(hello_world_quick_js_root_context_t), "plugin_id1");
