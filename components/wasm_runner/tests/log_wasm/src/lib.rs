use log::info;

use proxy_wasm::types::*;
use proxy_wasm::traits::*;
use proxy_wasm::hostcalls::*;

proxy_wasm::main! {{
    proxy_wasm::set_log_level(LogLevel::Trace);
    proxy_wasm::set_root_context(|_| -> Box<dyn RootContext> { Box::new(HelloWorld) });
}}

struct HelloWorld;

impl Context for HelloWorld {}

impl RootContext for HelloWorld {
    fn on_vm_start(&mut self, _: usize) -> bool {
        let log_level = get_log_level().expect("unexpected error");

        assert_eq!(log_level, LogLevel::Info);

        info!("Hello, World!");

        true
    }
}
