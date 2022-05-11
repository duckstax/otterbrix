#include "wasm.hpp"

#include <fstream>
#include <sstream>

#include <actor-zeta/core.hpp>

using namespace std;

namespace services::wasm {

    string read_wasm_file(const boost::filesystem::path& path) {
        ifstream file(path.string(), ios::binary);
        stringstream file_string_stream;

        file_string_stream << file.rdbuf();

        return file_string_stream.str();
    }

    auto wasm_runner_t::load_code(const boost::filesystem::path& path) -> void {
        auto code = read_wasm_file(path);

        wasm_manager_.initialize("", "", "", "", false, "", "", {}, {}, code, false);
        wasm_ = wasm_manager_.get_or_create_thread_local_plugin();
    }

} // namespace services::wasm
