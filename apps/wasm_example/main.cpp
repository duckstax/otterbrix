#include "services_manager.hpp"

#include <locale>
#include <iostream>

#include <boost/locale/generator.hpp>
#include <boost/program_options/options_description.hpp>
#include <boost/program_options/variables_map.hpp>
#include <boost/program_options/parsers.hpp>

using namespace std;
namespace po = boost::program_options;

auto main(int argc, char* argv[]) -> int {
    string log_dir(".");
    auto log = initialization_logger("wasm_example", log_dir);

    locale::global(boost::locale::generator()(""));

    po::options_description command_line_description("Allowed options");

    command_line_description.add_options()
        ("help", "Print help")
        ("wasm-path", po::value<filesystem::path>()->default_value("main.wasm"), "Path to wasm module");

    vector<string> all_args(argv, argv + argc);
    po::variables_map command_line;

    po::store(
        po::command_line_parser(all_args)
            .options(command_line_description)
            .run(),
        command_line);

    if (command_line.count("help")) {
        cout << command_line_description << endl;

        return 1;
    }

    auto wasm_path = command_line["wasm-path"].as<filesystem::path>();
    auto services_manager = goblin_engineer::make_manager_service<example_wasm_t>();

    actor_zeta::send(services_manager, actor_zeta::address_t::empty_address(), "start", wasm_path);

    // Temporary hack
    std::this_thread::sleep_for(std::chrono::seconds(1));

    return 0;
}
