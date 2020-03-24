#include <chrono>
#include <ctime>
#include <string>
#include <vector>

#include <boost/process/environment.hpp>

#include "spdlog/sinks/basic_file_sink.h"
#include "spdlog/spdlog.h"

auto logo() -> const char * {
  constexpr const char *data = R"_(
-------------------------------------------------
       __     __                                       __
  ____/ /__  / /_  __  ______ _   ____ ___  ____  ____/ /__
 / __  / _ \/ __ \/ / / / __ `/  / __ `__ \/ __ \/ __  / _ \
/ /_/ /  __/ /_/ / /_/ / /_/ /  / / / / / / /_/ / /_/ /  __/
\__,_/\___/_.___/\__,_/\__, /  /_/ /_/ /_/\____/\__,_/\___/
                      /____/
-------------------------------------------------
)_";
  return data;
}

int main(int argc, char *argv[]) {
  auto id = boost::this_process::get_id();
  std::string filename;
  ////example: "logs/basic.txt"
  filename = std::string("logs/").append(std::to_string(id)).append(".txt");
  auto file_logger = spdlog::basic_logger_mt("basic_logger", filename);
  spdlog::set_default_logger(file_logger);

  auto start = std::chrono::system_clock::now();
  auto end = std::chrono::system_clock::now();

  std::chrono::duration<double> elapsed_seconds = end - start;
  std::time_t end_time = std::chrono::system_clock::to_time_t(end);

  spdlog::info(logo());

  {
    std::string message;
    message.append("finished computation at ")
        .append(std::ctime(&end_time))
        .append("elapsed time: ")
        .append(std::to_string(elapsed_seconds.count()))
        .append("s");
    spdlog::info(message);
  }

  std::vector<std::string> all_args(argv, argv + argc);

  int j = 0;
  for (const auto &i : all_args) {
    std::string message;
    message.append("Arg").append(std::to_string(j)).append(" : ").append(i);
    spdlog::info(message);
    ++j;
  }

  return 0;
}