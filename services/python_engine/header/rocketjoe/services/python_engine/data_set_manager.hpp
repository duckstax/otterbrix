#pragma once

#include <unordered_map>
#include <functional>

#include <rocketjoe/services/python_engine/context_manager.hpp>
#include <rocketjoe/services/python_engine/file_manager.hpp>

#include <nlohmann/json.hpp>


namespace rocketjoe { namespace services { namespace python_engine {

            class data_set final {
            public:
                data_set(){}

                auto file() -> file_view* {
                }

            private:
                nlohmann::json data_set_;
                context_manager* ctx_;
            };
}}}