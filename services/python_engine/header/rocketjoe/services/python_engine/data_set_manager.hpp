#pragma once

#include <unordered_map>
#include <functional>

#include <rocketjoe/services/python_engine/forward.hpp>
#include <rocketjoe/services/python_engine/context_manager.hpp>

#include <nlohmann/json.hpp>


namespace rocketjoe { namespace services { namespace python_engine {

            class data_set final {
            public:
                data_set() = delete;
                data_set(context*ctx):ctx_(ctx){}

            private:
                nlohmann::json data_set_;
                std::unique_ptr<python_wrapper_data_set> wrapper_data_set_;
                context* ctx_;
            };
}}}