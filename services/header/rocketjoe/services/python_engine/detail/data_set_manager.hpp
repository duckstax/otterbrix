#pragma once

#include <unordered_map>
#include <functional>

#include <rocketjoe/services/python_engine/detail/forward.hpp>
#include <rocketjoe/services/python_engine/detail/context_manager.hpp>

#include <nlohmann/json.hpp>


namespace rocketjoe { namespace services { namespace python_engine { namespace detail {

            class data_set final {
            public:
                data_set() = default;
            private:
                nlohmann::json data_set_;
            };
}}}}