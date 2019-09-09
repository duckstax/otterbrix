#pragma once

#include <unordered_map>
#include <functional>

#include "file_manager.hpp"


namespace rocketjoe { namespace services { namespace python_engine {

            using operand = std::function<std::string(const std::string&)>;

            class data_set final {
            private:
                using storage_t = std::map<std::size_t, std::string>;
                using const_iterator = storage_t::const_iterator;
            public:
                data_set(file_view*file):file_(file){}

                data_set():file_(nullptr){}

                auto transform(operand &&f) {
                    for (auto &i:data_) {
                        i.second = f(i.second);
                    }
                }

                auto file() -> file_view* {
                    return file_;
                }

                auto append(const std::string&value ){
                    data_.emplace(data_.size(),std::move(value));
                }

                auto begin() const -> const_iterator {
                    return data_.cbegin();
                }

                auto end() const -> const_iterator {
                    data_.cend();
                }

            private:
                file_view*file_;
                std::map<std::size_t, std::string> data_;
            };
}}}