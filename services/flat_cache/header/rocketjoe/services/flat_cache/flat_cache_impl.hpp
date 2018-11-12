#pragma once

#include <unordered_map>
#include <memory>

#include <rocketjoe/api/cache_element.hpp>

namespace rocketjoe { namespace services { namespace flat_cache {


            class space  final {
            public:

                space() = default;

                ~space() = default;

                auto set(const std::string&key,api::cache_element element) -> void {
                    storage.emplace(key,std::move(element));
                }

                auto get(const std::string&key) -> api::cache_element {
                    return storage.at(key);
                }

                auto pop(const std::string&key) -> void {
                    storage.erase(key);
                }

            private:
                std::unordered_map<std::string,api::cache_element> storage;
            };


            class flat_cache_impl final {
            public:

                flat_cache_impl() = default;

                ~flat_cache_impl() = default;

                auto create(const std::string&space_name) -> void {
                    storage.emplace(space_name,std::make_unique<space>());
                }

                auto set(const std::string&space_name,const std::string&key,api::cache_element_base* element) -> void {
                    get_space(space_name).set(key,api::cache_element(element));
                }

                auto get(const std::string&space_name,const std::string&key) -> api::cache_element {
                    return get_space(space_name).get(key);
                }

                auto pop(const std::string&space_name,const std::string&key) -> void {
                    get_space(space_name).pop(key);
                }

            private:
                auto get_space(const std::string&space_name) -> space& {
                    return *(storage.at(space_name).get());
                }
                std::unordered_map<std::string,std::unique_ptr<space>> storage;
            };



        }}}