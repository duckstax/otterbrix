#pragma once

#include <memory>

#include <utility>

#include <rocketjoe/cache/cache_element.hpp>


namespace rocketjoe { namespace api { namespace cache {

        struct cache_command final {

            cache_command() = default;
            cache_command(const cache_command&)= default;
            cache_command&operator=(const cache_command&)= default;
            cache_command(cache_command&&)= default;
            cache_command&operator=(cache_command&&) = default;
            ~cache_command() = default;

            cache_command(std::string command):command_(std::move(command)){}

            cache_command(std::string space,std::string command, std::string key ):
                space_(std::move(space)),
                command_(std::move(command)),
                key_(std::move(key)){

            }

            auto space() const -> const std::string& {
                return space_;
            }

            auto command() const -> const std::string& {
                    return command_;
            }

            auto key() const -> const std::string& {
                    return key_;
            }

            auto value() -> cache_element_base*   {
                return element_.get();
            }

        protected:
            std::string space_;
            std::string command_;
            std::string key_;
            actor_zeta::intrusive_ptr<cache_element_base> element_;
        };

}}}