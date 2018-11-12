#pragma once

#include <unordered_map>

#include <bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/builder/stream/document.hpp>
#include <bsoncxx/builder/stream/array.hpp>
#include <bsoncxx/builder/stream/value_context.hpp>
#include <bsoncxx/json.hpp>

#include <mongocxx/client.hpp>
#include <mongocxx/instance.hpp>
#include <mongocxx/uri.hpp>
#include <mongocxx/cursor.hpp>
#include <mongocxx/bulk_write.hpp>

namespace rocketjoe { namespace services { namespace object_storage {

            class object_storage_implement {
            public:
                object_storage_implement() = delete;

                object_storage_implement &operator=(object_storage_implement &&) = default;

                object_storage_implement(object_storage_implement &&) = default;

                object_storage_implement &operator=(const object_storage_implement &) = default;

                object_storage_implement(const object_storage_implement &) = default;

                virtual ~object_storage_implement() = default;

                object_storage_implement(const std::string &uri_mongo);

                auto insert(
                        const std::string&database,
                        const std::string&collection,
                        bsoncxx::document::view_or_value document
                ) ->  mongocxx::stdx::optional<mongocxx::result::insert_one>;


                auto replace(
                        const std::string&database,
                        const std::string&collection,
                        bsoncxx::document::view_or_value filter,
                        bsoncxx::document::view_or_value replacement
                ) -> mongocxx::stdx::optional<mongocxx::result::update> ;


                auto update(
                        const std::string&database,
                        const std::string&collection,
                        bsoncxx::document::view_or_value filter,
                        bsoncxx::document::view_or_value placement
                ) -> mongocxx::stdx::optional<mongocxx::result::update> ;


                auto remove(
                        const std::string &database,
                        const std::string &collection,
                        bsoncxx::document::view_or_value filter
                ) -> mongocxx::stdx::optional<mongocxx::result::delete_result> ;

            protected:
                mongocxx::database &find_and_create_database(const std::string &name);

                mongocxx::collection
                find_and_create_collection(const std::string &db_name, const std::string &collection);
            private:
                mongocxx::instance mongo_inst;
                std::unordered_map<std::string, mongocxx::database> database_storage;

                mongocxx::uri uri;
                mongocxx::client client;
            };

}}}