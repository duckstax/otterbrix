#include <rocketjoe/services/object_storage/object_storage_implement.hpp>

#include <bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/builder/stream/document.hpp>
#include <bsoncxx/builder/stream/array.hpp>
#include <bsoncxx/builder/stream/value_context.hpp>
#include <bsoncxx/json.hpp>

namespace rocketjoe { namespace services { namespace object_storage {


            object_storage_implement::object_storage_implement(const std::string &uri_mongo) {
                uri = mongocxx::uri{uri_mongo};
                client = mongocxx::client{uri};
            }

            mongocxx::database &object_storage_implement::find_and_create_database(const std::string &name) {

                auto db = database_storage.find(name);

                if (db == database_storage.end()) {

                    auto result = database_storage.emplace(name, client.database(name));
                    return result.first->second;
                } else {
                    return db->second;
                }

            }

            mongocxx::collection object_storage_implement::find_and_create_collection(
                    const std::string &db_name,
                    const std::string &collection) {

                auto &db = find_and_create_database(db_name);

                if (!db.has_collection(collection)) {
                    return db.create_collection(collection);
                } else {
                    return db.collection(collection);
                }

            }

            auto object_storage_implement::replace(
                    const std::string &database,
                    const std::string &collection,
                    bsoncxx::document::view_or_value filter,
                    bsoncxx::document::view_or_value replacement
            ) -> mongocxx::stdx::optional<mongocxx::result::update> {

                mongocxx::options::update options;

                options.upsert(true);

                return find_and_create_collection(database, collection).update_one(filter.view(), replacement.view(), options);

            }

            auto object_storage_implement::insert(
                    const std::string &database,
                    const std::string &collection,
                    bsoncxx::document::view_or_value document
            ) -> mongocxx::stdx::optional<mongocxx::result::insert_one> {
                mongocxx::options::insert options;
                return find_and_create_collection(database, collection).insert_one(document,options);
            }

            auto object_storage_implement::update(
                    const std::string &database,
                    const std::string &collection,
                    bsoncxx::document::view_or_value filter,
                    bsoncxx::document::view_or_value placement
            ) -> mongocxx::stdx::optional<mongocxx::result::update> {
                return find_and_create_collection(database, collection).update_one(filter,placement);
            }

            auto object_storage_implement::remove(
                    const std::string &database,
                    const std::string &collection,
                    bsoncxx::document::view_or_value filter
            ) -> mongocxx::stdx::optional<mongocxx::result::delete_result> {
                return find_and_create_collection(database, collection).delete_one(filter);
            }


        }}}