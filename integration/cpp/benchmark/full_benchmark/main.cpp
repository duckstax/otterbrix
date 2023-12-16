#include "../classes.hpp"

static constexpr int count_insert_one = 1000;
static constexpr int count_insert_many = 50;
static constexpr int size_insert_many = 100;
static constexpr int count_find_one = 100;
static constexpr int count_find_many = 100;
static constexpr int count_update_one = 100;
static constexpr int count_update_many = 100;
static constexpr int count_delete_one = 100;
static constexpr int count_delete_many = 100;

void work(benchmark::State& state) {
    state.PauseTiming();
    auto* dispatcher = unique_spaces::get().dispatcher();
    auto session = otterbrix::session_id_t();
    state.ResumeTiming();
    int n_state = 0;
    for (auto _ : state) {
        // create database and collection
        ++n_state;
        auto db_name = database_name + "_" + std::to_string(n_state);
        auto col_name = collection_name + "_" + std::to_string(n_state);
        dispatcher->create_database(session, db_name);
        dispatcher->create_collection(session, db_name, col_name);

        // insert_one
        for (int i_doc = 1; i_doc <= count_insert_one; ++i_doc) {
            auto doc = gen_doc(i_doc);
            dispatcher->insert_one(session, db_name, col_name, doc);
        }

        // insert_many
        auto n_doc = count_insert_one;
        for (int i_insert = 0; i_insert < count_insert_many; ++i_insert) {
            std::list<document_ptr> docs;
            for (int i_doc = 0; i_doc < size_insert_many; ++i_doc) {
                docs.push_back(gen_doc(++n_doc));
            }
            dispatcher->insert_many(session, db_name, col_name, docs);
        }

        // find_one
        for (int i_find = 1; i_find < count_find_one; ++i_find) {
            dispatcher->find_one(session, db_name, col_name, make_document());
            dispatcher->find_one(session, db_name, col_name, make_condition("id_", "$eq", std::to_string(i_find)));
            dispatcher->find_one(session,
                                 db_name,
                                 col_name,
                                 make_condition("id_", "$eq", std::to_string(size_collection - i_find)));
        }

        // find_many
        for (int i_find = 1; i_find < count_find_many; ++i_find) {
            dispatcher->find(session, db_name, col_name, make_document());
            dispatcher->find(session, db_name, col_name, make_condition("id_", "$eq", std::to_string(i_find)));
            dispatcher->find(session,
                             db_name,
                             col_name,
                             make_condition("id_", "$eq", std::to_string(size_collection - i_find)));
        }

        // update_one
        for (int i_update = 0; i_update < count_update_one; ++i_update) {
            dispatcher->update_one(session,
                                   db_name,
                                   col_name,
                                   make_condition("id_", "$eq", std::to_string(i_update)),
                                   make_condition("$set", "count", 0),
                                   false);
            dispatcher->update_one(session,
                                   db_name,
                                   col_name,
                                   make_condition("id_", "$eq", std::to_string(size_collection - i_update)),
                                   make_condition("$set", "count", size_collection),
                                   false);
        }

        // update_many
        for (int i_update = 0; i_update < count_update_many; ++i_update) {
            dispatcher->update_many(session,
                                    db_name,
                                    col_name,
                                    make_condition("count", "$lt", count_update_many * i_update),
                                    make_condition("$set", "count", 0),
                                    false);
            dispatcher->update_many(session,
                                    db_name,
                                    col_name,
                                    make_condition("count", "$gt", size_collection - count_update_many * i_update),
                                    make_condition("$set", "count", size_collection),
                                    false);
        }

        // delete_one
        for (int i_delete = 0; i_delete < count_delete_one; ++i_delete) {
            dispatcher->delete_one(session, db_name, col_name, make_condition("id_", "$eq", std::to_string(i_delete)));
            dispatcher->delete_one(session,
                                   db_name,
                                   col_name,
                                   make_condition("id_", "$eq", std::to_string(size_collection - i_delete)));
        }

        // delete_many
        for (int i_delete = 0; i_delete < count_delete_many; ++i_delete) {
            dispatcher->delete_many(session, db_name, col_name, make_condition("count", "$lt", 100 * i_delete));
            dispatcher->delete_many(session,
                                    db_name,
                                    col_name,
                                    make_condition("count", "$gt", size_collection - 100 * i_delete));
        }
        dispatcher->delete_many(session, db_name, col_name, make_document());
    }
}
BENCHMARK(work);

int main(int argc, char** argv) {
    ::benchmark::Initialize(&argc, argv);
    if (::benchmark::ReportUnrecognizedArguments(argc, argv)) {
        return 1;
    }
    unique_spaces::get();
    ::benchmark::RunSpecifiedBenchmarks();
    ::benchmark::Shutdown();
    return 0;
}
