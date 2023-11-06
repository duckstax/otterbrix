#pragma once

#include <integration/cpp/base_spaces.hpp>
#include <components/tests/generaty.hpp>

using namespace components::ql;
using namespace components::expressions;

static const database_name_t database_name = "TestDatabase";
static const collection_name_t collection_name_without_index = "TestCollectionWithoutIndex";
static const collection_name_t collection_name_with_index = "TestCollectionWithIndex";
constexpr int size_collection = 10000;


template <bool on_wal, bool on_disk>
inline configuration::config create_config() {
    auto config = configuration::config::default_config();
    config.log.level = log_t::level::off;
    config.disk.on = on_disk;
    config.wal.on = on_wal;
    config.wal.sync_to_disk = on_disk;
    return config;
}

template <bool on_wal, bool on_disk>
class test_spaces final : public ottergon::base_ottergon_t {
public:
    static test_spaces &get() {
        static test_spaces<on_wal, on_disk> spaces_;
        return spaces_;
    }

private:
    test_spaces()
        : ottergon::base_ottergon_t(create_config<on_wal, on_disk>())
    {}
};


template <bool on_wal, bool on_disk>
void init_collection(const collection_name_t& collection_name) {
    auto* resource = actor_zeta::detail::pmr::get_default_resource();
    auto* dispatcher = test_spaces<on_wal, on_disk>::get().dispatcher();
    auto session = ottergon::session_id_t();
    dispatcher->create_database(session, database_name);
    dispatcher->create_collection(session, database_name, collection_name);
    std::pmr::vector<document_ptr> docs(resource);
    for (int i = 1; i <= size_collection; ++i) {
        docs.push_back(gen_doc(i));
    }
    dispatcher->insert_many(session, database_name, collection_name, docs);
}

template <bool on_wal, bool on_disk>
void create_index(const collection_name_t& collection_name) {
    auto* dispatcher = test_spaces<on_wal, on_disk>::get().dispatcher();
    auto session = ottergon::session_id_t();
    create_index_t ql{database_name, collection_name, index_type::single, index_compare::int64};
    ql.keys_.emplace_back("count");
    dispatcher->create_index(session, ql);
}

template <bool on_wal, bool on_disk>
void init_spaces() {
    init_collection<on_wal, on_disk>(collection_name_without_index);
    init_collection<on_wal, on_disk>(collection_name_with_index);
    create_index<on_wal, on_disk>(collection_name_with_index);
}

template <bool on_wal, bool on_disk>
ottergon::wrapper_dispatcher_t* wr_dispatcher() {
    return test_spaces<on_wal, on_disk>::get().dispatcher();
}

template <bool on_index>
collection_name_t get_collection_name() {
    if constexpr (on_index) {
        return collection_name_with_index;
    } else {
        return collection_name_without_index;
    }
}

template <typename T = int>
aggregate_statement_raw_ptr create_aggregate(const collection_name_t& collection_name, compare_type compare = compare_type::eq,
                                             const std::string& key = {}, T value = T()) {
    auto* aggregate = new aggregate_statement(database_name, collection_name);
    aggregate::match_t match;
    if (!key.empty()) {
        aggregate->add_parameter(core::parameter_id_t{1}, value);
        match.query = make_compare_expression(actor_zeta::detail::pmr::get_default_resource(), compare,
                                              components::expressions::key_t{key}, core::parameter_id_t{1});
    }
    aggregate->append(aggregate::operator_type::match, match);
    return aggregate;
}

template <typename T = int>
document_ptr gen_update(const std::string& key = {}, T value = T()) {
    auto doc = document::impl::mutable_dict_t::new_dict();
    auto val = document::impl::mutable_dict_t::new_dict();
    val->set(key, value);
    doc->set("$set", val);
    return make_document(doc);
}
