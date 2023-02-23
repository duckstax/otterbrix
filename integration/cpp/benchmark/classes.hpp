#pragma once

#include <integration/cpp/base_spaces.hpp>
//#include <components/document/document.hpp>
//#include <components/document/mutable/mutable_array.h>
//#include <components/document/mutable/mutable_dict.h>
#include <components/tests/generaty.hpp>

static const database_name_t database_name = "TestDatabase";
static const collection_name_t collection_name_without_index = "TestCollectionWithoutIndex";
static const collection_name_t collection_name_with_index = "TestCollectionWithIndex";
constexpr int size_collection = 10000;


inline configuration::config create_config(bool on_disk) {
    auto config = configuration::config::default_config();
    config.log.level = log_t::level::off;
    config.disk.on = on_disk;
    config.wal.sync_to_disk = on_disk;
    return config;
}

template <bool on_disk>
class test_spaces final : public duck_charmer::base_spaces {
public:
    static test_spaces &get() {
        static test_spaces<on_disk> spaces_;
        return spaces_;
    }

private:
    test_spaces()
        : duck_charmer::base_spaces(create_config(on_disk))
    {}
};


template <bool on_disk>
void init_collection(const collection_name_t& collection_name) {
    auto* resource = actor_zeta::detail::pmr::get_default_resource();
    auto* dispatcher = test_spaces<on_disk>::get().dispatcher();
    auto session = duck_charmer::session_id_t();
    dispatcher->create_database(session, database_name);
    dispatcher->create_collection(session, database_name, collection_name);
    std::pmr::vector<document_ptr> docs(resource);
    for (int i = 1; i <= size_collection; ++i) {
        docs.push_back(gen_doc(i));
    }
    dispatcher->insert_many(session, database_name, collection_name, docs);
}

template <bool on_disk>
void create_index(const collection_name_t& collection_name) {
    auto* dispatcher = test_spaces<on_disk>::get().dispatcher();
    auto session = duck_charmer::session_id_t();
    components::ql::create_index_t ql{database_name, collection_name, components::ql::index_type::single};
    ql.keys_.emplace_back("count");
    dispatcher->create_index(session, ql);
}

template <bool on_disk>
void init_spaces() {
    init_collection<on_disk>(collection_name_without_index);
    init_collection<on_disk>(collection_name_with_index);
    create_index<on_disk>(collection_name_with_index);
}

template <bool on_disk>
duck_charmer::wrapper_dispatcher_t* wr_dispatcher() {
    return test_spaces<on_disk>::get().dispatcher();
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
components::ql::aggregate_statement_raw_ptr create_aggregate(const collection_name_t &collection_name,
                                                             components::expressions::compare_type compare = components::expressions::compare_type::eq,
                                                             const std::string &key = {}, T value = T()) {
    auto* aggregate = new components::ql::aggregate_statement(database_name, collection_name);
    components::ql::aggregate::match_t match;
    if (!key.empty()) {
        aggregate->add_parameter(core::parameter_id_t{1}, value);
        match.query = components::expressions::make_compare_expression(actor_zeta::detail::pmr::get_default_resource(), compare,
                                                                       components::expressions::key_t{key}, core::parameter_id_t{1});
    }
    aggregate->append(components::ql::aggregate::operator_type::match, match);
    return aggregate;
}

//template<class T>
//document::retained_t<mutable_dict_t> make_dict(const std::string& field, const std::string& key, T value) {
//    auto key_value = mutable_dict_t::new_dict();
//    key_value->set(key, value);
//    auto cond = mutable_dict_t::new_dict();
//    cond->set(field, key_value);
//    return cond;
//}
//
//template<class T>
//document_ptr make_condition(const std::string& field, const std::string& key, T value) {
//    auto dict = make_dict(field, key, value);
//    return make_document(dict);
//}
//
//inline document::retained_t<mutable_dict_t> make_dict(const std::string& aggregate, const std::list<document::retained_t<mutable_dict_t>> &sub_dict) {
//    auto dict = mutable_dict_t::new_dict();
//    auto array = mutable_array_t::new_array();
//    for (const auto& sub_cond : sub_dict) {
//        array->append(sub_cond);
//    }
//    dict->set(aggregate, array);
//    return dict;
//}
//
//inline document_ptr make_condition(const std::string& aggregate, const std::list<document::retained_t<mutable_dict_t>> &sub_conditions) {
//    auto dict = make_dict(aggregate, sub_conditions);
//    return make_document(dict);
//}
