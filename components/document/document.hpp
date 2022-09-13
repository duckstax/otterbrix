#pragma once

#include <msgpack.hpp>
#include <components/document/mutable/mutable_value.hpp>
#include <components/document/support/ref_counted.hpp>
#include <components/document/range.hpp>
#include <components/document/document_id.hpp>

namespace document::impl {
    class dict_t;
    class mutable_dict_t;
}

namespace components::document {

    static constexpr auto key_value_document = "___value___";

    using document_structure_t = ::document::retained_t<::document::impl::mutable_dict_t>;
    using document_data_t = msgpack::sbuffer;
    using document_value_t = ::document::retained_t<::document::impl::value_t>;

    class document_t : public ::document::ref_counted_t {
    public:
        document_structure_t structure;
        document_data_t data;
        document_value_t value_;

        document_t();
        document_t(document_structure_t structure, const document_data_t &data);

        template <class T> void set(const std::string &key, T value);

        bool update(const document_t& update);
        void commit();
        void rollback();

    private:
        data_ranges_t removed_data_;

        void set_(const std::string &key, ::document::retained_const_t<::document::impl::value_t> value);
    };

    using document_ptr = ::document::retained_t<document_t>;

    document_ptr make_document();
    document_ptr make_document(document_structure_t structure, const document_data_t &data);
    document_ptr make_document(const ::document::impl::dict_t *dict, int version = 0);

    document_ptr make_upsert_document(const document_ptr& source);

    document_id_t get_document_id(const document_ptr &document);

    document_ptr document_from_json(const std::string &json);
    std::string document_to_json(const document_ptr &doc);

    msgpack::type::object_type get_msgpack_type(const ::document::impl::value_t *value);
    msgpack::object get_msgpack_object(const ::document::impl::value_t *value);


    template<class T>
    void document_t::set(const std::string& key, T value) {
        set_(key, ::document::impl::new_value(value));
    }

    template<>
    inline void document_t::set(const std::string& key, const std::string &value) {
        set_(key, ::document::impl::new_value(::document::slice_t(value)));
    }

    template<>
    inline void document_t::set(const std::string& key, ::document::retained_const_t<::document::impl::value_t> value) {
        set_(key, std::move(value));
    }

    std::string serialize_document(const document_ptr &document);
    document_ptr deserialize_document(const std::string &text);

}
