#pragma once

#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>
#include <components/document/mutable/mutable_value.hpp>
#include <components/document/support/ref_counted.hpp>
#include <components/document/document_id.hpp>
#include "document_traits.hpp"

namespace document::impl {
    class dict_iterator_t;
}

namespace components::document {

    class document_view_t;

    using document_value_t = ::document::retained_t<::document::impl::value_t>;
    using document_const_value_t = ::document::retained_const_t<::document::impl::value_t>;

    class document_t final : public boost::intrusive_ref_counter<document_t> {
    public:
        using ptr = boost::intrusive_ptr<document_t>;
        using traits = type_traits;

        document_t();
        explicit document_t(document_value_t value);

        template <class T>
        void set(const std::string &key, T value);

        bool update(const ptr& update);

    private:
        document_value_t value_;

        void set_(const std::string &key, const document_const_value_t& value);
        void set_(std::string_view key, const document_const_value_t& value);

        friend class document_view_t;
    };

    using document_ptr = document_t::ptr;

    document_ptr make_document();
    document_ptr make_document(const ::document::impl::dict_t *dict);
    document_ptr make_document(const ::document::impl::array_t *array);
    document_ptr make_document(const ::document::impl::value_t *value);

    template <class T>
    document_ptr make_document(const std::string &key, T value);

    document_ptr make_upsert_document(const document_ptr& source);

    document_id_t get_document_id(const document_ptr &document);

    document_ptr document_from_json(const std::string &json);
    std::string document_to_json(const document_ptr &doc);


    template<class T>
    void document_t::set(const std::string& key, T value) {
        set_(key, ::document::impl::new_value(value));
    }

    template<>
    inline void document_t::set(const std::string& key, const std::string &value) {
        set_(key, ::document::impl::new_value(value));
    }

    template<>
    inline void document_t::set(const std::string& key, std::string_view value) {
        set_(key, ::document::impl::new_value(value));
    }

    template<>
    inline void document_t::set(const std::string& key, document_const_value_t value) {
        set_(key, value);
    }

    template <class T>
    document_ptr make_document(const std::string &key, T value) {
        auto document = make_document();
        document->set(key, value);
        return document;
    }

    bool is_equals_documents(const document_ptr &doc1, const document_ptr &doc2);

}
