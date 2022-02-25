#pragma once
#include <msgpack.hpp>
#include <absl/container/btree_map.h>
#include <components/document/document.hpp>
#include <components/document/document_id.hpp>
#include "btree_storage/range.hpp"

namespace document::impl {
    class mutable_dict_t;
}

namespace components::btree {

    using document_structure_t = ::document::retained_t<::document::impl::mutable_dict_t>;
    using document_data_t = msgpack::sbuffer;
    using input_document_t = components::document::document_t;
    using document_id_t = document::document_id_t;

    class document_t {
    public:
        document_structure_t structure;
        document_data_t data;

        document_t();
        document_t(document_structure_t structure, const std::string &data);

        bool update(const input_document_t& update);
        void commit();
        void rollback();

    private:
        data_ranges_t removed_data_;
    };

    using document_unique_ptr = std::unique_ptr<document_t>;
    using storage_t = absl::btree_map<document_id_t, document_unique_ptr>;

    document_unique_ptr make_document(const input_document_t &input_document, int version = 0);

}