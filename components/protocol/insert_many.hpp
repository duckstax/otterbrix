#pragma once

#include "base.hpp"

struct insert_many_t : statement_t {

    insert_many_t(const std::string& database, const std::string& collection, std::list<components::document::document_t> documents)
        : statement_t(statement_type::insert_many, documents.size() + 1, database, collection)
        , documents_(std::move(documents)){};

    std::list<components::document::document_t> documents_;
};

template<typename Stream>
void pack(msgpack::packer<Stream>& o, insert_many_t const& data)  {
    o.pack_char(static_cast<char>(data.type_));
    o.pack(data.database_);
    o.pack(data.collection_);
    o.pack_uint32(data.size());
    for (const auto& i : data.documents_) {
        o.pack(i);
    }
}