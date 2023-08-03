#pragma once

#include "document.hpp"
#include "serializer/msgpack_reader.hpp"
#include "serializer/msgpack_writer.hpp"
#include "serializer/output_adapters.hpp"
#include "serializer/sax.hpp"


namespace components::document {

    std::vector<std::uint8_t> to_msgpack(const document_ptr& j);
    void to_msgpack(const document_ptr& j, output_adapter<std::uint8_t> o);
    void to_msgpack(const document_ptr& j, output_adapter<char> o);
    document_ptr from_msgpack(std::vector<std::uint8_t> msgpackBinaryArray);

    document_ptr from_json(const std::string &json);
    std::string to_json(const document_ptr &doc);
    std::string serialize_document(const document_ptr &document);
    document_ptr deserialize_document(const std::string &text);


} // namespace components::document