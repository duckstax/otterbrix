#pragma once

#include "document.hpp"
#include "serializer/msgpack_reader.hpp"
#include "serializer/msgpack_writer.hpp"
#include "serializer/output_adapters.hpp"
#include "serializer/sax.hpp"


namespace components::document {

    template<typename InputType>
    bool from_msgpack(InputType&& i ) {
        const bool strict = true;
        const bool allow_exceptions = true;
        auto result = make_document();

        json_sax_dom_parser<document_t> sdp(result, allow_exceptions);
        auto ia = input_adapter(std::forward<InputType>(i));
        const bool res = binary_reader<document_t, decltype(ia)>(std::move(ia)).sax_parse(&sdp, strict);
        return res;
    }

    template<typename IteratorType>
    bool from_msgpack(IteratorType first, IteratorType last) {
        constexpr static bool strict = true;
        constexpr static bool allow_exceptions = true;
        auto result = make_document();

        json_sax_dom_parser<document_view_t> sdp(result, allow_exceptions);
        auto ia = input_adapter(std::move(first), std::move(last));
        const bool res = binary_reader<document_view_t, decltype(ia)>(std::move(ia)).sax_parse(&sdp, strict);
        return res;
    }

    std::vector<std::uint8_t> to_msgpack(const document_ptr& j);
    void to_msgpack(const document_ptr& j, output_adapter<std::uint8_t> o);
    void to_msgpack(const document_ptr& j, output_adapter<char> o);

    document_ptr from_json(const std::string &json);
    std::string to_json(const document_ptr &doc);
    std::string serialize_document(const document_ptr &document);
    document_ptr deserialize_document(const std::string &text);


} // namespace components::document