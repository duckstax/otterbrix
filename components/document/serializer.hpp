#pragma once

#include "document.hpp"
#include "serializer/msgpack_reader.hpp"
#include "serializer/msgpack_writer.hpp"

template<typename CharType, typename StringType = std::basic_string<CharType>>
class output_adapter {
public:
    template<typename AllocatorType = std::allocator<CharType>>
    output_adapter(std::vector<CharType, AllocatorType>& vec)
        : oa(std::make_shared<output_vector_adapter<CharType, AllocatorType>>(vec)) {}

#ifndef JSON_NO_IO
    output_adapter(std::basic_ostream<CharType>& s)
        : oa(std::make_shared<output_stream_adapter<CharType>>(s)) {}
#endif // JSON_NO_IO

    output_adapter(StringType& s)
        : oa(std::make_shared<output_string_adapter<CharType, StringType>>(s)) {}

    operator output_adapter_t<CharType>() {
        return oa;
    }

private:
    output_adapter_t<CharType> oa = nullptr;
};

namespace components::document {

    template<typename InputType>
    bool from_msgpack(InputType&& i ) {
        const bool strict = true;
        const bool allow_exceptions = true;

        json_sax_dom_parser<basic_json> sdp(result, allow_exceptions);
        auto ia = input_adapter(std::forward<InputType>(i));
        const bool res = binary_reader<decltype(ia)>(std::move(ia)).sax_parse(&sdp, strict);
        return res;
    }

    template<typename IteratorType>
    bool from_msgpack(IteratorType first, IteratorType last) {
        constexpr static bool strict = true;
        constexpr static bool allow_exceptions = true;

        json_sax_dom_parser<basic_json> sdp(result, allow_exceptions);
        auto ia = input_adapter(std::move(first), std::move(last));
        const bool res = binary_reader<decltype(ia)>(std::move(ia), ).sax_parse(&sdp, strict);
        return res;
    }

    std::vector<std::uint8_t> to_msgpack(const basic_json& j);
    void to_msgpack(const basic_json& j, output_adapter<std::uint8_t> o);
    void to_msgpack(const basic_json& j, output_adapter<char> o);

    document_ptr from_json(const std::string &json);
    std::string to_json(const document_ptr &doc);
    std::string serialize_document(const document_ptr &document);
    document_ptr deserialize_document(const std::string &text);


} // namespace components::document