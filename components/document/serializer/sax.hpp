#pragma once

#include <cassert>
#include <cstddef>
#include <cstdint>

#include <string>
#include <utility>
#include <vector>

#include "utils.hpp"

template<typename BasicJsonType>
class json_sax_dom_parser {
public:
    using number_integer_t = typename BasicJsonType::number_integer_t;
    using number_unsigned_t = typename BasicJsonType::number_unsigned_t;
    using number_float_t = typename BasicJsonType::number_float_t;
    using string_t = typename BasicJsonType::string_t;
    using binary_t = typename BasicJsonType::binary_t;

    explicit json_sax_dom_parser(BasicJsonType& r, const bool allow_exceptions_ = true)
        : root(r)
        , allow_exceptions(allow_exceptions_) {}

    json_sax_dom_parser(const json_sax_dom_parser&) = delete;
    json_sax_dom_parser(json_sax_dom_parser&&) = default;
    json_sax_dom_parser& operator=(const json_sax_dom_parser&) = delete;
    json_sax_dom_parser& operator=(json_sax_dom_parser&&) = default;
    ~json_sax_dom_parser() = default;

    bool null() {
        handle_value(nullptr);
        return true;
    }

    bool boolean(bool val) {
        handle_value(val);
        return true;
    }

    bool number_integer(number_integer_t val) {
        handle_value(val);
        return true;
    }

    bool number_unsigned(number_unsigned_t val) {
        handle_value(val);
        return true;
    }

    bool number_float(number_float_t val, const string_t& /*unused*/) {
        handle_value(val);
        return true;
    }

    bool string(string_t& val) {
        handle_value(val);
        return true;
    }

    bool binary(binary_t& val) {
        handle_value(std::move(val));
        return true;
    }

    bool start_object(std::size_t len) {
        ref_stack.push_back(handle_value(BasicJsonType::value_t::object));

        if (JSON_HEDLEY_UNLIKELY(len != static_cast<std::size_t>(-1) && len > ref_stack.back()->max_size())) {
            JSON_THROW(out_of_range::create(408, concat("excessive object size: ", std::to_string(len)), ref_stack.back()));
        }

        return true;
    }

    bool key(string_t& val) {
        assert(!ref_stack.empty());
        assert(ref_stack.back()->is_object());

        // add null at given key and store the reference for later
        object_element = &(ref_stack.back()->m_value.object->operator[](val));
        return true;
    }

    bool end_object() {
        assert(!ref_stack.empty());
        assert(ref_stack.back()->is_object());

        ref_stack.back()->set_parents();
        ref_stack.pop_back();
        return true;
    }

    bool start_array(std::size_t len) {
        ref_stack.push_back(handle_value(BasicJsonType::value_t::array));

        if (JSON_HEDLEY_UNLIKELY(len != static_cast<std::size_t>(-1) && len > ref_stack.back()->max_size())) {
            JSON_THROW(out_of_range::create(408, concat("excessive array size: ", std::to_string(len)), ref_stack.back()));
        }

        return true;
    }

    bool end_array() {
        assert(!ref_stack.empty());
        assert(ref_stack.back()->is_array());

        ref_stack.back()->set_parents();
        ref_stack.pop_back();
        return true;
    }

    template<class Exception>
    bool parse_error(std::size_t /*unused*/, const std::string& /*unused*/,
                     const Exception& ex) {
        errored = true;
        static_cast<void>(ex);
        if (allow_exceptions) {
            JSON_THROW(ex);
        }
        return false;
    }

    constexpr bool is_errored() const {
        return errored;
    }

private:
    template<typename Value>
    BasicJsonType*
    handle_value(Value&& v) {
        if (ref_stack.empty()) {
            root = BasicJsonType(std::forward<Value>(v));
            return &root;
        }

        assert(ref_stack.back()->is_array() || ref_stack.back()->is_object());

        if (ref_stack.back()->is_array()) {
            ref_stack.back()->m_value.array->emplace_back(std::forward<Value>(v));
            return &(ref_stack.back()->m_value.array->back());
        }

        assert(ref_stack.back()->is_object());
        assert(object_element);
        *object_element = BasicJsonType(std::forward<Value>(v));
        return object_element;
    }

    BasicJsonType& root;
    std::vector<BasicJsonType*> ref_stack{};
    BasicJsonType* object_element = nullptr;
    bool errored = false;
    const bool allow_exceptions = true;
};

template<typename BasicJsonType>
class json_sax_dom_callback_parser {
public:
    using number_integer_t = typename BasicJsonType::number_integer_t;
    using number_unsigned_t = typename BasicJsonType::number_unsigned_t;
    using number_float_t = typename BasicJsonType::number_float_t;
    using string_t = typename BasicJsonType::string_t;
    using binary_t = typename BasicJsonType::binary_t;
    using parser_callback_t = typename BasicJsonType::parser_callback_t;
    using parse_event_t = typename BasicJsonType::parse_event_t;

    json_sax_dom_callback_parser(BasicJsonType& r,
                                 const parser_callback_t cb,
                                 const bool allow_exceptions_ = true)
        : root(r)
        , callback(cb)
        , allow_exceptions(allow_exceptions_) {
        keep_stack.push_back(true);
    }

    // make class move-only
    json_sax_dom_callback_parser(const json_sax_dom_callback_parser&) = delete;
    json_sax_dom_callback_parser(json_sax_dom_callback_parser&&) = default;
    json_sax_dom_callback_parser& operator=(const json_sax_dom_callback_parser&) = delete;
    json_sax_dom_callback_parser& operator=(json_sax_dom_callback_parser&&) = default;
    ~json_sax_dom_callback_parser() = default;

    bool null() {
        handle_value(nullptr);
        return true;
    }

    bool boolean(bool val) {
        handle_value(val);
        return true;
    }

    bool number_integer(number_integer_t val) {
        handle_value(val);
        return true;
    }

    bool number_unsigned(number_unsigned_t val) {
        handle_value(val);
        return true;
    }

    bool number_float(number_float_t val, const string_t& /*unused*/) {
        handle_value(val);
        return true;
    }

    bool string(string_t& val) {
        handle_value(val);
        return true;
    }

    bool binary(binary_t& val) {
        handle_value(std::move(val));
        return true;
    }

    bool start_object(std::size_t len) {
        // check callback for object start
        const bool keep = callback(static_cast<int>(ref_stack.size()), parse_event_t::object_start, discarded);
        keep_stack.push_back(keep);

        auto val = handle_value(BasicJsonType::value_t::object, true);
        ref_stack.push_back(val.second);

        // check object limit
        if (ref_stack.back() && JSON_HEDLEY_UNLIKELY(len != static_cast<std::size_t>(-1) && len > ref_stack.back()->max_size())) {
            JSON_THROW(out_of_range::create(408, concat("excessive object size: ", std::to_string(len)), ref_stack.back()));
        }

        return true;
    }

    bool key(string_t& val) {
        BasicJsonType k = BasicJsonType(val);

        // check callback for key
        const bool keep = callback(static_cast<int>(ref_stack.size()), parse_event_t::key, k);
        key_keep_stack.push_back(keep);

        // add discarded value at given key and store the reference for later
        if (keep && ref_stack.back()) {
            object_element = &(ref_stack.back()->m_value.object->operator[](val) = discarded);
        }

        return true;
    }

    bool end_object() {
        if (ref_stack.back()) {
            if (!callback(static_cast<int>(ref_stack.size()) - 1, parse_event_t::object_end, *ref_stack.back())) {
                // discard object
                *ref_stack.back() = discarded;
            } else {
                ref_stack.back()->set_parents();
            }
        }

        assert(!ref_stack.empty());
        assert(!keep_stack.empty());
        ref_stack.pop_back();
        keep_stack.pop_back();

        if (!ref_stack.empty() && ref_stack.back() && ref_stack.back()->is_structured()) {
            // remove discarded value
            for (auto it = ref_stack.back()->begin(); it != ref_stack.back()->end(); ++it) {
                if (it->is_discarded()) {
                    ref_stack.back()->erase(it);
                    break;
                }
            }
        }

        return true;
    }

    bool start_array(std::size_t len) {
        const bool keep = callback(static_cast<int>(ref_stack.size()), parse_event_t::array_start, discarded);
        keep_stack.push_back(keep);

        auto val = handle_value(BasicJsonType::value_t::array, true);
        ref_stack.push_back(val.second);

        // check array limit
        if (ref_stack.back() && JSON_HEDLEY_UNLIKELY(len != static_cast<std::size_t>(-1) && len > ref_stack.back()->max_size())) {
            JSON_THROW(out_of_range::create(408, concat("excessive array size: ", std::to_string(len)), ref_stack.back()));
        }

        return true;
    }

    bool end_array() {
        bool keep = true;

        if (ref_stack.back()) {
            keep = callback(static_cast<int>(ref_stack.size()) - 1, parse_event_t::array_end, *ref_stack.back());
            if (keep) {
                ref_stack.back()->set_parents();
            } else {
                // discard array
                *ref_stack.back() = discarded;
            }
        }

        assert(!ref_stack.empty());
        assert(!keep_stack.empty());
        ref_stack.pop_back();
        keep_stack.pop_back();

        // remove discarded value
        if (!keep && !ref_stack.empty() && ref_stack.back()->is_array()) {
            ref_stack.back()->m_value.array->pop_back();
        }

        return true;
    }

    template<class Exception>
    bool parse_error(std::size_t /*unused*/, const std::string& /*unused*/, const Exception& ex) {
        errored = true;
        static_cast<void>(ex);
        if (allow_exceptions) {
            JSON_THROW(ex);
        }
        return false;
    }

    constexpr bool is_errored() const {
        return errored;
    }

private:

    template<typename Value>
    std::pair<bool, BasicJsonType*> handle_value(Value&& v, const bool skip_callback = false) {
        assert(!keep_stack.empty());

        if (!keep_stack.back()) {
            return {false, nullptr};
        }

        auto value = BasicJsonType(std::forward<Value>(v));

        const bool keep = skip_callback || callback(static_cast<int>(ref_stack.size()), parse_event_t::value, value);

        if (!keep) {
            return {false, nullptr};
        }

        if (ref_stack.empty()) {
            root = std::move(value);
            return {true, &root};
        }

        if (!ref_stack.back()) {
            return {false, nullptr};
        }

        assert(ref_stack.back()->is_array() || ref_stack.back()->is_object());

        if (ref_stack.back()->is_array()) {
            ref_stack.back()->m_value.array->emplace_back(std::move(value));
            return {true, &(ref_stack.back()->m_value.array->back())};
        }


        assert(ref_stack.back()->is_object());
        assert(!key_keep_stack.empty());
        const bool store_element = key_keep_stack.back();
        key_keep_stack.pop_back();

        if (!store_element) {
            return {false, nullptr};
        }

        assert(object_element);
        *object_element = std::move(value);
        return {true, object_element};
    }

    BasicJsonType& root;
    std::vector<BasicJsonType*> ref_stack{};
    std::vector<bool> keep_stack{};
    std::vector<bool> key_keep_stack{};
    BasicJsonType* object_element = nullptr;
    bool errored = false;
    const parser_callback_t callback = nullptr;
    const bool allow_exceptions = true;
    BasicJsonType discarded = BasicJsonType::value_t::discarded;
};

template<typename BasicJsonType>
class json_sax_acceptor {
public:
    using number_integer_t = typename BasicJsonType::number_integer_t;
    using number_unsigned_t = typename BasicJsonType::number_unsigned_t;
    using number_float_t = typename BasicJsonType::number_float_t;
    using string_t = typename BasicJsonType::string_t;
    using binary_t = typename BasicJsonType::binary_t;

    bool null() {
        return true;
    }

    bool boolean(bool /*unused*/) {
        return true;
    }

    bool number_integer(number_integer_t /*unused*/) {
        return true;
    }

    bool number_unsigned(number_unsigned_t /*unused*/) {
        return true;
    }

    bool number_float(number_float_t /*unused*/, const string_t& /*unused*/) {
        return true;
    }

    bool string(string_t& /*unused*/) {
        return true;
    }

    bool binary(binary_t& /*unused*/) {
        return true;
    }

    bool start_object(std::size_t /*unused*/ = static_cast<std::size_t>(-1)) {
        return true;
    }

    bool key(string_t& /*unused*/) {
        return true;
    }

    bool end_object() {
        return true;
    }

    bool start_array(std::size_t /*unused*/ = static_cast<std::size_t>(-1)) {
        return true;
    }

    bool end_array() {
        return true;
    }

    bool parse_error(std::size_t /*unused*/, const std::string& /*unused*/, const detail::exception& /*unused*/) {
        return false;
    }
};