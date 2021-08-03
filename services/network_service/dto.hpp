#pragma once

#include <iterator>
#include <list>
#include <sstream>
#include <string>
#include <vector>

#include <boost/beast/http.hpp>

#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>

#include <msgpack.hpp>

#include "tracy/tracy.hpp"

namespace beast = boost::beast; // from <boost/beast.hpp>
namespace http = beast::http;   // from <boost/beast/http.hpp>

using response_t = http::response<http::string_body>;
using request_t = http::request<http::string_body>;

namespace ws_message_type {
    using bindata_t = std::vector<unsigned char>;
    using strdata_t = std::string;
} // namespace ws_message_type

using ws_binmessages_t = std::list<ws_message_type::bindata_t>;
using ws_strmessages_t = std::list<ws_message_type::strdata_t>;

// public interface
class ws_message_t : public boost::intrusive_ref_counter<ws_message_t> {
public:
    ws_message_t() = default;
    ws_message_t(ws_message_t&&) = default;
    ws_message_t(const ws_message_t&) = default;
    virtual ~ws_message_t() = default;

    auto data() const -> const void* {
        return data_impl();
    }
    auto size() const -> size_t {
        return size_impl();
    }
    auto is_text() const -> bool {
        return is_text_impl();
    }

protected:
    // https://www.boost.org/doc/libs/1_75_0/doc/html/boost_asio/reference/buffer.html
    virtual auto data_impl() const -> const void* = 0;
    virtual auto size_impl() const -> size_t = 0;
    virtual auto is_text_impl() const -> bool = 0;
};

using ws_message_ptr = boost::intrusive_ptr<ws_message_t>;

template<class message_type_t>
class ws_message_base_t : public ws_message_t {
public:
    ws_message_base_t(std::stringstream& ss)
        : ws_message_t() {
        ZoneScoped;
        auto bof = ss.tellg();
        ss.seekg(0, std::ios::end);
        auto stream_size = std::size_t(ss.tellg() - bof);
        ss.seekg(0, std::ios::beg);
        message_.resize(stream_size);
        ss.read((char*) message_.data(), std::streamsize(message_.size()));
    }

    ws_message_base_t(msgpack::sbuffer& sbuffer)
        : ws_message_t()
        , message_(sbuffer.data(), sbuffer.data() + sbuffer.size()) { ZoneScoped; }

    ws_message_base_t(message_type_t&& message)
        : ws_message_t()
        , message_(std::move(message)) { ZoneScoped; }

    ws_message_base_t(ws_message_base_t&&) = default;
    ws_message_base_t(const ws_message_base_t&) = default;
    virtual ~ws_message_base_t() = default;

    auto data_impl() const -> const void* {
        return static_cast<const void*>(message_.data());
    }
    auto size_impl() const -> size_t {
        return message_.size();
    }
    auto is_text_impl() const -> bool {
        return std::is_same<message_type_t, ws_message_type::strdata_t>::value;
    }

private:
    message_type_t message_;
};

using ws_string_message_t = ws_message_base_t<ws_message_type::strdata_t>;
using ws_binary_message_t = ws_message_base_t<ws_message_type::bindata_t>;

/*class ws_string_message_t final : public ws_message_base_t<ws_message_type::strdata_t> {
public:
    ws_string_message_t(ws_message_type::strdata_t&& message)
        : ws_message_base_t<ws_message_type::strdata_t>(std::move(message)) { ZoneScoped; }
    ws_string_message_t(std::stringstream& ss)
        : ws_message_base_t<ws_message_type::strdata_t>(ss) { ZoneScoped; }
    virtual ~ws_string_message_t() = default;
};
class ws_binary_message_t final : public ws_message_base_t<ws_message_type::bindata_t> {
public:
    ws_binary_message_t(ws_message_type::bindata_t&& message)
        : ws_message_base_t<ws_message_type::bindata_t>(std::move(message)) { ZoneScoped; }
    ws_binary_message_t(std::stringstream& ss)
        : ws_message_base_t<ws_message_type::bindata_t>(ss) {}
    virtual ~ws_binary_message_t() = default;
};*/

void make_http_client_request(
    const std::string& host,
    const std::string& target,
    http::verb method,
    request_t& request);
