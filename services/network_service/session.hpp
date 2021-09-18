#pragma once
#include "dto.hpp"

class http_session_abstract_t {
public:
    void write(response_t response) {
        ZoneScoped;
        write_impl(std::move(response));
    }

    virtual ~http_session_abstract_t() = default;

protected:
    virtual void write_impl(response_t response) = 0;
};

class ws_session_abstract_t {
public:
    void write(ws_message_ptr message) {
        ZoneScoped;
        write_impl(message);
    }

    void close() {
        ZoneScoped;
        close_impl();
    }

    bool is_close() {
        ZoneScoped;
        return is_close_impl();
    }

    virtual ~ws_session_abstract_t() = default;

protected:
    virtual void write_impl(ws_message_ptr) = 0;
    virtual void close_impl() = 0;
    virtual bool is_close_impl() = 0;
};