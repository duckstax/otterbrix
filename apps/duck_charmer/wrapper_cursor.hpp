#pragma once
#include <components/cursor/cursor.hpp>
#include <components/session/session.hpp>

#include <goblin-engineer/core.hpp>

#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>

#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <pybind11/stl_bind.h>

#include "forward.hpp"

class PYBIND11_EXPORT  wrapper_cursor  final : public boost::intrusive_ref_counter<wrapper_cursor> {
public:
    using type = components::cursor::cursor_t;
    using pointer = type*;

    wrapper_cursor() = default; // todo refactoring;
    wrapper_cursor(components::session::session_t session,pointer cursor);
    void close();
    void next();
    std::size_t size() const;

private:
    std::atomic_bool close_;
    duck_charmer::session_t session_;
    std::unique_ptr<type> ptr_;
    goblin_engineer::actor_address dispatcher_;
};


using wrapper_cursor_ptr = boost::intrusive_ptr<wrapper_cursor>;