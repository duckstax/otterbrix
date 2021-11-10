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

namespace py = pybind11;

class PYBIND11_EXPORT  wrapper_cursor  final : public boost::intrusive_ref_counter<wrapper_cursor> {
public:
    using type = components::cursor::cursor_t;
    using pointer = type*;

    wrapper_cursor() = default; // todo refactoring;
    wrapper_cursor(goblin_engineer::actor_address dispatcher, components::session::session_t session, pointer cursor);

    void close();
    bool has_next();
    bool next();
    std::size_t size();
    py::object get(const std::string& key);
    std::string print();

    //paginate();
    //_order();
    //sort();

private:
    std::atomic_bool close_;
    duck_charmer::session_t session_;
    pointer ptr_;
    goblin_engineer::actor_address dispatcher_;

    void d_();
    std::atomic_int i = 0;
    std::mutex mtx_;
    std::condition_variable cv_;
};


using wrapper_cursor_ptr = boost::intrusive_ptr<wrapper_cursor>;
