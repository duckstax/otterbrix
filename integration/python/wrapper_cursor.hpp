#pragma once
#include <components/cursor/cursor.hpp>
#include <components/session/session.hpp>

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

    wrapper_cursor(components::session::session_id_t session, pointer cursor);

    void close();
    bool has_next();
    wrapper_cursor &next();
    wrapper_cursor &iter();
    std::size_t size();
    py::object get(py::object key);
    std::string print();
    wrapper_cursor &sort(py::object sorter, py::object order);

    //paginate();
    //_order();

private:
    std::atomic_bool close_;
    duck_charmer::session_id_t session_;
    pointer ptr_;
    actor_zeta::address_t dispatcher_;

    py::object get_(const std::string &key) const;
    py::object get_(std::size_t index) const;
};


using wrapper_cursor_ptr = boost::intrusive_ptr<wrapper_cursor>;
