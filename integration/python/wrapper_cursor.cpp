#include "wrapper_cursor.hpp"
#include "convert.hpp"
#include "integration/cpp/route.hpp"

// The bug related to the use of RTTI by the pybind11 library has been fixed: a
// declaration should be in each translation unit.
PYBIND11_DECLARE_HOLDER_TYPE(T, boost::intrusive_ptr<T>)

wrapper_cursor::wrapper_cursor(components::session::session_id_t session, wrapper_cursor::pointer cursor)
        : session_(session)
        , ptr_(cursor)
        , dispatcher_(actor_zeta::address_t::empty_address()) {
}
void wrapper_cursor::close() {
    close_ = true;
}

bool wrapper_cursor::has_next() {
    return ptr_->has_next();
}

wrapper_cursor &wrapper_cursor::next() {
    if (!ptr_->next()) {
        throw py::stop_iteration();
    }
    return *this;
}

wrapper_cursor &wrapper_cursor::iter() {
    return *this;
}

std::size_t wrapper_cursor::size() {
    return ptr_->size();
}

py::object wrapper_cursor::get(py::object key) {
    if (py::isinstance<py::str>(key)) {
        return get_(key.cast<std::string>());
    }
    if (py::isinstance<py::int_>(key)) {
        return get_(key.cast<std::size_t>());
    }
    return py::object();
}

std::string wrapper_cursor::print() {
    return ptr_->get()->to_json();
}

wrapper_cursor &wrapper_cursor::sort(py::object sorter, py::object order) {
    if (py::isinstance<py::dict>(sorter)) {
        ptr_->sort(to_sorter(sorter));
    } else {
        ptr_->sort(services::storage::sort::sorter_t(py::str(sorter).cast<std::string>(), to_order(order)));
    }
    return *this;
}

py::object wrapper_cursor::get_(const std::string &key) const {
    return from_object(*ptr_->get(), key);
}

py::object wrapper_cursor::get_(std::size_t index) const {
    return from_document(*ptr_->get(index));
}
