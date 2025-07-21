#include "wrapper_cursor.hpp"
#include "convert.hpp"

// The bug related to the use of RTTI by the pybind11 library has been fixed: a
// declaration should be in each translation unit.
PYBIND11_DECLARE_HOLDER_TYPE(T, boost::intrusive_ptr<T>)

wrapper_cursor::wrapper_cursor(pointer cursor, otterbrix::wrapper_dispatcher_t* dispatcher)
    : ptr_(std::move(cursor))
    , dispatcher_(dispatcher) {}
void wrapper_cursor::close() { close_ = true; }

bool wrapper_cursor::has_next() { return ptr_->has_next(); }

wrapper_cursor& wrapper_cursor::next() {
    if (!ptr_->next()) {
        throw py::stop_iteration();
    }
    return *this;
}

wrapper_cursor& wrapper_cursor::iter() { return *this; }

std::size_t wrapper_cursor::size() { return ptr_->size(); }

py::object wrapper_cursor::get(py::object key) {
    if (py::isinstance<py::str>(key)) {
        return get_(key.cast<std::string>());
    }
    if (py::isinstance<py::int_>(key)) {
        return get_(key.cast<std::size_t>());
    }
    return py::object();
}

bool wrapper_cursor::is_success() const noexcept { return ptr_->is_success(); }

bool wrapper_cursor::is_error() const noexcept { return ptr_->is_error(); }

py::tuple wrapper_cursor::get_error() const {
    using error_code_t = components::cursor::error_code_t;

    py::str type;
    switch (ptr_->get_error().type) {
        case error_code_t::none:
            type = "none";
            break;

        case error_code_t::database_already_exists:
            type = "database_already_exists";
            break;

        case error_code_t::database_not_exists:
            type = "database_not_exists";
            break;

        case error_code_t::collection_already_exists:
            type = "collection_already_exists";
            break;

        case error_code_t::collection_not_exists:
            type = "collection_not_exists";
            break;

        case error_code_t::collection_dropped:
            type = "collection_dropped";
            break;

        case error_code_t::sql_parse_error:
            type = "sql_parse_error";
            break;

        case error_code_t::create_phisical_plan_error:
            type = "create_phisical_plan_error";
            break;

        case error_code_t::other_error:
            type = "other_error";
            break;

        default:
            break;
    }
    return py::make_tuple(type, ptr_->get_error().what);
}

std::string wrapper_cursor::print() { return std::string(ptr_->get()->to_json()); }

wrapper_cursor& wrapper_cursor::sort(py::object sorter, py::object order) {
    if (py::isinstance<py::dict>(sorter)) {
        ptr_->sort(to_sorter(sorter));
    } else {
        ptr_->sort(components::collection::sort::sorter_t(py::str(sorter).cast<std::string>(), to_order(order)));
    }
    return *this;
}

void wrapper_cursor::execute(std::string& query) {
    ptr_ = dispatcher_->execute_sql(components::session::session_id_t(), query);
}

py::object wrapper_cursor::get_(const std::string& key) const { return from_object(ptr_->get(), key); }

py::object wrapper_cursor::get_(std::size_t index) const { return from_document(ptr_->get(index)); }
