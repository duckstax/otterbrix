#include "wrapper_result.hpp"
#include "convert.hpp"

namespace ottergon {

wrapper_result_delete::wrapper_result_delete(const components::result::result_delete &src)
    : deleted_ids_(to_pylist(src.deleted_ids())) {
}

const py::list &wrapper_result_delete::raw_result() const {
    //todo
    return deleted_ids_;
}

std::size_t wrapper_result_delete::deleted_count() const {
    return deleted_ids_.size();
}


wrapper_result_update::wrapper_result_update(const components::result::result_update &src)
    : result(src) {
}

py::list wrapper_result_update::raw_result() const {
    return to_pylist(result.modified_ids()) + to_pylist(result.nomodified_ids());
}

std::size_t wrapper_result_update::matched_count() const {
    return result.modified_ids().size() + result.nomodified_ids().size();
}

std::size_t wrapper_result_update::modified_count() const {
    return result.modified_ids().size();
}

py::object wrapper_result_update::upserted_id() const {
    if (result.upserted_id().is_null()) {
        py::none();
    }
    return py::str(result.upserted_id().to_string());
}


wrapper_result::wrapper_result()
    : result_(components::result::empty_result_t()) {
}

wrapper_result::wrapper_result(const components::session::session_id_t& session, const components::result::result_t& result)
    : session_(session)
    , result_(result) {
}

std::size_t wrapper_result::inserted_count() const {
    if (result_.is_type<components::result::result_insert>()) {
        return result_.get<components::result::result_insert>().inserted_ids().size();
    }
    return 0;
}

std::size_t wrapper_result::modified_count() const {
    if (result_.is_type<components::result::result_update>()) {
        return result_.get<components::result::result_update>().modified_ids().size();
    }
    return 0;
}

std::size_t wrapper_result::deleted_count() const {
    if (result_.is_type<components::result::result_delete>()) {
        return result_.get<components::result::result_delete>().deleted_ids().size();
    }
    return 0;
}

wrapper_cursor_ptr wrapper_result::cursor() const {
    if (result_.is_type<components::cursor::cursor_t*>()) {
        return wrapper_cursor_ptr(new wrapper_cursor(session_, result_.get<components::cursor::cursor_t*>()));
    }
    return wrapper_cursor_ptr();
}

}
