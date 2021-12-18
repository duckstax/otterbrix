#include "wrapper_result.hpp"
#include "convert.hpp"

namespace duck_charmer {

wrapper_result_delete::wrapper_result_delete(const result_delete &src)
    : deleted_ids_(to_pylist(src.deleted_ids())) {
}

const py::list &wrapper_result_delete::raw_result() const {
    //todo
    return deleted_ids_;
}

std::size_t wrapper_result_delete::deleted_count() const {
    return deleted_ids_.size();
}


wrapper_result_update::wrapper_result_update(const result_update &src)
    : result(src) {
}

py::list wrapper_result_update::raw_result() const {
    return to_pylist(result.modified_ids()) + to_pylist(result.nomodified_ids());
}

std::size_t wrapper_result_update::matched_count() const {
    return result.modified_ids().size() + result.nomodified_ids().size();;
}

std::size_t wrapper_result_update::modified_count() const {
    return result.modified_ids().size();
}

py::object wrapper_result_update::upserted_id() const {
    if (result.upserted_id().empty()) {
        py::none();
    }
    return py::str(result.upserted_id());
}

}