#include "wrapper_result.hpp"
#include "convert.hpp"

namespace duck_charmer {

wrapper_result_delete::wrapper_result_delete(const result_delete &src)
    : deleted_ids_(to_pylist(src.deleted_ids())) {
}

const py::list &wrapper_result_delete::raw_result() const {
    return deleted_ids_;
}

std::size_t wrapper_result_delete::deleted_count() const {
    return deleted_ids_.size();
}

}