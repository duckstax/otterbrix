#pragma once
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <pybind11/stl_bind.h>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>
#include "services/storage/result.hpp"

namespace py = pybind11;

namespace duck_charmer {

class PYBIND11_EXPORT wrapper_result_delete final : public boost::intrusive_ref_counter<wrapper_result_delete> {
public:
    wrapper_result_delete() = default;
    wrapper_result_delete(const result_delete &src);

    const py::list &raw_result() const;
    std::size_t deleted_count() const;

private:
    py::list deleted_ids_;
};

}