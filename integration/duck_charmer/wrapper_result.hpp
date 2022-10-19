#pragma once
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <pybind11/stl_bind.h>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>
#include "services/collection/result.hpp"

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


class PYBIND11_EXPORT wrapper_result_update final : public boost::intrusive_ref_counter<wrapper_result_update> {
public:
    wrapper_result_update() = default;
    wrapper_result_update(const result_update &src);

    py::list raw_result() const;
    std::size_t matched_count() const;
    std::size_t modified_count() const;
    py::object upserted_id() const;

private:
    result_update result;
};

}