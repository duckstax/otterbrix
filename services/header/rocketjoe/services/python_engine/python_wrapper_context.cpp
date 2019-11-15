#include <rocketjoe/services/python_engine/detail/python_wrapper_context.hpp>
#include <rocketjoe/services/python_engine/detail/python_wrapper_data_set.hpp>
#include <rocketjoe/services/python_engine/detail/mapreduce_context.hpp>
#include <rocketjoe/services/python_engine/detail/file_manager.hpp>


#include <pybind11/pybind11.h>
#include <pybind11/functional.h>
#include <pybind11/stl.h>

namespace rocketjoe { namespace services { namespace python_engine { namespace detail {

                using namespace pybind11::literals;
                namespace py = pybind11;

                python_wrapper_context::python_wrapper_context(const std::string &name, mapreduce_context *ctx)
                        : name_(name), ctx_(ctx) {
                }

                auto python_wrapper_context::text_file(const std::string &path) -> python_wrapper_data_set {
                    auto *file = ctx_->open_file(path);
                    auto all_file = file->raw_file();
                    return python_wrapper_data_set(py::str(all_file.to_string()), ctx_);
                }

                python_wrapper_context::python_wrapper_context(mapreduce_context *ctx)
                    : ctx_(ctx) {

                }

}}}}