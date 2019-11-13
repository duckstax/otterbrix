#include <rocketjoe/services/python_engine/detail/python_wrapper_context.hpp>
#include <rocketjoe/services/python_engine/detail/python_wrapper_data_set.hpp>
#include <rocketjoe/services/python_engine/detail/context.hpp>
#include <rocketjoe/services/python_engine/detail/file_manager.hpp>


#include <pybind11/pybind11.h>
#include <pybind11/functional.h>
#include <pybind11/stl.h>

namespace rocketjoe { namespace services { namespace python_engine { namespace detail {

                using namespace pybind11::literals;
                namespace py = pybind11;

                context_wrapper::context_wrapper(const std::string &name, context *ctx)
                        : name_(name), ctx_(ctx) {
                }

                auto context_wrapper::text_file(const std::string &path) -> python_wrapper_data_set {
                    auto *file = ctx_->open_file(path);
                    auto all_file = file->raw_file();
                    return python_wrapper_data_set(py::str(all_file.to_string()), ctx_);
                }

}}}}