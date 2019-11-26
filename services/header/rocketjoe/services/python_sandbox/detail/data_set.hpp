#pragma once

#include <deque>

#include <pybind11/pybind11.h>
#include <pybind11/functional.h>
#include <pybind11/stl.h>

#include <rocketjoe/services/python_sandbox/detail/forward.hpp>
#include <nlohmann/json.hpp>

#include <boost/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>

PYBIND11_DECLARE_HOLDER_TYPE(T, boost::intrusive_ptr<T>);

namespace rocketjoe { namespace services { namespace python_sandbox { namespace detail {

                using namespace pybind11::literals;
                namespace py = pybind11;

                class pipelien_data_set;

                template <typename T>
                using ref_counter = boost::intrusive_ref_counter<T, boost::thread_safe_counter>;

                using  boost::intrusive_ptr;

                class data_set :  public ref_counter<data_set> {
                public:
                    data_set(const data_set&) = delete;

                    data_set&operator = (const data_set&) = delete;

                    data_set(const py::object &, context *);

                    data_set();

                    virtual ~data_set() = default;

                    auto map(py::function, bool preservesPartitioning = false) -> intrusive_ptr<pipelien_data_set>;

                    auto reduce_by_key(py::function, bool preservesPartitioning = false) -> intrusive_ptr<pipelien_data_set>;

                    auto flat_map(py::function, bool preservesPartitioning = false) -> intrusive_ptr<pipelien_data_set>;

                    auto collect() -> py::list;

                    auto map_partitions_with_index(py::function, bool preservesPartitioning = false) -> intrusive_ptr<pipelien_data_set>;

                    auto map_partitions(py::function, bool preservesPartitioning = false) -> intrusive_ptr<pipelien_data_set>;

                    auto ctx() -> context*;

                protected:
                    py::object collection_;
                    context *ctx_;
                private:
                    bool serializable_;
                    nlohmann::json json_data_set_;
                };


                class pipelien_data_set final : public data_set {
                public:
                        pipelien_data_set(data_set*, py::function, bool);

                private:
                    py::function f_;

                };

}}}}