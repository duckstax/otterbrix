#include <rocketjoe/services/python_engine/mapreduce.hpp>

#include <pybind11/functional.h>
#include <pybind11/stl.h>

#include <rocketjoe/services/python_engine/context_manager.h>

namespace rocketjoe { namespace services { namespace python_engine {

            using namespace pybind11::literals;
            namespace py = pybind11;
            using py_task = py::function;

            class data_set_wrapper final {
            public:
                explicit data_set_wrapper(context *ctx) : ctx_(ctx) {}

                auto map(py::function f) -> data_set_wrapper & {
                    auto &current = *ctx_->top();
                    auto &new_set = *(ctx_->next());
                    for (auto &i:current) {
                        auto result = f(i);
                        auto tmp = result.cast<std::string>();
                        new_set.append(tmp);
                    }
                    return *this;
                }

                auto reduce_by_key(py::function f) -> data_set_wrapper & {
                    auto &current = *ctx_->top();
                    auto &new_set = *(ctx_->next());
                    for (auto &i:current) {
                        auto result = f(i);
                        auto tmp = result.cast<std::string>();
                        new_set.append(tmp);
                    }

                    return *this;
                }

                auto flat_map(py::function f) -> data_set_wrapper & {
                    auto tmp = ctx_->top()->file()->raw_file();
                    py::list result = f(tmp.to_string());

                    for (auto &i:result) {
                        ctx_->top()->append(i.cast<std::string>());
                    }

                    ctx_->next();

                    return *this;
                }

                auto collect() -> py::list {

                    py::list tmp{};

                    auto &current = *ctx_->top();
                    for (const auto &i :current) {
                        tmp.append(i.second);
                    }

                    return tmp;
                }

            private:
                context *ctx_;
            };


            class context_wrapper final {
            public:
                context_wrapper(const std::string &name, context *ctx)
                        : name_(name), ctx_(ctx)
                        {
                }

                auto text_file(const std::string &path) -> data_set_wrapper {
                    ctx_->read_file(path);
                    return data_set_wrapper{ctx_};
                }

            private:
                std::string name_;
                context *ctx_;
            };


            auto add_mapreduce(py::module &pyrocketjoe, context_manager *cm_) -> void {

                auto mapreduce_submodule = pyrocketjoe.def_submodule("MapReduce");

                py::class_<context_wrapper>(mapreduce_submodule, "RocketJoeContext")
                        .def(
                                py::init(
                                        [cm_](const std::string &name) {
                                            auto *ctx = cm_->create_context(name);
                                            return new context_wrapper(name, ctx);
                                        }
                                )
                        )
                        .def("textFile", &context_wrapper::text_file);

                py::class_<data_set_wrapper>(mapreduce_submodule, "DataSet")
                        .def("map", &data_set_wrapper::map)
                        .def("reduceByKey", &data_set_wrapper::reduce_by_key)
                        .def("flatMap", &data_set_wrapper::flat_map)
                        .def("collect", &data_set_wrapper::collect);
            }

}}}