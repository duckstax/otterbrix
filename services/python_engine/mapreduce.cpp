#include <rocketjoe/services/python_engine/mapreduce.hpp>

#include <pybind11/functional.h>
#include <pybind11/stl.h>

#include <nlohmann/json.hpp>

#include <rocketjoe/services/python_engine/context_manager.hpp>

namespace rocketjoe { namespace services { namespace python_engine {

            using namespace pybind11::literals;
            namespace py = pybind11;
            using namespace nlohmann;

            json to_json_impl(const py::handle &obj) {
                if (obj.is_none()) {
                    return nullptr;
                } if (py::isinstance<py::bool_>(obj)) {
                    return obj.cast<bool>();
                } if (py::isinstance<py::int_>(obj)) {
                    return obj.cast<long>();
                } if (py::isinstance<py::float_>(obj)) {
                    return obj.cast<double>();
                } if (py::isinstance<py::str>(obj)) {
                    return obj.cast<std::string>();
                } if (py::isinstance<py::tuple>(obj) || py::isinstance<py::list>(obj)) {
                    auto out = json::array();
                    for (const py::handle &value : obj) {
                        out.push_back(to_json_impl(value));
                    }
                    return out;
                } if (py::isinstance<py::dict>(obj)) {
                    auto out = json::object();
                    for (const py::handle &key : obj) {
                        out[py::str(key).cast<std::string>()] = to_json_impl(obj[key]);
                    }
                    return out;
                }
                throw std::runtime_error("to_json not implemented for this type of object: " + obj.cast<std::string>());
            }

            py::object from_json_impl(const json &j) {
                if (j.is_null()) {
                    return py::none();
                } else if (j.is_boolean()) {
                    return py::bool_(j.get<bool>());
                } else if (j.is_number()) {
                    auto number = j.get<double>();
                    if (number == std::floor(number)) {
                        return py::int_(j.get<long>());
                    } else {
                        return py::float_(number);
                    }
                } else if (j.is_string()) {
                    return py::str(j.get<std::string>());
                } else if (j.is_array()) {
                    py::list obj;
                    for (const auto &el : j) {
                        obj.attr("append")(from_json_impl(el));
                    }
                    return obj;
                } else { // Object
                    py::dict obj;
                    for (json::const_iterator it = j.cbegin(); it != j.cend(); ++it) {
                        obj[py::str(it.key())] = from_json_impl(it.value());
                    }
                    return obj;
                }
            }

            class python_wrapper_data_set final {
            public:
                python_wrapper_data_set(const py::object& collections,context *ctx)
                    : collection_(collections)
                    , ctx_(ctx) {}

                python_wrapper_data_set() {
                    std::cerr << "constructor python_wrapper_data_set()" << std::endl;
                }


                auto map(py::function f) -> python_wrapper_data_set {
                    auto &current = *ctx_->top();
                    auto &new_set = *(ctx_->next());

                    for (auto &i:current) {
                        auto result = f(i.second);
                        for(auto&i:result){

                        }
                        ///new_set.append();
                    }

                    return *this;
                }

                auto reduce_by_key(py::function f) -> python_wrapper_data_set {
                    auto &current = *ctx_->top();
                    auto &new_set = *(ctx_->next());


                    return *this;
                }

                auto flat_map(py::function f,bool preservesPartitioning=false) -> python_wrapper_data_set {
                    auto &current = *ctx_->top();
                    auto &new_set = *(ctx_->next());

                    collection_ = f(collection_);

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
                py::object collection_;
                context *ctx_;
            };


            class context_wrapper final {
            public:
                context_wrapper(const std::string &name, context *ctx)
                        : name_(name)
                        , ctx_(ctx)
                        {
                }

                auto text_file(const std::string &path) -> python_wrapper_data_set {
                    auto * file = ctx_->open_file(path);
                    auto all_file = file->raw_file();
                    return python_wrapper_data_set(py::str(all_file.to_string()),ctx_);
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
                                        [cm_](
                                                  std::string master
                                                , std::string appName
                                                , std::string sparkHome
                                                , std::string pyFiles
                                                , std::string environment
                                                , int batchSize
                                                , int serializer
                                                , int conf
                                                , int gateway
                                                , int jsc
                                                , int profiler_cls
                                        ) {
                                            auto *ctx = cm_->create_context(appName);
                                            return new context_wrapper(name, ctx);
                                        }
                                )
                        )
                        .def("textFile", &context_wrapper::text_file);

                py::class_<python_wrapper_data_set>(mapreduce_submodule, "DataSet")
                        .def("map", &python_wrapper_data_set::map)
                        .def("reduceByKey", &python_wrapper_data_set::reduce_by_key)
                        .def("flatMap", &python_wrapper_data_set::flat_map)
                        .def("collect", &python_wrapper_data_set::collect);
            }

}}}