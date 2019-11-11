#include <rocketjoe/services/python_engine/detail/mapreduce.hpp>

#include <deque>

#include <pybind11/functional.h>
#include <pybind11/stl.h>

#include <nlohmann/json.hpp>

#include <rocketjoe/services/python_engine/detail/context_manager.hpp>
#include <rocketjoe/services/python_engine/detail/file_manager.hpp>
#include <rocketjoe/services/python_engine/detail/python_wrapper_context.hpp>
#include <rocketjoe/services/python_engine/detail/python_wrapper_data_set.hpp>

namespace rocketjoe { namespace services { namespace python_engine { namespace detail {



            auto add_mapreduce(py::module &pyrocketjoe, context_manager *cm_) -> void  {

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
                                            return new context_wrapper(appName, ctx);
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

}}}}