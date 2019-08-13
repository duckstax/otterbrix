#include <rocketjoe/services/python_engine/mapreduce.hpp>

namespace rocketjoe { namespace services { namespace python_engine {

            data_set_wrapper::data_set_wrapper(data_set *ds) :ds_(ds) {}

            auto data_set_wrapper::map(py::function f) -> data_set_wrapper & {
                ds_->transform(
                        [=](const std::string& data)-> std::string{
                            auto result =  f(data);
                            auto unpack = result.cast<std::string>();
                            return unpack;
                        }
                );
                return *this;
            }

            auto data_set_wrapper::reduce_by_key(py::function f) -> data_set_wrapper & {
                ds_->transform(
                        [=](const std::string& data)-> std::string{
                            auto result =  f(data);
                            auto unpack = result.cast<std::string>();
                            return unpack;
                        }
                );

                return *this;
            }

            auto data_set_wrapper::flat_map(py::function f) -> data_set_wrapper & {
                ds_->transform(
                        [=](const std::string& data)-> std::string{
                            auto result =  f(data);
                            auto unpack = result.cast<std::string>();
                            return unpack;
                        }
                );
                return *this;
            }

            auto data_set_wrapper::collect() -> py::list {
                py::list tmp{};
                ///auto range = ds_->range();
                ///std::copy(range.first,range.second,std::back_inserter(tmp));
                return  tmp;

            }

            context_wrapper::context_wrapper(const std::string &name, context *ctx)
                    : name_(name)
                    , context_(ctx)
            {

            }

            auto context_wrapper::text_file(const std::string &path) -> data_set_wrapper & {
                auto* ds = context_->create_data_set(name_);
                ds_ = new data_set_wrapper(ds);
                return *ds_;
            }

            void add_mapreduce(py::module &pyrocketjoe, data_set_manager *dsm) {

                auto mapreduce_submodule = pyrocketjoe.def_submodule("mapreduce");

                py::class_<context_wrapper>(mapreduce_submodule, "RocketJoeContext")
                        .def(
                                py::init(
                                        [dsm](const std::string& name) {
                                            auto* ctx = dsm->create_context(name);
                                            return new context_wrapper(name, ctx);
                                        }
                                )
                        )
                        .def("textFile",&context_wrapper::text_file);

                py::class_<data_set_wrapper>(mapreduce_submodule, "DataSet")
                        .def("map",&data_set_wrapper::map)
                        .def("reduceByKey",&data_set_wrapper::reduce_by_key)
                        .def("flatMap",&data_set_wrapper::flat_map)
                        .def("collect",&data_set_wrapper::collect);
            }

}}}