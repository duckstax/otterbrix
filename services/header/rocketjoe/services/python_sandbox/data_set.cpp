#include <rocketjoe/services/python_sandbox/detail/data_set.hpp>
#include <rocketjoe/services/python_sandbox/detail/context.hpp>

#include <nlohmann/json.hpp>

namespace rocketjoe { namespace services { namespace python_sandbox { namespace detail {

                using namespace pybind11::literals;
                namespace py = pybind11;
                using namespace nlohmann;

                json to_json_impl(const py::handle &obj) {
                    if (obj.is_none()) {
                        return nullptr;
                    }
                    if (py::isinstance<py::bool_>(obj)) {
                        return obj.cast<bool>();
                    }
                    if (py::isinstance<py::int_>(obj)) {
                        return obj.cast<long>();
                    }
                    if (py::isinstance<py::float_>(obj)) {
                        return obj.cast<double>();
                    }
                    if (py::isinstance<py::str>(obj)) {
                        return obj.cast<std::string>();
                    }
                    if (py::isinstance<py::tuple>(obj) || py::isinstance<py::list>(obj)) {
                        auto out = json::array();
                        for (const py::handle &value : obj) {
                            out.push_back(to_json_impl(value));
                        }
                        return out;
                    }
                    if (py::isinstance<py::dict>(obj)) {
                        auto out = json::object();
                        for (const py::handle &key : obj) {
                            out[py::str(key).cast<std::string>()] = to_json_impl(obj[key]);
                        }
                        return out;
                    }
                    throw std::runtime_error(
                            "to_json not implemented for this type of object: " + obj.cast<std::string>());
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


                data_set::data_set(const py::object &collections, context *ctx)
                        : collection_(collections)
                        , ctx_(ctx)
                        {

                }

                data_set::data_set() {
                    std::cerr << "constructor python_wrapper_data_set()" << std::endl;
                    assert(false);
                }

                auto data_set::map(py::function f, bool preservesPartitioning) -> intrusive_ptr<pipelien_data_set> {


                    return map_partitions_with_index(f,preservesPartitioning);
                }

                auto data_set::reduce_by_key(py::function f, bool preservesPartitioning) -> intrusive_ptr<pipelien_data_set> {

                }

                auto data_set::flat_map(py::function f, bool preservesPartitioning) -> intrusive_ptr<pipelien_data_set> {
                    collection_ = f(collection_);
                    return map_partitions_with_index(f,preservesPartitioning);
                }

                auto data_set::collect() -> py::list {

                    py::list tmp{};

                    return tmp;
                }

                auto data_set::map_partitions_with_index(py::function f, bool preservesPartitioning) -> intrusive_ptr<pipelien_data_set> {
                    return ctx_->make_new_step<pipelien_data_set>(this, f, preservesPartitioning);
                }

                auto data_set::map_partitions(py::function f, bool preservesPartitioning) -> intrusive_ptr<pipelien_data_set> {

                    return map_partitions_with_index(f, preservesPartitioning);
                }

                auto data_set::ctx() -> context * {
                    return ctx_;
                }

                pipelien_data_set::pipelien_data_set(data_set* ds , py::function f, bool preservesPartitioning)
                    :data_set(py::object(),ds->ctx()),f_(f) {}



            }}}}