#include <detail/context.hpp>
#include <detail/data_set.hpp>

#include <nlohmann/json.hpp>

PYBIND11_DECLARE_HOLDER_TYPE(T, boost::intrusive_ptr<T>)

namespace components { namespace python { namespace detail {

    using namespace pybind11::literals;
    namespace py = pybind11;
    using namespace nlohmann;

    data_set::data_set(const py::object& collections, context* ctx)
        : collection_(collections)
        , ctx_(ctx) {
    }

    data_set::data_set() {
        std::cerr << "constructor python_wrapper_data_set()" << std::endl;
        assert(false);
    }

    auto data_set::map(py::function f, bool preservesPartitioning) -> boost::intrusive_ptr<pipelien_data_set> {
        return map_partitions_with_index(f, preservesPartitioning);
    }

    auto data_set::reduce_by_key(py::function /*f*/, bool /*preservesPartitioning*/) -> boost::intrusive_ptr<pipelien_data_set> {
        return boost::intrusive_ptr<pipelien_data_set>{};
    }

    auto data_set::flat_map(py::function f, bool preservesPartitioning) -> boost::intrusive_ptr<pipelien_data_set> {
        collection_ = f(collection_);
        return map_partitions_with_index(f, preservesPartitioning);
    }

    auto data_set::collect() -> py::list {
        py::list tmp{};

        return tmp;
    }

    auto data_set::map_partitions_with_index(py::function f, bool preservesPartitioning) -> boost::intrusive_ptr<pipelien_data_set> {
        return ctx_->make_new_step<pipelien_data_set>(this, f, preservesPartitioning);
    }

    auto data_set::map_partitions(py::function f, bool preservesPartitioning) -> boost::intrusive_ptr<pipelien_data_set> {
        return map_partitions_with_index(f, preservesPartitioning);
    }

    auto data_set::ctx() -> context* {
        return ctx_;
    }

    pipelien_data_set::pipelien_data_set(data_set* ds, py::function f, bool /*preservesPartitioning*/)
        : data_set(py::object(), ds->ctx())
        , f_(f) {}

}}} // namespace components::python::detail
