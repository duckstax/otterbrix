#include <pybind11/pybind11.h>

#include "wrapper_client.hpp"
#include "wrapper_collection.hpp"
#include "wrapper_database.hpp"
#include "wrapper_cursor.hpp"
#include "wrapper_document.hpp"
#include "wrapper_result.hpp"

#include <boost/uuid/uuid.hpp>            // uuid class
#include <boost/uuid/uuid_generators.hpp> // generators
#include <boost/uuid/uuid_io.hpp>         // streaming operators etc.

#include "spaces.hpp"

// The bug related to the use of RTTI by the pybind11 library has been fixed: a
// declaration should be in each translation unit.
PYBIND11_DECLARE_HOLDER_TYPE(T, boost::intrusive_ptr<T>)

using namespace duck_charmer;

spaces* spaces::instance_ = nullptr;

spaces* spaces::get_instance() {
    if (instance_ == nullptr) {
        instance_ = new spaces();
    }
    return instance_;
}

PYBIND11_MODULE(duck_charmer, m) {
    py::class_<wrapper_client>(m, "Client")
        .def(py::init([](){
            auto* spaces =  spaces::get_instance();
            auto dispatcher =  spaces->dispatcher();
            auto log = spaces::get_instance()->get_log().clone();
            return new wrapper_client(log,dispatcher);
        }))
        .def("__getitem__", &wrapper_client::get_or_create)
        .def("database_names", &wrapper_client::database_names)
        ;

    py::class_<wrapper_database, boost::intrusive_ptr<wrapper_database>>(m, "DataBase")
        .def("collection_names", &wrapper_database::collection_names)
        .def("drop_collection", &wrapper_database::drop_collection)
        .def("__getitem__", &wrapper_database::create)
        ;

    py::class_<wrapper_collection, boost::intrusive_ptr<wrapper_collection>>(m, "Collection")
        .def("__repr__", &wrapper_collection::print)
        .def("__len__", &wrapper_collection::size)
        .def("count", &wrapper_collection::size)
        .def("insert", &wrapper_collection::insert, py::arg("documents"))
        .def("insert_one", &wrapper_collection::insert_one, py::arg("document"))
        .def("insert_many", &wrapper_collection::insert_many, py::arg("documents"))
        .def("update_one", &wrapper_collection::update_one, py::arg("filter"), py::arg("update"), py::arg("upsert") = false)
        .def("update_many", &wrapper_collection::update_many, py::arg("filter"), py::arg("update"), py::arg("upsert") = false)
        .def("find", &wrapper_collection::find, py::arg("filter"))
        .def("find_one", &wrapper_collection::find_one, py::arg("filter"))
        .def("delete_one", &wrapper_collection::delete_one, py::arg("filter"))
        .def("delete_many", &wrapper_collection::delete_many, py::arg("filter"))
        .def("drop", &wrapper_collection::drop)
        ;

    py::class_<wrapper_document, boost::intrusive_ptr<wrapper_document>>(m, "Document")
        .def("__repr__", &wrapper_document::print)
        .def("__getitem__", &wrapper_document::get)
        .def("get", &wrapper_document::get)
        ;

    py::class_<wrapper_cursor, boost::intrusive_ptr<wrapper_cursor>>(m, "Cursor")
        .def("__repr__", &wrapper_cursor::print)
        .def("__del__", &wrapper_cursor::close)
        .def("__len__", &wrapper_cursor::size)
        .def("__getitem__", &wrapper_cursor::get)
        .def("__iter__", &wrapper_cursor::__iter__)
        .def("__next__", &wrapper_cursor::__next__)
        .def("count", &wrapper_cursor::size)
        .def("close", &wrapper_cursor::close)
        .def("hasNext", &wrapper_cursor::has_next)
        .def("next", &wrapper_cursor::next)
        //.def("paginate", &wrapper_cursor::paginate)
        //.def("_order", &wrapper_cursor::_order)
        //.def("sort", &wrapper_cursor::sort)
        ;

    py::class_<wrapper_result_delete, boost::intrusive_ptr<wrapper_result_delete>>(m, "DeleteResult")
        .def_property_readonly("raw_result", &wrapper_result_delete::raw_result)
        .def_property_readonly("deleted_count", &wrapper_result_delete::deleted_count)
        ;

    py::class_<wrapper_result_update, boost::intrusive_ptr<wrapper_result_update>>(m, "UpdateResult")
        .def_property_readonly("raw_result", &wrapper_result_update::raw_result)
        .def_property_readonly("matched_count", &wrapper_result_update::matched_count)
        .def_property_readonly("modified_count", &wrapper_result_update::modified_count)
        .def_property_readonly("upserted_id", &wrapper_result_update::upserted_id)
        ;

    m.def(
        "generate_id",
        []() {
            boost::uuids::random_generator generator;
            return boost::uuids::to_string(generator());
        });
}
