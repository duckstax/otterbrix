#include <pybind11/pybind11.h>

#include "wrapper_client.hpp"
#include "wrapper_collection.hpp"
#include "wrapper_database.hpp"
#include "wrapper_document.hpp"

#include <boost/uuid/uuid.hpp>            // uuid class
#include <boost/uuid/uuid_generators.hpp> // generators
#include <boost/uuid/uuid_io.hpp>         // streaming operators etc.
#include <goblin-engineer/core.hpp>

#include "components/log/log.hpp"

#include "services/storage/database.hpp"
#include "services/storage/collection.hpp"

// The bug related to the use of RTTI by the pybind11 library has been fixed: a
// declaration should be in each translation unit.
PYBIND11_DECLARE_HOLDER_TYPE(T, boost::intrusive_ptr<T>)

class spaces final {
public:
    spaces(spaces& other) = delete;

    void operator=(const spaces&) = delete;

    static spaces* get_instance(const std::string& value);
/*
    void SomeBusinessLogic() {
        // ...
    }

    std::string value() const {
        return value_;
    }
*/
protected:
    spaces(){
        std::string log_dir("/tmp/docker_logs/");
        auto log = initialization_logger("duck_charmer", log_dir);
        log.set_level(log_t::level::trace);

        manager_database_ = goblin_engineer::make_manager_service<services::storage::manager_database_t>(log, 1, 1000);
        database_ = goblin_engineer::make_manager_service<services::storage::database_t>(manager_database_, log, 1, 1000);
        collection_ = goblin_engineer::make_service<services::storage::collection_t>(database_, log);

        //auto manager_dispatcher = goblin_engineer::make_manager_service<kv::manager_dispatcher_t>(log, 1, 1000);
        //auto dispatcher = goblin_engineer::make_service<kv::dispatcher_t>(manager_dispatcher, log);
    }

    static spaces* instance_;

    services::storage::manager_database_ptr manager_database_;
    services::storage::database_ptr database_;
   goblin_engineer::actor_address collection_;

};

spaces* spaces::instance_ = nullptr;

spaces* spaces::get_instance(const std::string& value) {
    if (instance_ == nullptr) {
        instance_ = new spaces();
    }
    return instance_;
}

PYBIND11_MODULE(friedrich_db, m) {
    py::class_<wrapper_client>(m, "Client")
        .def(py::init<>())
        .def("__getitem__", &wrapper_client::get_or_create)
        .def("database_names", &wrapper_client::database_names);

    py::class_<wrapper_database, boost::intrusive_ptr<wrapper_database>>(m, "DataBase")
        ///.def(py::init<friedrichdb::core::database_t*>())
        .def("collection_names", &wrapper_database::collection_names)
        .def("drop_collection", &wrapper_database::drop_collection)
        .def("__getitem__", &wrapper_database::create);

    py::class_<wrapper_collection, boost::intrusive_ptr<wrapper_collection>>(m, "Collection")
        ///.def(py::init<friedrichdb::core::collection_t*>())
        .def("insert", &wrapper_collection::insert)
        .def("insert_many", &wrapper_collection::insert_many)
        .def("get", &wrapper_collection::get, py::arg("cond"))
        .def("search", &wrapper_collection::search, py::arg("cond"))
        .def("all", &wrapper_collection::all)
        .def("update", &wrapper_collection::update, py::arg("fields"), py::arg("cond"))
        .def("remove", &wrapper_collection::remove, py::arg("cond"))
        .def("drop", &wrapper_collection::drop)
        .def("__len__", &wrapper_collection::size);

    py::class_<wrapper_document, boost::intrusive_ptr<wrapper_document>>(m, "Document")
        ///.def(py::init<friedrichdb::core::document_t*>())
        .def("__repr__", &wrapper_document::print)
        .def("__getitem__", &wrapper_document::get)
        .def("get", &wrapper_document::get);

    m.def(
        "generate_id",
        []() {
            boost::uuids::random_generator generator;
            return boost::uuids::to_string(generator());
        });
}