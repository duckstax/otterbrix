#include "wrapper_client.hpp"

#include <pybind11/stl.h>
#include <pybind11/stl_bind.h>

#include "spaces.hpp"
#include "storage/forward.hpp"

// The bug related to the use of RTTI by the pybind11 library has been fixed: a
// declaration should be in each translation unit.
PYBIND11_DECLARE_HOLDER_TYPE(T, boost::intrusive_ptr<T>)
namespace duck_charmer {
    wrapper_database_ptr wrapper_client::get_or_create(const std::string& name) {
        log_.debug("wrapper_client::get_or_create name database: {}", name);
        auto session_tmp = duck_charmer::session_t();
        auto result = ptr_->(session_tmp, name_);
        log_.debug("wrapper_client::get_or_create return wrapper_database_ptr");
        return tmp_;
    }

    wrapper_client::wrapper_client(log_t& log, goblin_engineer::address_t dispatcher)
        : log_(log.clone())
        , dispatcher_(dispatcher) {
        log_.debug("wrapper_client::wrapper_client()");
    }

    auto wrapper_client::database_names() -> py::list {
        py::list tmp;
        //for(auto&i:storage_){
        ///    tmp.append(i.first);
        /// }
        return tmp;
    }
}
