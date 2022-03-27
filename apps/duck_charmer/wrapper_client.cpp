#include "wrapper_client.hpp"

#include <pybind11/stl.h>
#include <pybind11/stl_bind.h>

#include "spaces.hpp"
#include <services/collection/collection.hpp>

#include "wrapper_database.hpp"

// The bug related to the use of RTTI by the pybind11 library has been fixed: a
// declaration should be in each translation unit.
PYBIND11_DECLARE_HOLDER_TYPE(T, boost::intrusive_ptr<T>)

namespace duck_charmer {

    wrapper_database_ptr wrapper_client::get_or_create(const std::string& name) {
        log_.debug("wrapper_client::get_or_create name database: {}", name);
        auto session_tmp = duck_charmer::session_id_t();
        auto result = ptr_->create_database(session_tmp, name);
        log_.debug("wrapper_client::get_or_create return wrapper_database_ptr");
        names_.emplace(name,result);
        return result;
    }

    wrapper_client::wrapper_client(log_t& log, wrapper_dispatcher_t* dispatcher)
        : ptr_(dispatcher)
        , log_(log.clone()){
        log_.debug("wrapper_client::wrapper_client()");
    }

    auto wrapper_client::database_names() -> py::list {
        py::list tmp;
        for(auto&i:names_){
            tmp.append(i.first);
        }
        return tmp;
    }
}
