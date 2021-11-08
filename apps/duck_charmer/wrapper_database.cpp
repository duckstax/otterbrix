#include "wrapper_database.hpp"
#include "spaces.hpp"
namespace duck_charmer {
    wrapper_database::~wrapper_database() {
    }

    auto wrapper_database::collection_names() -> py::list {
        py::list tmp;
        ///for (auto &i:*ptr_) {
        ///    tmp.append(i.first);
        ///}
        return tmp;
    }

    bool wrapper_database::drop_collection(const std::string& name) {
        log_.debug("start wrapper_database::drop_collection: {}", name);

        log_.debug("finish wrapper_database::drop_collection: {}", name);
        return true;
    }

    wrapper_collection_ptr wrapper_database::create(const std::string& name) {
        log_.debug("wrapper_database::create name collection: {}", name);
        auto session_tmp = duck_charmer::session_t();
        auto result =  ptr_->create_collection(session_tmp,name_);
        log_.debug("wrapper_client::get_or_create return wrapper_database_ptr");
        return result;
    }
    wrapper_database::wrapper_database(const std::string& name, wrapper_dispatcher_t* ptr, log_t& log)
        : ptr_(ptr)
        , log_(log.clone()) {
        log_.debug("wrapper_database");
    }

}