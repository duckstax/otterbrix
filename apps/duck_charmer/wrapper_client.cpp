#include "wrapper_client.hpp"

#include <pybind11/stl.h>
#include <pybind11/stl_bind.h>

#include "spaces.hpp"
#include "storage/forward.hpp"

// The bug related to the use of RTTI by the pybind11 library has been fixed: a
// declaration should be in each translation unit.
PYBIND11_DECLARE_HOLDER_TYPE(T, boost::intrusive_ptr<T>)
using services::storage::session_t;
wrapper_database_ptr wrapper_client::get_or_create(const std::string& name) {
    goblin_engineer::send(
        dispatcher_,
        goblin_engineer::actor_address(),
        "create_database",
        session_t(),
        name,
        std::function<void(goblin_engineer::actor_address)>([this](goblin_engineer::actor_address address) {
            tmp_ = boost::intrusive_ptr<wrapper_database>(new wrapper_database(address));
            d();
        }));

    std::unique_lock<std::mutex> lk(mtx_);
    cv_.wait(lk, [this]() { return i == 1; });
    return tmp_;
    /*
    auto it = storage_.find(name);
    if(it == storage_.end()){
        auto result =  storage_.emplace(name, new wrapper_database(new friedrichdb::core::database_t ));
        return result.first->second;
    } else {
        return it->second;
    }
     */
}

wrapper_client::wrapper_client() {
    dispatcher_ = spaces::get_instance()->dispatcher();
}

auto wrapper_client::database_names() -> py::list {
    py::list tmp;
    //for(auto&i:storage_){
    ///    tmp.append(i.first);
    /// }
    return tmp;
}