#include "wrapper_database.hpp"
#include "spaces.hpp"

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
    log_.debug("start wrapper_database::drop_collection: {}",name);

    std::unique_lock<std::mutex> lk(mtx_);
    cv_.wait(lk, [this]() { return i == 1; });
    log_.debug("finish wrapper_database::drop_collection: {}",name);
    return drop_collection_;
}

wrapper_collection_ptr wrapper_database::create(const std::string& name) {
    log_.debug("wrapper_database::create name collection: {}",name);
    goblin_engineer::send(
        dispatcher_,
        goblin_engineer::actor_address(),
        "create_collection",
        session_t(),
        name,
        std::function<void(goblin_engineer::actor_address)>([this](goblin_engineer::actor_address address) {
            tmp_ = boost::intrusive_ptr<wrapper_collection>(new wrapper_collection(log_,dispatcher_,database_,address));
            d_();
        }));
    log_.debug("wrapper_client::get_or_create send -> dispatcher: {}",dispatcher_->type());
    std::unique_lock<std::mutex> lk(mtx_);
    cv_.wait(lk, [this]() { return i == 1; });
    log_.debug("wrapper_client::get_or_create return wrapper_database_ptr");
    return tmp_;
}
wrapper_database::wrapper_database(log_t&log,goblin_engineer::actor_address dispatcher,goblin_engineer::actor_address database)
        : log_(log.clone())
        , database_(std::move(database))
        , dispatcher_(dispatcher) {
    log_.debug("wrapper_database");
}

void wrapper_database::d_() {
    cv_.notify_all();
    i = 1;
}
