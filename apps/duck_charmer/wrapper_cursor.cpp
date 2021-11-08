#include "wrapper_cursor.hpp"
#include "convert.hpp"
#include "route.hpp"
#include <storage/result.hpp>

// The bug related to the use of RTTI by the pybind11 library has been fixed: a
// declaration should be in each translation unit.
PYBIND11_DECLARE_HOLDER_TYPE(T, boost::intrusive_ptr<T>)

wrapper_cursor::wrapper_cursor(goblin_engineer::actor_address dispatcher, components::session::session_t session, wrapper_cursor::pointer cursor)
        : session_(session)
        , ptr_(cursor)
        , dispatcher_(dispatcher) {
}
void wrapper_cursor::close() {
    close_ = true;
    goblin_engineer::send(
        dispatcher_,
        goblin_engineer::actor_address(),
        duck_charmer::collection::close_cursor,
        session_
                );
}

bool wrapper_cursor::has_next() {
    bool res = false;
    i = 0;
    goblin_engineer::send(
                dispatcher_,
                goblin_engineer::actor_address(),
                duck_charmer::cursor::has_next_cursor,
                session_,
                std::function<void(bool)>([&](bool has_next) {
                    res = has_next;
                    d_();
                }));
    std::unique_lock<std::mutex> lk(mtx_);
    cv_.wait(lk, [this]() { return i == 1; });
    return res;
}

bool wrapper_cursor::next() {
    bool res = false;
    i = 0;
    goblin_engineer::send(
                dispatcher_,
                goblin_engineer::actor_address(),
                duck_charmer::cursor::next_cursor,
                session_,
                std::function<void(bool)>([&](bool result) {
                    res = result;
                    d_();
                }));
    std::unique_lock<std::mutex> lk(mtx_);
    cv_.wait(lk, [this]() { return i == 1; });
    return res;
}

std::size_t wrapper_cursor::size() {
    return ptr_->size();
}

py::object wrapper_cursor::get(const std::string &key) {
    return from_object(ptr_->get(), key);
}

std::string wrapper_cursor::print() {
    return ptr_->get().to_json();
}

void wrapper_cursor::d_() {
    cv_.notify_all();
    i = 1;
}
