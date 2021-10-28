#include "wrapper_cursor.hpp"

// The bug related to the use of RTTI by the pybind11 library has been fixed: a
// declaration should be in each translation unit.
PYBIND11_DECLARE_HOLDER_TYPE(T, boost::intrusive_ptr<T>)

wrapper_cursor::wrapper_cursor(components::session::session_t session, wrapper_cursor::pointer cursor)
        : session_(session)
        , ptr_(cursor)
        , dispatcher_(goblin_engineer::address_t::empty_address()){
    std::cerr << "session :" << session.data() << std::endl;
}
void wrapper_cursor::close() {
    close_=true;
    goblin_engineer::send(
        dispatcher_,
        goblin_engineer::address_t::empty_address(),
        "close_cursor",
        session_
    );
}
std::size_t wrapper_cursor::size() const {
    return ptr_->size();
}
void wrapper_cursor::next() {

}
