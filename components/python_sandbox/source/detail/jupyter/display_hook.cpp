#include <detail/jupyter/display_hook.hpp>

#include <boost/smart_ptr/intrusive_ptr.hpp>

#include <detail/jupyter/session.hpp>

//The bug related to the use of RTTI by the pybind11 library has been fixed: a
//declaration should be in each translation unit.
PYBIND11_DECLARE_HOLDER_TYPE(T, boost::intrusive_ptr<T>)

namespace components { namespace detail { namespace jupyter {

    using namespace pybind11::literals;

    display_hook::display_hook(
        boost::intrusive_ptr<session> current_session,
        socket_manager iopub_socket
    ) : current_session{current_session}, socket_manager_{iopub_socket},
        execution_count{0} {}

    auto display_hook::set_execution_count(size_t execution_count) -> void {
        this->execution_count = execution_count;
    }

    auto display_hook::operator()(py::object value) -> void {
        if(!value) {
            return;
        }

        auto builtins = py::module::import("builtins");

        builtins.attr("_") = value;

        auto sys{py::module::import("sys")};

        sys.attr("stdout").attr("flush")();
        sys.attr("stderr").attr("flush")();

        socket_manager_->iopub( current_session->construct_message(
                {"execute_result"}, {{"msg_type", "execute_result"}},
                {}, {},
                {{"execution_count", execution_count},
                 {"data", {{"text/plain", py::repr(value)}}},
                 {"metadata", nl::json::object()}}, {}
            )
        );
    }

}}}
