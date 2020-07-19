#pragma once

#include <functional>
#include <memory>
#include <unordered_set>

#include <boost/optional/optional.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>
#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <boost/uuid/uuid.hpp>

#include <nlohmann/json.hpp>

#include <zmq.hpp>

#include <pybind11/pybind11.h>

#include <boost/variant/variant.hpp>

#include "session.hpp"
#include "zmq_socket_shared.hpp"

namespace components { namespace detail { namespace jupyter {

    namespace nl = nlohmann;
    namespace py = pybind11;

    enum class poll_flags : std::uint8_t {
        none = 0,
        shell_socket = 1 << 0,
        control_socket = 1 << 1,
        heartbeat_socket = 1 << 2
    };

    constexpr auto operator|(poll_flags a, poll_flags b) noexcept -> poll_flags {
        using U = typename std::underlying_type<poll_flags>::type;

        return static_cast<poll_flags>(static_cast<U>(a) | static_cast<U>(b));
    }

    constexpr auto operator|=(poll_flags& a, poll_flags b) noexcept -> void {
        using U = typename std::underlying_type<poll_flags>::type;

        a = static_cast<poll_flags>(static_cast<U>(a) | static_cast<U>(b));
    }

    constexpr auto operator&(poll_flags a, poll_flags b) noexcept -> poll_flags {
        using U = typename std::underlying_type<poll_flags>::type;

        return static_cast<poll_flags>(static_cast<U>(a) & static_cast<U>(b));
    }

    constexpr auto operator&=(poll_flags& a, poll_flags b) noexcept -> void {
        using U = typename std::underlying_type<poll_flags>::type;

        a = static_cast<poll_flags>(static_cast<U>(a) & static_cast<U>(b));
    }

    constexpr auto operator^(poll_flags a, poll_flags b) noexcept -> poll_flags {
        using U = typename std::underlying_type<poll_flags>::type;

        return static_cast<poll_flags>(static_cast<U>(a) ^ static_cast<U>(b));
    }

    constexpr auto operator^=(poll_flags& a, poll_flags b) noexcept -> void {
        using U = typename std::underlying_type<poll_flags>::type;

        a = static_cast<poll_flags>(static_cast<U>(a) ^ static_cast<U>(b));
    }

    constexpr auto operator~(poll_flags a) noexcept -> poll_flags {
        using U = typename std::underlying_type<poll_flags>::type;

        return static_cast<poll_flags>(~static_cast<U>(a));
    }

    class BOOST_SYMBOL_VISIBLE pykernel final : public boost::intrusive_ref_counter<pykernel> {
    public:
        pykernel(
                std::string signature_key
                , std::string signature_scheme
                , zmq::socket_t shell_socket
                , zmq::socket_t control_socket
                , boost::optional<zmq::socket_t> stdin_socket
                , zmq::socket_t iopub_socket
                , boost::optional<zmq::socket_t> heartbeat_socket
                , boost::optional<zmq::socket_t> registration_socket
                , bool engine_mode
                , boost::uuids::uuid identifier);

        ~pykernel();

        pykernel(const pykernel&) = delete;

        pykernel& operator=(const pykernel&) = delete;

        auto registration() -> bool;

        auto poll(poll_flags polls) -> bool;

    private:
        auto topic(std::string topic) const -> std::string;

        auto publish_status(std::string status, nl::json) -> void;

        auto set_parent(std::vector<std::string> identifiers, nl::json parent) -> void;

        auto init_metadata() -> nl::json;

        auto finish_metadata(nl::json& metadata, execute_ok_reply reply);

        auto finish_metadata(nl::json& metadata,execute_error_reply reply);

        auto finish_metadata(nl::json& metadata,apply_ok_reply reply);

        auto finish_metadata(nl::json& metadata, apply_error_reply reply);

        auto do_execute(std::string code,
                        bool silent,
                        bool store_history,
                        nl::json user_expressions,
                        bool allow_stdin) -> boost::variant<execute_ok_reply,execute_error_reply>;

        auto execute_request(zmq::socket_t& socket,
                             std::vector<std::string> identifiers,
                             nl::json parent) -> void;

        auto do_inspect(std::string code,
                        std::size_t cursor_pos,
                        std::size_t detail_level) -> nl::json;

        auto inspect_request(zmq::socket_t& socket,
                             std::vector<std::string> identifiers,
                             nl::json parent) -> void;

        auto do_complete(std::string code,
                         std::size_t cursor_pos) -> nl::json;

        auto complete_request(zmq::socket_t& socket,
                              std::vector<std::string> identifiers,
                              nl::json parent) -> void;

        auto history_request(bool output,
                             bool raw,
                             const std::string& hist_access_type,
                             std::int64_t session,
                             std::size_t start,
                             boost::optional<std::size_t> stop,
                             boost::optional<std::size_t> n,
                             const boost::optional<std::string>& pattern,
                             bool unique) -> nl::json;

        auto do_is_complete(std::string code) -> nl::json;

        auto is_complete_request(zmq::socket_t& socket,
                                 std::vector<std::string> identifiers,
                                 nl::json parent) -> void;

        auto do_kernel_info() -> nl::json;

        auto kernel_info_request(zmq::socket_t& socket,
                                 std::vector<std::string> identifiers,
                                 nl::json parent) -> void;

        auto do_shutdown(bool restart) -> nl::json;

        auto shutdown_request(zmq::socket_t& socket,
                              std::vector<std::string> identifiers,
                              nl::json parent) -> void;

        auto do_interrupt() -> nl::json;

        auto interrupt_request(zmq::socket_t& socket,
                               std::vector<std::string> identifiers,
                               nl::json parent) -> void;

        auto do_apply(std::string msg_id,
                      std::vector<std::string> buffers) -> boost::variant<apply_ok_reply,
                                                                          apply_error_reply>;

        auto apply_request(zmq::socket_t& socket,
                           std::vector<std::string> identifiers,
                           nl::json parent,
                           std::vector<std::string> buffers) -> void;

        auto do_clear() -> nl::json;

        auto clear_request(zmq::socket_t& socket,
                           std::vector<std::string> identifiers,
                           nl::json parent) -> void;

        auto do_abort(boost::optional<std::vector<std::string>> identifiers) -> nl::json;

        auto abort_request(zmq::socket_t& socket,
                           std::vector<std::string> identifiers,
                           nl::json parent) -> void;

        auto abort_reply(zmq::socket_t& socket,
                         std::vector<std::string> identifiers,
                         nl::json parent) -> void;

        auto dispatch_shell(std::vector<std::string> msgs) -> bool;

        auto dispatch_control(std::vector<std::string> msgs) -> bool;

        zmq::socket_t shell_socket;
        zmq::socket_t control_socket;
        boost::intrusive_ptr<zmq_socket_shared> stdin_socket;
        boost::intrusive_ptr<zmq_socket_shared> iopub_socket;
        boost::optional<zmq::socket_t> heartbeat_socket;
        boost::optional<zmq::socket_t> registration_socket;
        bool engine_mode;
        boost::intrusive_ptr<session> current_session;
        py::object shell;
        py::object pystdout;
        py::object pystderr;
        boost::uuids::uuid identifier;
        std::uint64_t engine_identifier;
        std::vector<std::string> parent_identifiers;
        nl::json parent_header;
        bool abort_all;
        std::unordered_set<std::string> aborted;
        std::size_t execution_count;
    };
}}} // namespace components::detail::jupyter
