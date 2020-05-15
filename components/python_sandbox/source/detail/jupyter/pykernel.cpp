#include <detail/jupyter/pykernel.hpp>

#include <iostream>
#include <algorithm>
#include <cassert>
#include <memory>
#include <stdexcept>
#include <string>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include <boost/locale/date_time.hpp>
#include <boost/locale/format.hpp>
#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <boost/variant/variant.hpp>

#include <nlohmann/json.hpp>

#include <pybind11/embed.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include <zmq_addon.hpp>

#include <detail/jupyter/display_hook.hpp>
#include <detail/jupyter/shell_display_hook.hpp>
#include <detail/jupyter/session.hpp>
#include <detail/jupyter/shell.hpp>
#include <detail/jupyter/zmq_socket_shared.hpp>

//The bug related to the use of RTTI by the pybind11 library has been fixed: a
//declaration should be in each translation unit.
PYBIND11_DECLARE_HOLDER_TYPE(T, boost::intrusive_ptr<T>)

namespace pybind11 { namespace detail {
    template<typename T>
    struct type_caster<boost::optional<T>>
        : optional_caster<boost::optional<T>> {};

    template<typename... Ts>
    struct type_caster<boost::variant<Ts...>>
        : variant_caster<boost::variant<Ts...>> {};

    template<>
    struct visit_helper<boost::variant> {
        template<typename... Args>
        static auto call(Args &&...args)
            -> decltype(boost::apply_visitor(args...));
    };

    template<typename... Args>
    auto visit_helper<boost::variant>::call(Args &&...args)
        -> decltype(boost::apply_visitor(args...)) {
        return boost::apply_visitor(std::forward(args...));
    }
}}

namespace nlohmann {
    template<typename T>
    struct adl_serializer<boost::optional<T>> {
        static auto to_json(
            json &data, const boost::optional<T> &maybe_data
        ) -> void;

        static auto from_json(const json &data,
                              boost::optional<T> &maybe_data) -> void;
    };

    template<typename... Args>
    struct adl_serializer<boost::variant<Args...>> {
        static auto to_json(
            json &data, const boost::variant<Args...> &variant_data
        ) -> void;

        static auto from_json(
            const json &data, boost::variant<Args...> &variant_data
        ) -> void;
    };

    template<typename T>
    auto adl_serializer<boost::optional<T>>::to_json(
        json &data, const boost::optional<T> &maybe_data
    ) -> void {
        if(maybe_data == boost::none) {
            data = nullptr;
        } else {
            data = *maybe_data;
        }
    }

    template<typename T>
    auto adl_serializer<boost::optional<T>>::from_json(
        const json &data, boost::optional<T> &maybe_data
    ) -> void {
        if(data.is_null()) {
            maybe_data = boost::none;
        } else {
            maybe_data = data.get<T>();
        }
    }

    template<typename... Args>
    auto adl_serializer<boost::variant<Args...>>::to_json(
        json &data, const boost::variant<Args...> &variant_data
    ) -> void {
        boost::apply_visitor([&](auto &&value) {
            data = std::forward<decltype(value)>(value);
        }, variant_data);
    }

    template<typename... Args>
    auto adl_serializer<boost::variant<Args...>>::from_json(
        const json &data, boost::variant<Args...> &variant_data
    ) -> void {
        throw std::runtime_error{"Casting from nlohmann::json to boost::variant"
                                 " isn\'t supported"};
    }
}

namespace rocketjoe { namespace services { namespace detail { namespace jupyter {

    namespace nl = nlohmann;
    namespace py = pybind11;

    using namespace pybind11::literals;

    class BOOST_SYMBOL_VISIBLE input_redirection final {
    public:
        input_redirection(
            boost::intrusive_ptr<zmq_socket_shared> stdin_socket,
            boost::intrusive_ptr<session> current_session,
            std::vector<std::string> parent_identifiers, nl::json parent_header,
            bool allow_stdin
        );

        ~input_redirection();

    private:
        py::object m_input;
        py::object m_getpass;
    };

    class BOOST_SYMBOL_VISIBLE interpreter_impl final {
    public:
        interpreter_impl(std::string signature_key,
                         std::string signature_scheme,
                         zmq::socket_t shell_socket,
                         zmq::socket_t control_socket,
                         boost::optional<zmq::socket_t> stdin_socket,
                         zmq::socket_t iopub_socket,
                         boost::optional<zmq::socket_t> heartbeat_socket,
                         boost::optional<zmq::socket_t> registration_socket,
                         bool engine_mode, boost::uuids::uuid identifier);

        ~interpreter_impl();

        interpreter_impl(const interpreter_impl &) = delete;

        interpreter_impl &operator=(const interpreter_impl &) = delete;

        auto registration() -> bool;

        auto poll(poll_flags polls) -> bool;

    private:
        auto topic(std::string topic) const -> std::string;

        auto publish_status(std::string status, nl::json) -> void;

        auto set_parent(std::vector<std::string> identifiers,
                        nl::json parent) -> void;

        auto init_metadata() -> nl::json;

        auto finish_metadata(nl::json &metadata, nl::json reply_content);

        auto do_execute(std::string code, bool silent, bool store_history,
                        nl::json user_expressions,
                        bool allow_stdin) -> nl::json;

        auto execute_request(zmq::socket_t &socket,
                             std::vector<std::string> identifiers,
                             nl::json parent) -> void;

        auto do_inspect(std::string code, std::size_t cursor_pos,
                        std::size_t detail_level) -> nl::json;

        auto inspect_request(zmq::socket_t &socket,
                             std::vector<std::string> identifiers,
                             nl::json parent) -> void;

        auto do_complete(std::string code, std::size_t cursor_pos) -> nl::json;

        auto complete_request(zmq::socket_t &socket,
                              std::vector<std::string> identifiers,
                              nl::json parent) -> void;

        auto history_request(bool output, bool raw,
                             const std::string &hist_access_type,
                             std::int64_t session, std::size_t start,
                             boost::optional<std::size_t> stop,
                             boost::optional<std::size_t> n,
                             const boost::optional<std::string> &pattern,
                             bool unique) -> nl::json;

        auto do_is_complete(std::string code) -> nl::json;

        auto is_complete_request(zmq::socket_t &socket,
                                 std::vector<std::string> identifiers,
                                 nl::json parent) -> void;

        auto do_kernel_info() -> nl::json;

        auto kernel_info_request(zmq::socket_t &socket,
                                 std::vector<std::string> identifiers,
                                 nl::json parent) -> void;

        auto do_shutdown(bool restart) -> nl::json;

        auto shutdown_request(zmq::socket_t &socket,
                              std::vector<std::string> identifiers,
                              nl::json parent) -> void;

        auto do_interrupt() -> nl::json;

        auto interrupt_request(zmq::socket_t &socket,
                               std::vector<std::string> identifiers,
                               nl::json parent) -> void;

        auto do_clear() -> nl::json;

        auto clear_request(zmq::socket_t &socket,
                           std::vector<std::string> identifiers,
                           nl::json parent) -> void;

        auto do_abort(
            boost::optional<std::vector<std::string>> identifiers
        ) -> nl::json;

        auto abort_request(zmq::socket_t &socket,
                           std::vector<std::string> identifiers,
                           nl::json parent) -> void;

        auto abort_reply(zmq::socket_t &socket,
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

    static auto input_request(
        boost::intrusive_ptr<zmq_socket_shared> stdin_socket,
        boost::intrusive_ptr<session> current_session,
        std::vector<std::string> parent_identifiers, nl::json parent_header,
        const std::string &prompt, bool password
    ) -> std::string {
        auto sys{py::module::import("sys")};

        sys.attr("stdout").attr("flush")();
        sys.attr("stderr").attr("flush")();

        while(true) {
            std::vector<zmq::message_t> msgs{};

            if(!zmq::recv_multipart(**stdin_socket, std::back_inserter(msgs),
                                    zmq::recv_flags::dontwait)) {
                break;
            }
        }

        current_session->send(**stdin_socket,
            current_session->construct_message(
                std::move(parent_identifiers), {{"msg_type", "input_request"}},
                std::move(parent_header), {}, {{"prompt", prompt},
                                               {"password", password}}, {}
            )
        );

        std::vector<zmq::message_t> msgs{};

        if(zmq::recv_multipart(**stdin_socket, std::back_inserter(msgs))) {
            std::vector<std::string> msgs_for_parse{};

            msgs_for_parse.reserve(msgs.size());

            for(const auto &msg : msgs) {
                std::cerr << "Stdin: " << msg << std::endl;
                msgs_for_parse.push_back(std::move(msg.to_string()));
            }

            std::vector<std::string> identifiers{};
            nl::json header{};
            nl::json parent_header{};
            nl::json metadata{};
            nl::json content{};
            std::vector<std::string> buffers{};

            if(!current_session->parse_message(std::move(msgs_for_parse),
                                               identifiers, header,
                                               parent_header, metadata,
                                               content, buffers)) {
                throw std::runtime_error("bad answer from stdin.");
            }

            if(header["msg_type"] != "input_reply") {
                throw std::runtime_error("bad answer from stdin.");
            }

            return std::move(content["value"]);
        }

        throw std::runtime_error("there is no answer from stdin");
    }

    static auto unimpl(const std::string &promt = "") -> std::string {
        throw std::runtime_error{"The kernel doesn\'t support an input "
                                 "requests"};
    }

    input_redirection::input_redirection(
        boost::intrusive_ptr<zmq_socket_shared> stdin_socket,
        boost::intrusive_ptr<session> current_session,
        std::vector<std::string> parent_identifiers, nl::json parent_header,
        bool allow_stdin
    ) {
        auto builtins_m = py::module::import("builtins");

        m_input = builtins_m.attr("input");
        builtins_m.attr("input") = allow_stdin
            ? py::cpp_function([stdin_socket, current_session,
                                parent_identifiers, parent_header](
                                   const std::string &promt = ""
                               ) {
                                   return input_request(
                                       std::move(stdin_socket),
                                       std::move(current_session),
                                       std::move(parent_identifiers),
                                       std::move(parent_header), promt, false
                                   );
                               }, "prompt"_a = "")
            : py::cpp_function(&unimpl, "prompt"_a = "");

        auto getpass_m = py::module::import("getpass");

        m_getpass = getpass_m.attr("getpass");
        getpass_m.attr("getpass") = allow_stdin
            ? py::cpp_function([stdin_socket, current_session,
                                parent_identifiers, parent_header](
                                   const std::string &promt = ""
                               ) {
                                   return input_request(
                                       std::move(stdin_socket),
                                       std::move(current_session),
                                       std::move(parent_identifiers),
                                       std::move(parent_header), promt, true
                                   );
                               }, "prompt"_a = "")
            : py::cpp_function(&unimpl, "prompt"_a = "");
    }

    input_redirection::~input_redirection() {
        py::module::import("builtins").attr("input") = m_input;
        py::module::import("getpass").attr("getpass") = m_getpass;
    }

    interpreter_impl::interpreter_impl(
        std::string signature_key, std::string signature_scheme,
        zmq::socket_t shell_socket, zmq::socket_t control_socket,
        boost::optional<zmq::socket_t> stdin_socket, zmq::socket_t iopub_socket,
        boost::optional<zmq::socket_t> heartbeat_socket,
        boost::optional<zmq::socket_t> registration_socket,
        bool engine_mode, boost::uuids::uuid identifier
    )
        : shell_socket{std::move(shell_socket)}
        , control_socket{std::move(control_socket)}
        , iopub_socket{boost::intrusive_ptr<zmq_socket_shared>{
              new zmq_socket_shared{std::move(iopub_socket)}
          }}
        , heartbeat_socket{std::move(heartbeat_socket)}
        , registration_socket{std::move(registration_socket)}
        , engine_mode{engine_mode}
        , identifier{std::move(identifier)}
        , engine_identifier{0}
        , parent_header(nl::json::object())
        , abort_all{false}
        , execution_count{1} {
        if(engine_mode) {
            current_session = boost::intrusive_ptr<session>{new session{
                std::move(signature_key), std::move(signature_scheme),
                this->identifier
            }};
            this->stdin_socket = this->iopub_socket;
        } else {
            current_session = boost::intrusive_ptr<session>{new session{
                std::move(signature_key), std::move(signature_scheme)
            }};
            this->stdin_socket = boost::intrusive_ptr<zmq_socket_shared>{
                new zmq_socket_shared{std::move(*stdin_socket)}
            };
        }

        auto pykernel{py::module::import("pyrocketjoe.pykernel")};

        shell = pykernel.attr("RocketJoeShell")(
            "_init_location_id"_a = "shell.py:1"
        );
        shell.attr("displayhook").attr("current_session") = current_session;
        shell.attr("displayhook").attr("iopub_socket") = this->iopub_socket;
        shell.attr("display_pub").attr("current_session") = current_session;
        shell.attr("display_pub").attr("iopub_socket") = this->iopub_socket;

        auto zmq_ostream{pykernel.attr("ZMQOstream")};
        auto new_stdout{zmq_ostream()};

        new_stdout.attr("current_session") = current_session;
        new_stdout.attr("iopub_socket") = this->iopub_socket;

        std::string out_name{"stdout"};

        new_stdout.attr("name") = out_name;

        auto new_stderr{zmq_ostream()};

        new_stderr.attr("current_session") = current_session;
        new_stderr.attr("iopub_socket") = this->iopub_socket;

        std::string err_name{"stderr"};

        new_stderr.attr("name") = err_name;

        if(engine_mode) {
            shell.attr("displayhook").attr("topic") = "engine." +
                std::to_string(engine_identifier) + topic(".execute_result");
            shell.attr("display_pub").attr("topic") = "engine." +
                std::to_string(engine_identifier) + topic(".displaypub");
            new_stdout.attr("topic") = "engine." +
                std::to_string(engine_identifier) + std::move(out_name);
            new_stderr.attr("topic") = "engine." +
                std::to_string(engine_identifier) + std::move(err_name);
        } else {
            shell.attr("displayhook").attr("topic") = topic("execute_result");
            shell.attr("display_pub").attr("topic") = topic("display_data");
            new_stdout.attr("topic") = "stream." + std::move(out_name);
            new_stderr.attr("topic") = "stream." + std::move(err_name);
        }

        display_hook displayhook{current_session, this->iopub_socket};

        auto sys{py::module::import("sys")};

        pystdout = sys.attr("stdout");
        pystderr = sys.attr("stderr");

        pystdout.attr("flush")();
        pystderr.attr("flush")();

        sys.attr("stdout") = std::move(new_stdout);
        sys.attr("stderr") = std::move(new_stderr);
        sys.attr("displayhook") = std::move(displayhook);

        if(!engine_mode) {
            publish_status("starting", {});
        }
    }

    interpreter_impl::~interpreter_impl() {
        auto sys{py::module::import("sys")};

        sys.attr("stdout") = pystdout;
        sys.attr("stderr") = pystderr;
    }

    auto interpreter_impl::registration() -> bool {
        current_session->send(*registration_socket,
            current_session->construct_message(
                {}, {{"msg_type", "registration_request"}}, {}, {},
                {{"uuid", boost::uuids::to_string(identifier)},
                 {"id", engine_identifier}}, {}
            )
        );

        std::vector<zmq::message_t> msgs{};

        if(!zmq::recv_multipart(*registration_socket,
                                std::back_inserter(msgs))) {
            return false;
        }

        std::vector<std::string> msgs_for_parse{};

        msgs_for_parse.reserve(msgs.size());

        for(const auto &msg : msgs) {
            std::cerr << "Registration: " << msg << std::endl;
            msgs_for_parse.push_back(std::move(msg.to_string()));
        }

        std::vector<std::string> identifiers{};
        nl::json header{};
        nl::json parent_header{};
        nl::json metadata{};
        nl::json content{};
        std::vector<std::string> buffers{};

        if(!current_session->parse_message(std::move(msgs_for_parse),
                                           identifiers, header, parent_header,
                                           metadata, content, buffers)) {
            return true;
        }

        if(header["msg_type"].get<std::string>() != "registration_reply") {
            return false;
        } else if(content["status"].get<std::string>() != "ok") {
            return false;
        } else {
            engine_identifier = content["id"].get<std::uint64_t>();
        }

        publish_status("starting", {});

        return true;
    }

    auto interpreter_impl::poll(poll_flags polls) -> bool {
        if(!engine_mode) {
            if((polls & poll_flags::heartbeat_socket) ==
                poll_flags::heartbeat_socket) {
                std::vector<zmq::message_t> msgs{};

                while(zmq::recv_multipart(*heartbeat_socket,
                                          std::back_inserter(msgs),
                                          zmq::recv_flags::dontwait)) {
                    for(const auto &msg : msgs) {
                        std::cerr << "Heartbeat: " << msg << std::endl;
                    }

                    zmq::send_multipart(*heartbeat_socket, std::move(msgs),
                                        zmq::send_flags::dontwait);
                }
            }
        }

        if((polls & poll_flags::control_socket) == poll_flags::control_socket) {
            std::vector<zmq::message_t> msgs{};

            while(zmq::recv_multipart(control_socket, std::back_inserter(msgs),
                                      zmq::recv_flags::dontwait)) {
                std::vector<std::string> msgs_for_parse{};

                msgs_for_parse.reserve(msgs.size());

                for(const auto &msg : msgs) {
                    std::cerr << "Control: " << msg << std::endl;
                    msgs_for_parse.push_back(std::move(msg.to_string()));
                }

                if(!dispatch_control(std::move(msgs_for_parse))) {
                    return false;
                }
            }
        }

        if((polls & poll_flags::shell_socket) == poll_flags::shell_socket) {
            std::vector<zmq::message_t> msgs{};

            while(zmq::recv_multipart(shell_socket, std::back_inserter(msgs),
                                      zmq::recv_flags::dontwait)) {
                std::vector<std::string> msgs_for_parse{};

                msgs_for_parse.reserve(msgs.size());

                for(const auto &msg : msgs) {
                    std::cerr << "Shell: " << msg << std::endl;
                    msgs_for_parse.push_back(std::move(msg.to_string()));
                }

                if(!dispatch_shell(std::move(msgs_for_parse))) {
                    return false;
                }
            }
        }

        abort_all = false;

        return true;
    }

    auto interpreter_impl::topic(std::string topic) const -> std::string {
        if(engine_mode) {
            return "engine." + std::to_string(engine_identifier);
        } else {
            return "kernel." + boost::uuids::to_string(identifier) + "."
                             + topic;
        }
    }

    auto interpreter_impl::publish_status(std::string status,
                                          nl::json parent) -> void {
        if(parent.is_null()) {
            parent = parent_header;
        }

        current_session->send(**iopub_socket,
            current_session->construct_message(
                {topic("status")}, {{"msg_type", "status"}}, std::move(parent),
                {}, {{"execution_state", std::move(status)}}, {}
            )
        );
    }

    auto interpreter_impl::set_parent(std::vector<std::string> identifiers,
                                      nl::json parent) -> void {
        parent_identifiers = std::move(identifiers);
        parent_header = parent;

        shell::set_parent(shell, py::module::import("json")
            .attr("loads")(parent.dump()));
    }

    auto interpreter_impl::init_metadata() -> nl::json {
        return {{"started", (boost::locale::format("{1,ftime='%FT%T%Ez'}") %
                             boost::locale::date_time{}).str(std::locale())},
                {"dependencies_met", true},
                {"engine", boost::uuids::to_string(identifier)}};
    }

    auto interpreter_impl::finish_metadata(nl::json& metadata,
                                           nl::json reply_content) {
        metadata["status"] = std::move(reply_content["status"]);

        if(metadata["status"].get<std::string>() == "error") {
            if(reply_content["ename"].get<std::string>() == "UnmetDependency") {
                metadata["dependencies_met"] = true;
            }

            metadata["engine_info"] = {{"engine_uuid",
                                        boost::uuids::to_string(identifier)},
                                       {"engine_id", engine_identifier}};
        }
    }

    auto interpreter_impl::do_execute(std::string code, bool silent,
                                      bool store_history,
                                      nl::json user_expressions,
                                      bool allow_stdin) -> nl::json {
        nl::json execute_reply{};

        // Scope guard performing the temporary monkey patching of input and
        // getpass with a function sending input_request messages.
        input_redirection input_guard{stdin_socket, current_session,
                                      parent_identifiers, parent_header,
                                      allow_stdin};

        auto result = shell.attr("run_cell")(std::move(code), store_history,
                                             silent);

        if(py::hasattr(result, "execution_count")) {
            execution_count = result.attr("execution_count")
                .cast<std::size_t>() - 1;
        }

        execute_reply["execution_count"] = execution_count;
        py::module::import("sys").attr("displayhook")
            .attr("set_execution_count")(execution_count);

        auto json = py::module::import("json");
        auto json_loads = json.attr("loads");
        auto json_dumps = json.attr("dumps");

        if(result.attr("success").cast<bool>()) {
            execute_reply["status"] = "ok";
            execute_reply["user_expressions"] = nl::json::object();
            execute_reply["user_expressions"] = nl::json::parse(json_dumps(
                shell.attr("user_expressions")(json_loads(
                    user_expressions.dump()
                ))
            ).cast<std::string>());
        } else {
            py::dict error = shell.attr("_user_obj_error")();

            execute_reply["status"] = "error";
            execute_reply["traceback"] = error["traceback"]
                .cast<std::vector<std::string>>();
            execute_reply["ename"] = error["ename"].cast<std::string>();
            execute_reply["evalue"] = error["evalue"].cast<std::string>();
        }

        execute_reply["payload"] = nl::json::array();
        execute_reply["payload"] = nl::json::parse(json_dumps(
            shell.attr("payload_manager").attr("read_payload")()
        ).cast<std::string>());
        shell.attr("payload_manager").attr("clear_payload")();

        return std::move(execute_reply);
    }

    auto interpreter_impl::execute_request(zmq::socket_t &socket,
                                           std::vector<std::string> identifiers,
                                           nl::json parent) -> void {
        auto content = std::move(parent["content"]);
        auto code{content["code"].get<std::string>()};
        auto silent{content["silent"].get<boost::optional<bool>>()};

        if(!silent) {
            silent = false;
        }

        auto store_history{content["store_history"]
            .get<boost::optional<bool>>()};

        if(!store_history) {
            store_history = !*silent;
        }

        auto execution_count = shell.attr("execution_count")
            .cast<std::size_t>();

        auto allow_stdin{content["allow_stdin"].get<boost::optional<bool>>()};

        if(!allow_stdin) {
            allow_stdin = true;
        }

        auto stop_on_error{content["stop_on_error"]
            .get<boost::optional<bool>>()};

        if(!stop_on_error) {
            stop_on_error = true;
        }

        if(!*silent) {
            current_session->send(**iopub_socket,
                current_session->construct_message(
                    {topic("execute_input")}, {{"msg_type", "execute_input"}},
                    parent, {}, {
                        {"code", code}, {"execution_count", execution_count}
                    }, {}
                )
            );
        }

        auto metadata = init_metadata();
        auto reply_content = do_execute(std::move(code), *silent,
                                        *store_history,
                                        std::move(content["user_expressions"]),
                                        *allow_stdin);
        auto sys{py::module::import("sys")};

        sys.attr("stdout").attr("flush")();
        sys.attr("stderr").attr("flush")();

        finish_metadata(metadata, reply_content);

        if(!*silent && reply_content["status"].get<std::string>() == "error" &&
           *stop_on_error) {
           abort_all = true;
        }

        current_session->send(socket, current_session->construct_message(
            std::move(identifiers), {{"msg_type", "execute_reply"}},
            std::move(parent), metadata, std::move(reply_content), {}
        ));
    }

    auto interpreter_impl::do_inspect(std::string code, std::size_t cursor_pos,
                                      std::size_t detail_level) -> nl::json {
        using data_type = std::unordered_map<std::string, std::string>;

        auto found{false};
        data_type data{};
        auto name = py::module::import("IPython.utils.tokenutil").attr(
            "token_at_cursor"
        )(std::move(code), cursor_pos);

        try {
            data = shell.attr("object_inspect_mime")(name, detail_level)
                .cast<data_type>();

            if(!shell.attr("enable_html_pager").cast<bool>()) {
                data.erase("text/html");
            }

            found = true;
        } catch(const pybind11::error_already_set &error) {}

        return {{"status", "ok"}, {"found", found}, {"data", std::move(data)},
                {"metadata", nl::json::object()}};
    }

    auto interpreter_impl::inspect_request(zmq::socket_t &socket,
                                           std::vector<std::string> identifiers,
                                           nl::json parent) -> void {
        auto content = std::move(parent["content"]);

        current_session->send(socket, current_session->construct_message(
            std::move(identifiers), {{"msg_type", "inspect_reply"}},
            std::move(parent), {}, do_inspect(std::move(content["code"]),
                                              content["cursor_pos"],
                                              content["detail_level"]), {}
        ));
    }

    auto interpreter_impl::do_complete(std::string code,
                                       std::size_t cursor_pos) -> nl::json {
        auto completer{py::module::import("IPython.core.completer")};
        auto provisionalcompleter{completer.attr("provisionalcompleter")};
        auto completer_completions{shell.attr("Completer").attr("completions")};
        auto rectify_completions{completer.attr("rectify_completions")};
        auto locals{py::dict(
            "provisionalcompleter"_a = std::move(provisionalcompleter),
            "completer_completions"_a = std::move(completer_completions),
            "rectify_completions"_a = std::move(rectify_completions),
            "code"_a = std::move(code), "cursor_pos"_a = cursor_pos
        )};

        py::exec(R"(
            with provisionalcompleter():
                completions = completer_completions(code, cursor_pos)
                completions = list(rectify_completions(code, completions))
                completions = [{'start': completion.start,
                                'end': completion.end, 'text': completion.text,
                                'type': completion.type}
                               for completion in completions]
        )", py::globals(), locals);

        auto py_completions{locals["completions"]
            .cast<std::vector<py::dict>>()};
        std::vector<nl::json> matches{};
        std::vector<nl::json> completions{};

        matches.reserve(py_completions.size());
        completions.reserve(py_completions.size());

        auto cursor_start{cursor_pos};
        auto is_first{true};

        for(const auto &py_completion : py_completions) {
            auto start{py_completion["start"].cast<std::size_t>()};
            auto end{py_completion["end"].cast<std::size_t>()};

            if(is_first) {
                cursor_start = start;
                cursor_pos = end;
                is_first = false;
            }

            auto text{py_completion["text"].cast<std::string>()};

            matches.push_back(text);
            completions.push_back({
                {"start", start}, {"end", end}, {"text", std::move(text)},
                {"type", py_completion["type"].cast<std::string>()}
            });
        }

        return {{"matches", std::move(matches)}, {"cursor_start", cursor_start},
                {"cursor_end", cursor_pos}, {"metadata", {{
                    "_jupyter_types_experimental", std::move(completions)
                }}}, {"status", "ok"}};
    }

    auto interpreter_impl::complete_request(
        zmq::socket_t &socket, std::vector<std::string> identifiers,
        nl::json parent
    ) -> void {
        auto content = std::move(parent["content"]);

        current_session->send(socket, current_session->construct_message(
            std::move(identifiers), {{"msg_type", "complete_reply"}},
            std::move(parent), {}, do_complete(std::move(content["code"]),
                                               content["cursor_pos"]), {}
        ));
    }

    auto interpreter_impl::history_request(
        bool output, bool raw, const std::string &hist_access_type,
        std::int64_t session, std::size_t start,
        boost::optional<std::size_t> stop, boost::optional<std::size_t> n,
        const boost::optional<std::string> &pattern, bool unique
    ) -> nl::json {
        using history_type = std::vector<std::tuple<
            std::size_t, std::size_t, boost::variant<
                std::string, std::tuple<std::string,
                                        boost::optional<std::string>>
            >
        >>;

        history_type history{};

        if(hist_access_type == "tail") {
            history = py::list(shell.attr("history_manager").attr("get_tail")(
                n, raw, output, "include_latest"_a = true
            )).cast<history_type>();
        } else if(hist_access_type == "range") {
            history = py::list(shell.attr("history_manager").attr("get_range")(
                session, start, stop, raw, output
            )).cast<history_type>();
        } else if(hist_access_type == "range") {
            history = py::list(shell.attr("search").attr("search")(
                pattern, raw, output, n, unique
            )).cast<history_type>();
        }

        return {{"status", "ok"}, {"history", std::move(history)}};
    }

    auto interpreter_impl::do_is_complete(std::string code) -> nl::json {
        boost::optional<std::string> indent{};
        auto result{shell.attr("input_transformer_manager")
            .attr("check_complete")(std::move(code))
            .cast<std::tuple<std::string, boost::optional<std::size_t>>>()};
        auto status{std::get<0>(result)};

        if(status == "incomplete") {
            indent = std::string(*std::get<1>(result), ' ');
        }

        return {{"status", std::move(status)}, {"indent", std::move(indent)}};
    }

    auto interpreter_impl::is_complete_request(
        zmq::socket_t &socket, std::vector<std::string> identifiers,
        nl::json parent
    ) -> void {
        current_session->send(socket, current_session->construct_message(
            std::move(identifiers), {{"msg_type", "is_complete_reply"}},
            std::move(parent), {}, do_is_complete(
                std::move(parent["content"]["code"])
            ), {}
        ));
    }

    auto interpreter_impl::do_kernel_info() -> nl::json {
        auto sys{py::module::import("sys")};

        return {{"protocol_version", "5.3"},
                {"implementation", "ipython"},
                {"implementation_version", "ipython"},
                {"language_info", {
                    {"name", "python"},
                    {"version", sys.attr("version").attr("split")()
                                    .cast<std::vector<std::string>>()[0]},
                    {"mimetype", "text/x-python"},
                    {"codemirror_mode", {
                        {"name", "ipython"},
                        {"version", sys.attr("version_info")
                                        .attr("major").cast<std::size_t>()}
                    }},
                    {"pygments_lexer", "ipython3"},
                    {"nbconvert_exporter", "python"}, {"file_extension", ".py"}
               }}, {"banner", shell.attr("banner").cast<std::string>()},
               {"help_links", ""}};
    }

    auto interpreter_impl::kernel_info_request(
        zmq::socket_t &socket, std::vector<std::string> identifiers,
        nl::json parent
    ) -> void {
        current_session->send(socket, current_session->construct_message(
            std::move(identifiers), {{"msg_type", "kernel_info_reply"}},
            std::move(parent), {}, do_kernel_info(), {}
        ));
    }

    auto interpreter_impl::do_shutdown(bool restart) -> nl::json {
        return {{"restart", restart}};
    }

    auto interpreter_impl::shutdown_request(
        zmq::socket_t &socket, std::vector<std::string> identifiers,
        nl::json parent
    ) -> void {
        auto content = do_shutdown(parent["content"]["restart"]);

        current_session->send(socket, current_session->construct_message(
            std::move(identifiers), {{"msg_type", "shutdown_reply"}}, parent,
            {}, content, {}
        ));
        current_session->send(**iopub_socket,
            current_session->construct_message(
                {topic("shutdown")}, {{"msg_type", "shutdown_reply"}},
                std::move(parent), {}, std::move(content), {}
            )
        );
    }

    auto interpreter_impl::do_interrupt() -> nl::json {
        return nl::json::object();
    }

    auto interpreter_impl::interrupt_request(
        zmq::socket_t &socket, std::vector<std::string> identifiers,
        nl::json parent
    ) -> void {
        current_session->send(socket, current_session->construct_message(
            std::move(identifiers), {{"msg_type", "interrupt_reply"}},
            std::move(parent), {}, do_interrupt(), {}
        ));
    }

    auto interpreter_impl::do_clear() -> nl::json {
        shell.attr("reset")(false);

        return {{"status", "ok"}};
    }

    auto interpreter_impl::clear_request(
        zmq::socket_t &socket, std::vector<std::string> identifiers,
        nl::json parent
    ) -> void {
        current_session->send(socket, current_session->construct_message(
            std::move(identifiers), {{"msg_type", "clear_reply"}},
            std::move(parent), {}, do_clear(), {}
        ));
    }

    auto interpreter_impl::do_abort(
        boost::optional<std::vector<std::string>> identifiers
    ) -> nl::json {
        if(identifiers) {
            aborted.insert(std::make_move_iterator(identifiers->cbegin()),
                           std::make_move_iterator(identifiers->cend()));
        } else {
            abort_all = true;
        }

        return {{"status", "ok"}};
    }

    auto interpreter_impl::abort_request(
        zmq::socket_t &socket, std::vector<std::string> identifiers,
        nl::json parent
    ) -> void {
        current_session->send(socket, current_session->construct_message(
            std::move(identifiers), {{"msg_type", "abort_reply"}},
            std::move(parent), {}, do_abort(parent["content"]["msg_ids"]), {}
        ));
    }

    auto interpreter_impl::abort_reply(zmq::socket_t &socket,
                                       std::vector<std::string> identifiers,
                                       nl::json parent) -> void {
        auto msg_type{parent["header"]["msg_type"].get<std::string>()};

        current_session->send(socket, current_session->construct_message(
            std::move(identifiers), {{"msg_type", std::string{
                std::make_move_iterator(std::make_reverse_iterator(
                    msg_type.crend()
                )),
                std::make_move_iterator(std::make_reverse_iterator(
                    std::find(msg_type.crbegin(), msg_type.crend(), '_')
                ))
            }}}, std::move(parent), {
                {"status", "aborted"},
                {"engine", boost::uuids::to_string(identifier)}
            }, {{"status", "aborted"}}, {}
        ));
    }

    auto interpreter_impl::dispatch_shell(
        std::vector<std::string> msgs
    ) -> bool {
        std::vector<std::string> identifiers{};
        nl::json header{};
        nl::json parent_header{};
        nl::json metadata{};
        nl::json content{};
        std::vector<std::string> buffers{};

        if(!current_session->parse_message(std::move(msgs), identifiers, header,
                                           parent_header, metadata, content,
                                           buffers)) {
            return true;
        }

        auto msg_type{header["msg_type"].get<std::string>()};
        auto msg_id{header["msg_id"].get<std::string>()};
        nl::json parent{{"header", std::move(header)},
                        {"parent_header", std::move(parent_header)},
                        {"metadata", std::move(metadata)},
                        {"content", std::move(content)},
                        {"buffers", std::move(buffers)}};

        set_parent(identifiers, parent);
        publish_status("busy", {});

        if(abort_all) {
            abort_reply(shell_socket, std::move(identifiers),
                        std::move(parent));
            publish_status("idle", {});

            return true;
        }

        auto abort{std::find(aborted.cbegin(), aborted.cend(),
                   std::move(msg_id))};

        if(abort != aborted.cend()) {
            aborted.erase(abort);
            abort_reply(shell_socket, std::move(identifiers),
                        std::move(parent));
            publish_status("idle", {});

            return true;
        }

        auto resume{true};

        if(msg_type == "execute_request") {
            execute_request(shell_socket, std::move(identifiers),
                            std::move(parent));
        } else if(msg_type == "inspect_request") {
            inspect_request(shell_socket, std::move(identifiers),
                            std::move(parent));
        } else if(msg_type == "complete_request") {
            complete_request(shell_socket, std::move(identifiers),
                             std::move(parent));
        } else if(msg_type == "is_complete_request") {
            is_complete_request(shell_socket, std::move(identifiers),
                                std::move(parent));
        } else if(msg_type == "kernel_info_request") {
            kernel_info_request(shell_socket, std::move(identifiers),
                                std::move(parent));
        } else if(msg_type == "shutdown_request") {
            shutdown_request(shell_socket, std::move(identifiers),
                             std::move(parent));
            resume = false;
        } else if(msg_type == "interrupt_request") {
            interrupt_request(shell_socket, std::move(identifiers),
                              std::move(parent));
            resume = false;
        }

        auto sys{py::module::import("sys")};

        sys.attr("stdout").attr("flush")();
        sys.attr("stderr").attr("flush")();

        if(resume) {
            publish_status("idle", {});
        }

        return resume;
    }

    auto interpreter_impl::dispatch_control(
        std::vector<std::string> msgs
    ) -> bool {
        std::vector<std::string> identifiers{};
        nl::json header{};
        nl::json parent_header{};
        nl::json metadata{};
        nl::json content{};
        std::vector<std::string> buffers{};

        if(!current_session->parse_message(std::move(msgs), identifiers, header,
                                           parent_header, metadata, content,
                                           buffers)) {
            return true;
        }

        auto msg_type{header["msg_type"].get<std::string>()};
        nl::json parent{{"header", std::move(header)},
                        {"parent_header", std::move(parent_header)},
                        {"metadata", std::move(metadata)},
                        {"content", std::move(content)},
                        {"buffers", std::move(buffers)}};

        set_parent(identifiers, parent);
        publish_status("busy", {});

        if(abort_all) {
            abort_reply(control_socket, std::move(identifiers),
                        std::move(parent));
            publish_status("idle", {});

            return true;
        }

        auto resume{true};

        if(msg_type == "execute_request") {
            execute_request(control_socket, std::move(identifiers),
                            std::move(parent));
        } else if(msg_type == "inspect_request") {
            inspect_request(control_socket, std::move(identifiers),
                            std::move(parent));
        } else if(msg_type == "complete_request") {
            complete_request(control_socket, std::move(identifiers),
                             std::move(parent));
        } else if(msg_type == "is_complete_request") {
            is_complete_request(control_socket, std::move(identifiers),
                                std::move(parent));
        } else if(msg_type == "kernel_info_request") {
            kernel_info_request(control_socket, std::move(identifiers),
                                std::move(parent));
        } else if(msg_type == "shutdown_request") {
            shutdown_request(control_socket, std::move(identifiers),
                             std::move(parent));
            resume = false;
        } else if(msg_type == "interrupt_request") {
            interrupt_request(control_socket, std::move(identifiers),
                              std::move(parent));
            resume = false;
        } else if(msg_type == "clear_request") {
            clear_request(control_socket, std::move(identifiers),
                          std::move(parent));
        } else if(msg_type == "abort_request") {
            abort_request(control_socket, std::move(identifiers),
                          std::move(parent));
        }

        auto sys{py::module::import("sys")};

        sys.attr("stdout").attr("flush")();
        sys.attr("stderr").attr("flush")();

        if(resume) {
            publish_status("idle", {});
        }

        return resume;
    }

    pykernel::pykernel(std::string signature_key,
                       std::string signature_scheme,
                       zmq::socket_t shell_socket,
                       zmq::socket_t control_socket,
                       boost::optional<zmq::socket_t> stdin_socket,
                       zmq::socket_t iopub_socket,
                       boost::optional<zmq::socket_t> heartbeat_socket,
                       boost::optional<zmq::socket_t> registration_socket,
                       bool engine_mode, boost::uuids::uuid identifier)
        : pimpl{std::make_unique<interpreter_impl>(
                std::move(signature_key), std::move(signature_scheme),
                std::move(shell_socket), std::move(control_socket),
                std::move(stdin_socket), std::move(iopub_socket),
                std::move(heartbeat_socket), std::move(registration_socket),
                engine_mode, std::move(identifier)
          )} {}

    pykernel::~pykernel() = default;

    auto pykernel::registration() -> bool {
        return pimpl->registration();
    }

    auto pykernel::poll(poll_flags polls) -> bool {
        return pimpl->poll(polls);
    }

}}}}
