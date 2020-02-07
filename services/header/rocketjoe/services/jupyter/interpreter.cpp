#include "rocketjoe/services/jupyter/interpreter.hpp"

#include <unistd.h>
#include <pwd.h>

#include <iostream>

#include <algorithm>
#include <cassert>
#include <iterator>
#include <locale>
#include <memory>
#include <stdexcept>
#include <string>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>

#include <boost/locale/date_time.hpp>
#include <boost/locale/format.hpp>
#include <boost/optional/optional.hpp>
#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>
#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <boost/variant/variant.hpp>

#include <nlohmann/json.hpp>

#include <botan/hex.h>
#include <botan/mac.h>

#include <pybind11/embed.h>
#include <pybind11/functional.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include <zmq_addon.hpp>

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

PYBIND11_DECLARE_HOLDER_TYPE(T, boost::intrusive_ptr<T>);

namespace rocketjoe {
    namespace nl = nlohmann;
    namespace py = pybind11;

    using namespace pybind11::literals;

    class zmq_socket_shared final
        : public boost::intrusive_ref_counter<zmq_socket_shared> {
    public:
        zmq_socket_shared(zmq::socket_t socket);

        zmq::socket_t &operator*();

    private:
        zmq::socket_t socket;
    };

    class session final : public boost::intrusive_ref_counter<session> {
    public:
        session(std::string signature_key, std::string signature_scheme);

        session(const session &) = delete;

        session &operator=(const session &) = delete;

        auto parse_message(std::vector<std::string> msgs,
                           std::vector<std::string> &identifiers,
                           nl::json &header, nl::json &parent_header,
                           nl::json &metadata, nl::json &content) const -> bool;

        auto construct_message(std::vector<std::string> identifiers,
                               nl::json header, nl::json parent,
                               nl::json metadata,
                               nl::json content) -> std::vector<std::string>;

        auto send(zmq::socket_t &socket,
                  std::vector<std::string> msgs) const -> void;

    private:
        auto compute_signature(std::string header, std::string parent_header,
                               std::string metadata,
                               std::string content) const -> std::string;

        constexpr static auto version{"5.3"};
        constexpr static auto delimiter{"<IDS|MSG>"};
        boost::uuids::uuid session_id;
        std::size_t message_count;
        std::string signature_key;
        std::string signature_scheme;
    };

    class BOOST_SYMBOL_VISIBLE input_redirection final {
    public:
        input_redirection(
            boost::intrusive_ptr<zmq_socket_shared> stdin_socket,
            boost::intrusive_ptr<rocketjoe::session> current_session,
            std::vector<std::string> parent_identifiers, nl::json parent_header,
            bool allow_stdin
        );

        ~input_redirection();

    private:
        py::object m_input;
        py::object m_getpass;
    };

    class zmq_ostream final {
    public:
        static auto set_parent(py::object self, py::dict parent) -> void;

        static auto writable(py::object self) -> bool;

        static auto write(py::object self, py::str string) -> void;

        static auto flush(py::object self) -> void;
    };

    class display_hook final {
    public:
        static auto set_parent(py::object self, py::dict parent) -> void;

        static auto start_displayhook(py::object self) -> void;

        static auto write_output_prompt(py::object self) -> void;

        static auto write_format_data(py::object self, py::dict data,
                                      py::dict metadata) -> void;

        static auto finish_displayhook(py::object self) -> void;
    };

    class display_publisher final {
    public:
        static auto set_parent(py::object self, py::dict parent) -> void;

        static auto publish(py::object self, py::dict data, py::dict metadata,
                            py::str source, py::dict trasistent,
                            py::bool_ update) -> void;

        static auto clear_output(py::object self,
                                 py::bool_ wait = false) -> void;
    };

    class shell final {
    public:
        static auto _default_banner1(py::object self) -> py::object;

        static auto _default_exiter(py::object self) -> py::object;

        static auto init_hooks(py::object self) -> void;

        static auto ask_exit(py::object self) -> void;

        static auto run_cell(py::object self, py::args args,
                             py::kwargs kwargs) -> py::object;

        static auto _showtraceback(py::object self, py::object etype,
                                   py::object evalue, py::object stb) -> void;

        static auto set_next_input(py::object self, py::object text,
                                   py::bool_ replace = true) -> void;

        static auto set_parent(py::object self, py::dict parent) -> void;

        static auto init_environment(py::object self) -> void;

        static auto init_virtualenv(py::object self) -> void;
    };

    class BOOST_SYMBOL_VISIBLE interpreter_impl final {
    public:
        interpreter_impl(std::string signature_key,
                         std::string signature_scheme,
                         zmq::socket_t shell_socket,
                         zmq::socket_t control_socket,
                         zmq::socket_t stdin_socket,
                         zmq::socket_t iopub_socket,
                         zmq::socket_t heartbeat_socket);

        ~interpreter_impl();

        interpreter_impl(const interpreter_impl &) = delete;

        interpreter_impl &operator=(const interpreter_impl &) = delete;

        auto poll(poll_flags polls) -> bool;

    private:
        auto topic(std::string topic) const -> std::string;

        auto publish_status(std::string status, nl::json) -> void;

        auto set_parent(std::vector<std::string> identifiers,
                        nl::json parent) -> void;

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

        auto dispatch_shell(std::vector<std::string> msgs) -> void;

        auto dispatch_control(std::vector<std::string> msgs) -> bool;

        zmq::socket_t shell_socket;
        zmq::socket_t control_socket;
        boost::intrusive_ptr<zmq_socket_shared> stdin_socket;
        boost::intrusive_ptr<zmq_socket_shared> iopub_socket;
        zmq::socket_t heartbeat_socket;
        boost::intrusive_ptr<rocketjoe::session> current_session;
        py::object shell;
        py::object pystdout;
        py::object pystderr;
        boost::uuids::uuid identifier;
        std::vector<std::string> parent_identifiers;
        nl::json parent_header;
    };

    zmq_socket_shared::zmq_socket_shared(zmq::socket_t socket)
        : socket{std::move(socket)} {}

    zmq::socket_t &zmq_socket_shared::operator*() { return socket; }

    session::session(std::string signature_key, std::string signature_scheme)
        : session_id{boost::uuids::random_generator()()}
        , message_count{0}
        , signature_key{std::move(signature_key)}
        , signature_scheme{std::move(signature_scheme)} {}

    auto session::parse_message(std::vector<std::string> msgs,
                                std::vector<std::string> &identifiers,
                                nl::json &header, nl::json &parent_header,
                                nl::json &metadata,
                                nl::json &content) const -> bool {
        auto split_position{std::find(msgs.cbegin(), msgs.cend(),
                                      std::string{delimiter})};

        identifiers = std::vector<std::string>{
            std::make_move_iterator(msgs.cbegin()),
            std::make_move_iterator(split_position)
        };

        std::vector<std::string> msgs_tail{
            std::make_move_iterator(split_position + 1),
            std::make_move_iterator(msgs.cend())
        };
        auto header_raw{std::move(msgs_tail[1])};
        auto parent_header_raw{std::move(msgs_tail[2])};
        auto metadata_raw{std::move(msgs_tail[3])};
        auto content_raw{std::move(msgs_tail[4])};
        auto signature{compute_signature(header_raw, parent_header_raw,
                                         metadata_raw, content_raw)};

        if(signature != msgs_tail[0]) {
            return false;
        }

        header = nl::json::parse(std::move(header_raw));
        parent_header = nl::json::parse(std::move(parent_header_raw));
        metadata = nl::json::parse(std::move(metadata_raw));
        content = nl::json::parse(std::move(content_raw));

        return true;
    }

    auto session::construct_message(
        std::vector<std::string> identifiers, nl::json header, nl::json parent,
        nl::json metadata, nl::json content
    ) -> std::vector<std::string> {
        nl::json parent_header = nl::json::object();

        if(parent.contains("header")) {
            parent_header = std::move(parent["header"]);
        }

        if(!header.contains("version")) {
            header["version"] = std::string{version};
        }

        if(!header.contains("username")) {
            std::string username{"root"};
            auto user_info{getpwuid(geteuid())};

            if(user_info) {
                username = user_info->pw_name;
            }

            header["username"] = std::move(username);
        }

        if(!header.contains("session")) {
            header["session"] = boost::uuids::to_string(session_id);
        }

        if(!header.contains("msg_id")) {
            header["msg_id"] = boost::uuids::to_string(session_id) + "_"
                             + std::to_string(message_count++);
        }

        if(!header.contains("date")) {
            header["date"] = (boost::locale::format("{1,ftime='%FT%T%Ez'}") %
                              boost::locale::date_time{}).str(std::locale());
        }

        if(metadata.is_null()) {
            metadata = nl::json::object();
        }

        if(content.is_null()) {
            content = nl::json::object();
        }

        auto header_raw{header.dump()};
        auto parent_header_raw{parent_header.dump()};
        auto metadata_raw{metadata.dump()};
        auto content_raw{content.dump()};
        auto signature{compute_signature(header_raw, parent_header_raw,
                                         metadata_raw, content_raw)};

        std::vector<std::string> msgs{};

        msgs.reserve(identifiers.size() + 6);
        msgs.insert(msgs.end(), std::make_move_iterator(identifiers.begin()),
                    std::make_move_iterator(identifiers.end()));
        msgs.push_back(delimiter);
        msgs.push_back(std::move(signature));
        msgs.push_back(std::move(header_raw));
        msgs.push_back(std::move(parent_header_raw));
        msgs.push_back(std::move(metadata_raw));
        msgs.push_back(std::move(content_raw));

        return std::move(msgs);
    }

    auto session::send(zmq::socket_t &socket,
                       std::vector<std::string> msgs) const -> void {
        std::vector<zmq::const_buffer> msgs_for_send{};

        msgs_for_send.reserve(msgs.size());

        for(const auto &msg : msgs) {
            std::cerr << msg << std::endl;
            msgs_for_send.push_back(zmq::buffer(std::move(msg)));
        }

        assert(zmq::send_multipart(socket, std::move(msgs_for_send)));
    }

    auto session::compute_signature(std::string header,
                                    std::string parent_header,
                                    std::string metadata,
                                    std::string content) const -> std::string {
        std::unique_ptr<Botan::MessageAuthenticationCode> mac{};

        if(signature_scheme == "hmac-sha256") {
            mac = Botan::MessageAuthenticationCode::create("HMAC(SHA-256)");
        }

        if(!mac) {
            return {};
        }

        mac->set_key(std::vector<std::uint8_t>{signature_key.begin(),
                                               signature_key.end()});
        mac->start();
        mac->update(std::move(header));
        mac->update(std::move(parent_header));
        mac->update(std::move(metadata));
        mac->update(std::move(content));

        return Botan::hex_encode(mac->final(), false);
    }

    static auto input_request(
        boost::intrusive_ptr<zmq_socket_shared> stdin_socket,
        boost::intrusive_ptr<rocketjoe::session> current_session,
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
                                               {"password", password}}
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

            if(!current_session->parse_message(std::move(msgs_for_parse),
                                               identifiers, header,
                                               parent_header, metadata,
                                               content)) {
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
        boost::intrusive_ptr<rocketjoe::session> current_session,
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

    auto zmq_ostream::set_parent(py::object self, py::dict parent) -> void {
        self.attr("parent") = std::move(parent);
    }

    auto zmq_ostream::writable(py::object self) -> bool { return true; }

    auto zmq_ostream::write(py::object self, py::str string) -> void {
        auto current_session{self.attr("current_session")
            .cast<boost::intrusive_ptr<session>>()};

        current_session->send(**self.attr("iopub_socket")
            .cast<boost::intrusive_ptr<zmq_socket_shared>>(),
            current_session->construct_message(
                {self.attr("topic").cast<std::string>()},
                {{"msg_type", "stream"}},
                nl::json::parse(py::module::import("json").attr("dumps")(
                    self.attr("parent")
                ).cast<std::string>()), {},
                nl::json::parse(py::module::import("json").attr("dumps")(
                    py::dict("name"_a = self.attr("name").cast<std::string>(),
                             "text"_a = std::move(string))
                ).cast<std::string>())
            )
        );
    }

    auto zmq_ostream::flush(py::object self) -> void {}

    auto display_hook::set_parent(py::object self, py::dict parent) -> void {
        self.attr("parent") = std::move(parent);
    }

    auto display_hook::start_displayhook(py::object self) -> void {
        self.attr("msg") = py::dict("parent"_a = self.attr("parent"),
                                    "content"_a = py::dict());
    }

    auto display_hook::write_output_prompt(py::object self) -> void {
        self.attr("msg")["content"]["execution_count"] =
            self.attr("prompt_count");
    }

    auto display_hook::write_format_data(py::object self, py::dict data,
                                         py::dict metadata) -> void {
        self.attr("msg")["content"]["data"] = data;
        self.attr("msg")["content"]["metadata"] = metadata;
    }

    auto display_hook::finish_displayhook(py::object self) -> void {
        auto sys{py::module::import("sys")};

        sys.attr("stdout").attr("flush")();
        sys.attr("stderr").attr("flush")();

        auto msg = nl::json::parse(py::module::import("json").attr("dumps")(
            self.attr("msg")
        ).cast<std::string>());

        if(!msg["content"]["data"].is_null()) {
            auto current_session{self.attr("current_session")
                .cast<boost::intrusive_ptr<session>>()};

            current_session->send(**self.attr("iopub_socket")
                .cast<boost::intrusive_ptr<zmq_socket_shared>>(),
                current_session->construct_message(
                    {self.attr("topic").cast<std::string>()},
                    {{"msg_type", "execute_result"}}, std::move(msg["parent"]),
                    {}, std::move(msg["content"])
                )
            );
        }

        self.attr("msg") = py::none();
    }

    auto display_publisher::set_parent(py::object self,
                                       py::dict parent) -> void {
        self.attr("parent") = std::move(parent);
    }

    auto display_publisher::publish(py::object self, py::dict data,
                                    py::dict metadata, py::str source,
                                    py::dict trasistent,
                                    py::bool_ update) -> void {
        auto sys{py::module::import("sys")};

        sys.attr("stdout").attr("flush")();
        sys.attr("stderr").attr("flush")();

        if(!metadata) {
            metadata = py::dict();
        }

        self.attr("_validate_data")(data, metadata);

        if(!trasistent) {
            trasistent = py::dict();
        }

        auto current_session{self.attr("current_session")
            .cast<boost::intrusive_ptr<session>>()};

        current_session->send(**self.attr("iopub_socket")
            .cast<boost::intrusive_ptr<zmq_socket_shared>>(),
            current_session->construct_message(
                {self.attr("topic").cast<std::string>()},
                {{"msg_type", update ? "update_display_data" : "display_data"}},
                nl::json::parse(py::module::import("json").attr("dumps")(
                    self.attr("parent")
                ).cast<std::string>()), {},
                nl::json::parse(py::module::import("json").attr("dumps")(
                    py::dict("data"_a = std::move(data),
                             "metadata"_a = std::move(metadata),
                             "transistent"_a = std::move(trasistent))
                ).cast<std::string>())
            )
        );
    }

    auto display_publisher::clear_output(py::object self,
                                         py::bool_ wait) -> void {
        auto sys{py::module::import("sys")};

        sys.attr("stdout").attr("flush")();
        sys.attr("stderr").attr("flush")();

        auto current_session{self.attr("current_session")
            .cast<boost::intrusive_ptr<session>>()};

        current_session->send(**self.attr("iopub_socket")
            .cast<boost::intrusive_ptr<zmq_socket_shared>>(),
            current_session->construct_message(
                {self.attr("topic").cast<std::string>()},
                {{"msg_type", "clear_output"}},
                nl::json::parse(py::module::import("json").attr("dumps")(
                    self.attr("parent")
                ).cast<std::string>()), {},
                nl::json::parse(py::module::import("json").attr("dumps")(
                    py::dict("wait"_a = wait)
                ).cast<std::string>())
            )
        );
    }

    auto shell::_default_banner1(py::object self) -> py::object {
        return py::module::import("IPython.core.usage").attr("default_banner");
    }

    auto shell::_default_exiter(py::object self) -> py::object {
        return py::module::import("IPython.core.autocall")
            .attr("ZMQExitAutocall")(std::move(self));
    }

    auto shell::init_hooks(py::object self) -> void {
        py::module::import("IPython.core.interactiveshell")
            .attr("InteractiveShell").attr("init_hooks")(self);
        self.attr("set_hook")(
            "show_in_pager", py::module::import("IPython.core").attr("page")
                .attr("as_hook")(py::module::import("IPython.core.payloadpage")
                                    .attr("page")), 99
        );
    }

    auto shell::ask_exit(py::object self) -> void {
        auto keepkernel_on_exit{self.attr("keepkernel_on_exit")};

        self.attr("exit_now") = !keepkernel_on_exit.cast<py::bool_>();
        self.attr("payload_manager").attr("write_payload")(
            py::dict("source"_a = "ask_exit",
                     "keepkernel"_a = keepkernel_on_exit)
        );
    }

    auto shell::run_cell(py::object self, py::args args,
                         py::kwargs kwargs) -> py::object {
        self.attr("_last_traceback") = py::none();

        return py::module::import("IPython.core.interactiveshell")
            .attr("InteractiveShell").attr("run_cell")(self, *args, **kwargs);
    }

    auto shell::_showtraceback(py::object self, py::object etype,
                               py::object evalue, py::object stb) -> void {
        auto sys{py::module::import("sys")};

        sys.attr("stdout").attr("flush")();
        sys.attr("stderr").attr("flush")();

        auto displayhook{self.attr("displayhook")};
        auto topic{py::none()};

        if(!displayhook.attr("topic").is_none()) {
            topic = displayhook.attr("topic").attr("replace")("execute_result",
                                                              "error");
        }

        auto current_session{displayhook.attr("current_session")
            .cast<boost::intrusive_ptr<session>>()};

        current_session->send(**displayhook.attr("iopub_socket")
            .cast<boost::intrusive_ptr<zmq_socket_shared>>(),
            current_session->construct_message(
                {topic.cast<std::string>()}, {{"msg_type", "error"}},
                nl::json::parse(py::module::import("json").attr("dumps")(
                    displayhook.attr("parent")
                ).cast<std::string>()), {},
                nl::json::parse(py::module::import("json").attr("dumps")(
                    py::dict("traceback"_a = stb,
                             "ename"_a = etype.attr("__name__"),
                             "evalue"_a = py::str(std::move(evalue)))
                ).cast<std::string>())
            )
        );

        self.attr("_last_traceback") = std::move(stb);
    }

    auto shell::set_next_input(py::object self, py::object text,
                               py::bool_ replace) -> void {
        self.attr("payload_manager").attr("write_payload")(
            py::dict("source"_a = "set_next_input", "text"_a = std::move(text),
                     "replace"_a = replace)
        );
    }

    auto shell::set_parent(py::object self, py::dict parent) -> void {
        display_hook::set_parent(self.attr("displayhook"), parent);
        display_publisher::set_parent(self.attr("display_pub"), parent);

        auto sys{py::module::import("sys")};

        zmq_ostream::set_parent(sys.attr("stdout"), parent);
        zmq_ostream::set_parent(sys.attr("stderr"), parent);
    }

    auto shell::init_environment(py::object self) -> void {
        auto environ{py::module::import("os").attr("environ")};

        environ["TERM"] = "xterm-color";
        environ["CLICOLOR"] = "1";
        environ["PAGER"] = "cat";
        environ["GIT_PAGER"] = "cat";
    }

    auto shell::init_virtualenv(py::object self) -> void {}

    PYBIND11_EMBEDDED_MODULE(rocketjoe, rocketjoe) {
        auto pykernel{rocketjoe.def_submodule("pykernel")};
        auto type{py::reinterpret_borrow<py::object>((PyObject *)&PyType_Type)};
        auto traitlets{py::module::import("traitlets")};
        auto tl_type{traitlets.attr("Type")};
        auto tl_any{traitlets.attr("Any")};
        auto tl_cbool{traitlets.attr("CBool")};
        auto tl_instance{traitlets.attr("Instance")};
        auto tl_default{traitlets.attr("default")};
        auto text_io_base{py::module::import("io").attr("TextIOBase")};
        auto display_hook{py::module::import("IPython.core.displayhook")
            .attr("DisplayHook")};
        auto display_publisher{py::module::import("IPython.core.displaypub")
            .attr("DisplayPublisher")};
        auto shell_module{py::module::import("IPython.core.interactiveshell")};
        auto shell_abc{shell_module.attr("InteractiveShellABC")};
        auto shell{shell_module.attr("InteractiveShell")};
        auto zmq_exit_autocall{py::module::import("IPython.core.autocall")
            .attr("ZMQExitAutocall")};
        auto zmq_ostream_props{py::dict(
            "encoding"_a = "UTF-8",
            "writable"_a = py::cpp_function(&zmq_ostream::writable,
                                            py::is_method(py::none())),
            "write"_a = py::cpp_function(&zmq_ostream::write,
                                         py::is_method(py::none())),
            "flush"_a = py::cpp_function(&zmq_ostream::flush,
                                         py::is_method(py::none()))
        )};
        auto zmq_ostream{type("ZMQOstream",
                              py::make_tuple(std::move(text_io_base)),
                              std::move(zmq_ostream_props))};
        auto display_hook_props{py::dict(
            "parent"_a = tl_any("allow_none"_a = true),
            "start_displayhook"_a = py::cpp_function(
                &display_hook::start_displayhook, py::is_method(py::none())
            ),
            "write_output_prompt"_a = py::cpp_function(
                &display_hook::write_output_prompt, py::is_method(py::none())
            ),
            "write_format_data"_a = py::cpp_function(
                &display_hook::write_format_data, py::is_method(py::none())
            ),
            "finish_displayhook"_a = py::cpp_function(
                &display_hook::finish_displayhook, py::is_method(py::none())
            )
        )};
        auto rocketjoe_display_hook{type(
            "RocketJoeDisplayHook", py::make_tuple(std::move(display_hook)),
            std::move(display_hook_props)
        )};
        auto display_publisher_props{py::dict(
            "parent"_a = tl_any("allow_none"_a = true),
            "publish"_a = py::cpp_function(&display_publisher::publish,
                                           py::is_method(py::none())),
            "clear_output"_a = py::cpp_function(
                &display_publisher::clear_output, py::is_method(py::none())
            )
        )};
        auto rocketjoe_display_publisher{type(
            "RocketJoeDisplayPublisher",
            py::make_tuple(std::move(display_publisher)),
            std::move(display_publisher_props)
        )};
        auto shell_props{py::dict(
            "displayhook_class"_a = tl_type(rocketjoe_display_hook),
            "display_pub_class"_a = tl_type(rocketjoe_display_publisher),
            "readline_use"_a = tl_cbool(false),
            "autoindent"_a = tl_cbool(false),
            "exiter"_a = tl_instance(std::move(zmq_exit_autocall)),
            "keepkernel_on_exit"_a = py::none(),
            "_default_banner1"_a = tl_default("banner1")(
                py::cpp_function(&shell::_default_banner1,
                                 py::is_method(py::none()))
            ),
            "_default_exiter"_a = tl_default("exiter")(
                py::cpp_function(&shell::_default_exiter,
                                 py::is_method(py::none()))
            ),
            "init_hooks"_a = py::cpp_function(&shell::init_hooks,
                                              py::is_method(py::none())),
            "ask_exit"_a = py::cpp_function(&shell::ask_exit,
                                            py::is_method(py::none())),
            "run_cell"_a = py::cpp_function(&shell::run_cell,
                                            py::is_method(py::none())),
            "_showtraceback"_a = py::cpp_function(&shell::_showtraceback,
                                                  py::is_method(py::none())),
            "set_next_input"_a = py::cpp_function(&shell::set_next_input,
                                                  py::is_method(py::none())),
            "init_environment"_a = py::cpp_function(&shell::init_environment,
                                                    py::is_method(py::none())),
            "init_virtualenv"_a = py::cpp_function(&shell::init_virtualenv,
                                                   py::is_method(py::none()))
        )};
        auto rocketjoe_shell{type("RocketJoeShell",
                                  py::make_tuple(std::move(shell)),
                                  std::move(shell_props))};

        py::class_<zmq_socket_shared, boost::intrusive_ptr<zmq_socket_shared>>(
            pykernel, "ZMQSocket"
        );
        py::class_<session, boost::intrusive_ptr<session>>(pykernel,
                                                           "RocketJoeSession");
        pykernel.attr("ZMQOstream") = std::move(zmq_ostream);
        pykernel.attr("RocketJoeDisplayHook") =
            std::move(rocketjoe_display_hook);
        pykernel.attr("RocketJoeDisplayPublisher") =
            std::move(rocketjoe_display_publisher);
        pykernel.attr("RocketJoeShell") = rocketjoe_shell;
        shell_abc.attr("register")(std::move(rocketjoe_shell));
    }

    interpreter_impl::interpreter_impl(std::string signature_key,
                                       std::string signature_scheme,
                                       zmq::socket_t shell_socket,
                                       zmq::socket_t control_socket,
                                       zmq::socket_t stdin_socket,
                                       zmq::socket_t iopub_socket,
                                       zmq::socket_t heartbeat_socket)
        : shell_socket{std::move(shell_socket)}
        , control_socket{std::move(control_socket)}
        , stdin_socket{boost::intrusive_ptr<zmq_socket_shared>{
              new zmq_socket_shared{std::move(stdin_socket)}
          }}
        , iopub_socket{boost::intrusive_ptr<zmq_socket_shared>{
              new zmq_socket_shared{std::move(iopub_socket)}
          }}
        , heartbeat_socket{std::move(heartbeat_socket)}
        , current_session{boost::intrusive_ptr<rocketjoe::session>{
              new rocketjoe::session{std::move(signature_key),
                                     std::move(signature_scheme)}
          }}
        , identifier{boost::uuids::random_generator()()}
        , parent_header(nl::json::object()) {
        py::gil_scoped_acquire acquire{};

        shell = py::module::import("rocketjoe.pykernel").attr("RocketJoeShell")(
            "_init_location_id"_a = "shell.py:1"
        );
        shell.attr("displayhook").attr("current_session") = current_session;
        shell.attr("displayhook").attr("iopub_socket") = this->iopub_socket;
        shell.attr("displayhook").attr("topic") = topic("execute_result");
        shell.attr("display_pub").attr("current_session") = current_session;
        shell.attr("display_pub").attr("iopub_socket") = this->iopub_socket;

        auto zmq_ostream{py::module::import("rocketjoe.pykernel")
            .attr("ZMQOstream")};
        auto new_stdout{zmq_ostream()};

        new_stdout.attr("current_session") = current_session;
        new_stdout.attr("iopub_socket") = this->iopub_socket;

        std::string out_name{"stdout"};

        new_stdout.attr("topic") = "stream." + out_name;
        new_stdout.attr("name") = std::move(out_name);

        auto new_stderr{zmq_ostream()};

        new_stderr.attr("current_session") = current_session;
        new_stderr.attr("iopub_socket") = this->iopub_socket;

        std::string err_name{"stderr"};

        new_stderr.attr("topic") = "stream." + err_name;
        new_stderr.attr("name") = std::move(err_name);

        auto sys{py::module::import("sys")};

        pystdout = sys.attr("stdout");
        pystderr = sys.attr("stderr");

        pystdout.attr("flush")();
        pystderr.attr("flush")();

        sys.attr("stdout") = std::move(new_stdout);
        sys.attr("stderr") = std::move(new_stderr);

        publish_status("starting", {});
    }

    interpreter_impl::~interpreter_impl() {
        py::gil_scoped_acquire acquire{};

        auto sys{py::module::import("sys")};

        sys.attr("stdout") = pystdout;
        sys.attr("stderr") = pystderr;
    }

    auto interpreter_impl::poll(poll_flags polls) -> bool {
        auto resume{true};

        if((polls & poll_flags::shell_socket) == poll_flags::shell_socket) {
            std::vector<zmq::message_t> msgs{};

            if(zmq::recv_multipart(shell_socket, std::back_inserter(msgs),
                                   zmq::recv_flags::dontwait)) {
                std::vector<std::string> msgs_for_parse{};

                msgs_for_parse.reserve(msgs.size());

                for(const auto &msg : msgs) {
                    std::cerr << "Shell: " << msg << std::endl;
                    msgs_for_parse.push_back(std::move(msg.to_string()));
                }

                dispatch_shell(std::move(msgs_for_parse));
            }
        }

        if((polls & poll_flags::control_socket) == poll_flags::control_socket) {
            std::vector<zmq::message_t> msgs{};

            if(zmq::recv_multipart(control_socket, std::back_inserter(msgs),
                                   zmq::recv_flags::dontwait)) {
                std::vector<std::string> msgs_for_parse{};

                msgs_for_parse.reserve(msgs.size());

                for(const auto &msg : msgs) {
                    std::cerr << "Control: " << msg << std::endl;
                    msgs_for_parse.push_back(std::move(msg.to_string()));
                }

                resume = dispatch_control(std::move(msgs_for_parse));
            }
        }

        if((polls & poll_flags::heartbeat_socket) ==
            poll_flags::heartbeat_socket) {
            std::vector<zmq::message_t> msgs{};

            if(zmq::recv_multipart(heartbeat_socket, std::back_inserter(msgs),
                                   zmq::recv_flags::dontwait)) {
                for(const auto &msg : msgs) {
                    std::cerr << "Heartbeat: " << msg << std::endl;
                }

                zmq::send_multipart(heartbeat_socket, std::move(msgs),
                                    zmq::send_flags::dontwait);
            }
        }

        return resume;
    }

    auto interpreter_impl::topic(std::string topic) const -> std::string {
        return "kernel." + boost::uuids::to_string(identifier) + "." + topic;
    }

    auto interpreter_impl::publish_status(std::string status,
                                          nl::json parent) -> void {
        if(parent.is_null()) {
            parent = parent_header;
        }

        current_session->send(**iopub_socket,
            current_session->construct_message(
                {topic("status")}, {{"msg_type", "status"}}, std::move(parent),
                {}, {{"execution_state", std::move(status)}}
            )
        );
    }

    auto interpreter_impl::set_parent(std::vector<std::string> identifiers,
                                      nl::json parent) -> void {
        parent_identifiers = identifiers;
        parent_header = parent;

        shell::set_parent(shell, py::module::import("json")
            .attr("loads")(parent.dump()));
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

        execute_reply["execution_count"] = result.attr("execution_count")
            .cast<std::size_t>() - 1;

        if(result.attr("success").cast<bool>()) {
            execute_reply["status"] = "ok";
            //execute_reply["user_expressions"] = shell
            //    .attr("user_expressions")(user_expressions);
        } else {
            py::dict error = shell.attr("_user_obj_error")();

            execute_reply["status"] = "error";
            execute_reply["traceback"] = error["traceback"]
                .cast<std::vector<std::string>>();
            execute_reply["ename"] = error["ename"].cast<std::string>();
            execute_reply["evalue"] = error["evalue"].cast<std::string>();
        }

        execute_reply["payload"] = nl::json::array();
        //execute_reply["payload"] = shell.attr("payload_manager")
        //    .attr("read_payload")();
        shell.attr("payload_manager").attr("clear_payload")();

        return std::move(execute_reply);
    }

    auto interpreter_impl::execute_request(zmq::socket_t &socket,
                                           std::vector<std::string> identifiers,
                                           nl::json parent) -> void {
        auto content = std::move(parent["content"]);
        std::string code{std::move(content["code"])};
        bool silent{content["silent"]};
        auto execution_count = shell.attr("execution_count")
            .cast<std::size_t>();

        if(!silent) {
            current_session->send(**iopub_socket,
                current_session->construct_message(
                    {topic("execute_input")}, {{"msg_type", "execute_input"}},
                    parent, {}, {
                        {"code", code}, {"execution_count", execution_count}
                    }
                )
            );
        }

        auto reply_content = do_execute(std::move(code), silent,
                                        content["store_history"],
                                        std::move(content["user_expressions"]),
                                        content["allow_stdin"]);
        auto sys{py::module::import("sys")};

        sys.attr("stdout").attr("flush")();
        sys.attr("stderr").attr("flush")();
        current_session->send(socket, current_session->construct_message(
            std::move(identifiers), {{"msg_type", "execute_reply"}},
            std::move(parent), {}, std::move(reply_content)
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
                                              content["detail_level"])
        ));
    }

    auto interpreter_impl::do_complete(std::string code,
                                       std::size_t cursor_pos) -> nl::json {
        auto completer{py::module::import("IPython.core.completer")};
        auto provisionalcompleter{completer.attr("provisionalcompleter")};
        auto completer_completions{
            shell.attr("Completer").attr("completions")
        };
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
                                               content["cursor_pos"])
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
            )
        ));
    }

    auto interpreter_impl::do_kernel_info() -> nl::json {
        auto sys{py::module::import("sys")};

        return {{"protocol_version", "5.3"},
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
            std::move(parent), {}, do_kernel_info()
        ));
    }

    auto interpreter_impl::do_shutdown(bool restart) -> nl::json {
        return {{"restart", restart}};
    }

    auto interpreter_impl::shutdown_request(
        zmq::socket_t &socket, std::vector<std::string> identifiers,
        nl::json parent
    ) -> void {
        current_session->send(socket, current_session->construct_message(
            std::move(identifiers), {{"msg_type", "shutdown_reply"}},
            std::move(parent), {}, do_shutdown(parent["content"]["restart"])
        ));
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
            std::move(parent), {}, do_interrupt()
        ));
    }

    auto interpreter_impl::dispatch_shell(
        std::vector<std::string> msgs
    ) -> void {
        py::gil_scoped_acquire acquire{};
        publish_status("busy", {});

        std::vector<std::string> identifiers{};
        nl::json header{};
        nl::json parent_header{};
        nl::json metadata{};
        nl::json content{};

        if(!current_session->parse_message(std::move(msgs), identifiers, header,
                                           parent_header, metadata, content)) {
            return;
        }

        std::string msg_type{header["msg_type"]};
        nl::json parent{{"header", std::move(header)},
                        {"parent_header", std::move(parent_header)},
                        {"metadata", std::move(metadata)},
                        {"content", std::move(content)}};

        set_parent(identifiers, parent);

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
        }

        auto sys{py::module::import("sys")};

        sys.attr("stdout").attr("flush")();
        sys.attr("stderr").attr("flush")();
        publish_status("idle", {});
    }

    auto interpreter_impl::dispatch_control(
        std::vector<std::string> msgs
    ) -> bool {
        py::gil_scoped_acquire acquire{};
        publish_status("busy", {});

        std::vector<std::string> identifiers{};
        nl::json header{};
        nl::json parent_header{};
        nl::json metadata{};
        nl::json content{};

        if(!current_session->parse_message(std::move(msgs), identifiers, header,
                                           parent_header, metadata, content)) {
            return true;
        }

        std::string msg_type{header["msg_type"]};
        nl::json parent{{"header", std::move(header)},
                        {"parent_header", std::move(parent_header)},
                        {"metadata", std::move(metadata)},
                        {"content", std::move(content)}};

        set_parent(identifiers, parent);

        auto resume{true};

        if(msg_type == "shutdown_request") {
            shutdown_request(control_socket, std::move(identifiers),
                             std::move(parent));
            resume = false;
        } else if(msg_type == "interrupt_request") {
            interrupt_request(control_socket, std::move(identifiers),
                              std::move(parent));
            resume = false;
        }

        auto sys{py::module::import("sys")};

        sys.attr("stdout").attr("flush")();
        sys.attr("stderr").attr("flush")();
        publish_status("idle", {});

        return resume;
    }

    interpreter::interpreter(std::string signature_key,
                             std::string signature_scheme,
                             zmq::socket_t shell_socket,
                             zmq::socket_t control_socket,
                             zmq::socket_t stdin_socket,
                             zmq::socket_t iopub_socket,
                             zmq::socket_t heartbeat_socket)
        : pimpl{std::make_unique<interpreter_impl>(
              std::move(signature_key), std::move(signature_scheme),
              std::move(shell_socket), std::move(control_socket),
              std::move(stdin_socket), std::move(iopub_socket),
              std::move(heartbeat_socket)
          )} {}

    interpreter::~interpreter() = default;

    auto interpreter::poll(poll_flags polls) -> bool {
        return pimpl->poll(polls);
    }
}
