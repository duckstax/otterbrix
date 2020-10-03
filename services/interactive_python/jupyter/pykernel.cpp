#include <jupyter/pykernel.hpp>

#include <algorithm>
#include <cassert>
#include <iostream>
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
#include <boost/variant/get.hpp>
#include <boost/variant/variant.hpp>
#include <boost/core/ignore_unused.hpp>
#include <boost/algorithm/string/replace.hpp>

#include <nlohmann/json.hpp>

#include <pybind11/embed.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include <zmq_addon.hpp>

#include <components/log/log.hpp>
#include <jupyter/display_hook.hpp>
#include <jupyter/session.hpp>
#include <jupyter/shell.hpp>
#include <jupyter/shell_display_hook.hpp>

// The bug related to the use of RTTI by the pybind11 library has been fixed: a
// declaration should be in each translation unit.
PYBIND11_DECLARE_HOLDER_TYPE(T, boost::intrusive_ptr<T>)

namespace pybind11 { namespace detail {
    template<typename T>
    struct type_caster<boost::optional<T>> : optional_caster<boost::optional<T>> {};

    template<typename... Ts>
    struct type_caster<boost::variant<Ts...>> : variant_caster<boost::variant<Ts...>> {};

    template<>
    struct visit_helper<boost::variant> {
        template<typename... Args>
        static auto call(Args&&... args) -> decltype(boost::apply_visitor(args...));
    };

    template<typename... Args>
    auto visit_helper<boost::variant>::call(Args&&... args) -> decltype(boost::apply_visitor(args...)) {
        return boost::apply_visitor(std::forward(args...));
    }
}} // namespace pybind11::detail

namespace nlohmann {
    template<typename T>
    struct adl_serializer<boost::optional<T>> {
        static auto to_json(json& data,
                            const boost::optional<T>& maybe_data) -> void;

        static auto from_json(const json& data,
                              boost::optional<T>& maybe_data) -> void;
    };

    template<typename... Args>
    struct adl_serializer<boost::variant<Args...>> {
        static auto to_json(json& data,
                            const boost::variant<Args...>& variant_data) -> void;

        static auto from_json(const json& data,
                              boost::variant<Args...>& variant_data) -> void;
    };

    template<typename T>
    auto adl_serializer<boost::optional<T>>::to_json(json& data,
                                                     const boost::optional<T>& maybe_data) -> void {
        if (maybe_data == boost::none) {
            data = nullptr;
        } else {
            data = *maybe_data;
        }
    }

    template<typename T>
    auto adl_serializer<boost::optional<T>>::from_json(const json& data,
                                                       boost::optional<T>& maybe_data) -> void {
        if (data.is_null()) {
            maybe_data = boost::none;
        } else {
            maybe_data = data.get<T>();
        }
    }

    template<typename... Args>
    auto adl_serializer<boost::variant<Args...>>::to_json(json& data,
                                                          const boost::variant<Args...>& variant_data) -> void {
        boost::apply_visitor([&](auto&& value) { data = std::forward<decltype(value)>(value); },
                             variant_data);
    }

    template<typename... Args>
    auto adl_serializer<boost::variant<Args...>>::from_json(const json& data,
                                                            boost::variant<Args...>& variant_data) -> void {
        boost::ignore_unused(data);
        boost::ignore_unused(variant_data);
        throw std::runtime_error("Casting from nlohmann::json to boost::variant isn\'t supported");
    }
} // namespace nlohmann

namespace services { namespace interactive_python_interpreter { namespace jupyter {

        namespace nl = nlohmann;
        namespace py = pybind11;

        using namespace pybind11::literals;

        class BOOST_SYMBOL_VISIBLE input_redirection final {
        public:
            input_redirection(const socket_manager& manager,
                              boost::intrusive_ptr<session> current_session,
                              std::vector<std::string> parent_identifiers,
                              nl::json parent_header,
                              bool allow_stdin);

            ~input_redirection();

        private:
            py::object m_input;
            py::object m_getpass;
        };

        class BOOST_SYMBOL_VISIBLE apply_locals_guard final {
        public:
            apply_locals_guard(py::dict locals,
                               std::string shell_name,
                               std::string function_name,
                               std::string arg_name,
                               std::string kwarg_name,
                               std::string success_name,
                               std::string result_name,
                               py::object shell,
                               py::object function,
                               py::args args,
                               py::kwargs kwargs);

            ~apply_locals_guard();

        private:
            py::dict locals_;
            std::string shell_name_;
            std::string function_name_;
            std::string arg_name_;
            std::string kwarg_name_;
            std::string success_name_;
            std::string result_name_;
        };

        static auto input_request(const socket_manager& manger,
                                  boost::intrusive_ptr<session> current_session,
                                  std::vector<std::string> parent_identifiers,
                                  nl::json parent_header,
                                  const std::string& prompt,
                                  bool password) -> std::string {
            auto log  = components::get_logger();
            log.info( " static auto input_request 0");
            auto sys = py::module::import("sys");
            log.info( " static auto input_request 1");
            sys.attr("stdout").attr("flush")();
            log.info( " static auto input_request 2");
            sys.attr("stderr").attr("flush")();
            log.info( " static auto input_request 3");
            manger->stdin_socket([&](const std::vector<std::string>& ){
              return "";
            });
            log.info( " static auto input_request 4");
            manger->stdin_socket(current_session->construct_message(std::move(parent_identifiers),
                                                                    {{"msg_type", "input_request"}},
                                                                    std::move(parent_header),
                                                                    {},
                                                                    {{"prompt", prompt},
                                                                     {"password", password}},
                                                                    {}));
            log.info( " static auto input_request 5");
        return manger->stdin_socket([&](const std::vector<std::string> msgs_for_parse) {
                std::vector<std::string> identifiers;
                nl::json header;
                nl::json parent_header;
                nl::json metadata;
                nl::json content;
                std::vector<std::string> buffers;

                if (!current_session->parse_message(std::move(msgs_for_parse),
                                                    identifiers,
                                                    header,
                                                    parent_header,
                                                    metadata,
                                                    content,
                                                    buffers)) {
                    throw std::runtime_error("bad answer from stdin.");
                }

                if (header["msg_type"] != "input_reply") {
                    throw std::runtime_error("bad answer from stdin.");
                }

                return std::move(content["value"]);

                throw std::runtime_error("there is no answer from stdin");
            }
        );
            log.info( " static auto input_request 6");
    }

    static auto unimpl(const std::string& promt = "") -> std::string {
        boost::ignore_unused(promt);
        throw std::runtime_error("The kernel doesn\'t support an input requests");
    }

    input_redirection::input_redirection(
        const socket_manager& manger,
        boost::intrusive_ptr<session> current_session,
        std::vector<std::string> parent_identifiers,
        nl::json parent_header,
        bool allow_stdin) {
        auto builtins_m = py::module::import("builtins");

        m_input = builtins_m.attr("input");
        builtins_m.attr("input") =
            allow_stdin
                ? py::cpp_function([manger,
                                    current_session,
                                    parent_identifiers,
                                    parent_header](const std::string& promt = "") {
                      return input_request(manger,
                                           std::move(current_session),
                                           std::move(parent_identifiers),
                                           std::move(parent_header),
                                           promt,
                                           false);
                  },
                                   "prompt"_a = "")
                : py::cpp_function(&unimpl, "prompt"_a = "");

        auto getpass_m = py::module::import("getpass");

        m_getpass = getpass_m.attr("getpass");
        getpass_m.attr("getpass") =
            allow_stdin
                ? py::cpp_function([manger,
                                    current_session,
                                    parent_identifiers,
                                    parent_header](const std::string& promt = "") {
                      return input_request(manger,
                                           std::move(current_session),
                                           std::move(parent_identifiers),
                                           std::move(parent_header),
                                           promt,
                                           true);
                  },
                                   "prompt"_a = "")
                : py::cpp_function(&unimpl, "prompt"_a = "");
    }

    input_redirection::~input_redirection() {
        py::module::import("builtins").attr("input") = m_input;
        py::module::import("getpass").attr("getpass") = m_getpass;
    }

    apply_locals_guard::apply_locals_guard(py::dict locals,
                                           std::string shell_name,
                                           std::string function_name,
                                           std::string arg_name,
                                           std::string kwarg_name,
                                           std::string success_name,
                                           std::string result_name,
                                           py::object shell,
                                           py::object function,
                                           py::args args,
                                           py::kwargs kwargs)
        : locals_(std::move(locals))
        , shell_name_(std::move(shell_name))
        , function_name_(std::move(function_name))
        , arg_name_(std::move(arg_name))
        , kwarg_name_(std::move(kwarg_name))
        , success_name_(std::move(success_name))
        , result_name_(std::move(result_name)) {
        locals_[shell_name_.c_str()] = shell;
        locals_[function_name_.c_str()] = function;
        locals_[arg_name_.c_str()] = args;
        locals_[kwarg_name_.c_str()] = kwargs;
        locals_[success_name_.c_str()] = true;
    }

    apply_locals_guard::~apply_locals_guard() {
        py::exec(R"(
             for key in keys:
                old_locals.pop(key)
        )",
                 py::globals(),
                 py::dict(
                     "old_locals"_a = std::move(locals_),
                     "keys"_a = std::vector<std::string>{std::move(shell_name_),
                                                         std::move(function_name_),
                                                         std::move(arg_name_),
                                                         std::move(kwarg_name_),
                                                         std::move(success_name_),
                                                         std::move(result_name_)}));
    }

    pykernel::pykernel(
        components::log_t& log
        , std::string signature_key
        , std::string signature_scheme
        , bool engine_mode
        , boost::uuids::uuid identifier
        , socket_manager sockets)
        : log_(log.clone())
        , engine_mode(engine_mode)
        , identifier(std::move(identifier))
        , engine_identifier(0)
        , parent_header(nl::json::object())
        , abort_all(false)
        , execution_count(1)
        , socket_manager_(std::move(sockets)) {
        if (engine_mode) {
            current_session = boost::intrusive_ptr<session>(new session(std::move(signature_key),
                                                                        std::move(signature_scheme),
                                                                        this->identifier));
        } else {
            current_session = boost::intrusive_ptr<session>(new session(std::move(signature_key), std::move(signature_scheme)));
        }
        auto pykernel = py::module::import("pyrocketjoe.pykernel");

        shell = pykernel.attr("RocketJoeShell")("_init_location_id"_a = "shell.py:1");
        shell.attr("displayhook")
            .attr("current_session") = current_session;
        shell.attr("displayhook")
            .attr("iopub_socket") = socket_manager_;
        shell.attr("display_pub")
            .attr("current_session") = current_session;
        shell.attr("display_pub")
            .attr("iopub_socket") = socket_manager_;

        auto zmq_ostream = pykernel.attr("ZMQOstream");

        auto new_stdout = zmq_ostream();

        new_stdout.attr("current_session") = current_session;

        new_stdout.attr("iopub_socket") = socket_manager_;

        std::string out_name = "stdout";

        new_stdout.attr("name") = out_name;

        auto new_stderr = zmq_ostream();

        new_stderr.attr("current_session") = current_session;

        new_stderr.attr("iopub_socket") = socket_manager_;

        std::string err_name = "stderr";

        new_stderr.attr("name") = err_name;

        if (engine_mode) {
            shell.attr("displayhook")
                .attr("topic") = "engine." + std::to_string(engine_identifier) + topic(".execute_result");
            shell.attr("display_pub")
                .attr("topic") = "engine." + std::to_string(engine_identifier) + topic(".displaypub");
            new_stdout.attr("topic") = "engine." + std::to_string(engine_identifier) + std::move(out_name);
            new_stderr.attr("topic") = "engine." + std::to_string(engine_identifier) + std::move(err_name);
        } else {
            shell.attr("displayhook")
                .attr("topic") = topic("execute_result");
            shell.attr("display_pub")
                .attr("topic") = topic("display_data");
            new_stdout.attr("topic") = "stream." + std::move(out_name);
            new_stderr.attr("topic") = "stream." + std::move(err_name);
        }

        display_hook displayhook(current_session, socket_manager_);

        auto sys = py::module::import("sys");

        pystdout = sys.attr("stdout");

        pystderr = sys.attr("stderr");

        pystdout.attr("flush")();

        pystderr.attr("flush")();


        sys.attr("stdout") = std::move(new_stdout);

        sys.attr("stderr") = std::move(new_stderr);

        sys.attr("displayhook") = std::move(displayhook);

        if (!engine_mode) {
            publish_status("starting", {});
        }

    }

    pykernel::~pykernel() {
        auto sys = py::module::import("sys");

        sys.attr("stdout") = pystdout;
        sys.attr("stderr") = pystderr;
    }

    auto pykernel::registration() -> std::vector<std::string> {
        return current_session->construct_message(
            {},
            {{"msg_type", "registration_request"}},
            {},
            {},
            {{"uuid", boost::uuids::to_string(identifier)},
             {"id", engine_identifier}},
            {});
    }

    auto pykernel::registration(std::vector<std::string> msgs_for_parse) -> bool {
        std::vector<std::string> identifiers;
        nl::json header;
        nl::json parent_header;
        nl::json metadata;
        nl::json content;
        std::vector<std::string> buffers;

        if (!current_session->parse_message(std::move(msgs_for_parse),
                                            identifiers,
                                            header,
                                            parent_header,
                                            metadata,
                                            content,
                                            buffers)) {
            return true;
        }

        if (header["msg_type"].get<std::string>() != "registration_reply") {
            return false;
        } else if (content["status"].get<std::string>() != "ok") {
            return false;
        } else {
            engine_identifier = content["id"].get<std::uint64_t>();
        }

        publish_status("starting", {});

        return true;
    }

    auto pykernel::topic(std::string topic) const -> std::string {
        if (engine_mode) {
            return "engine." + std::to_string(engine_identifier);
        } else {
            return "kernel." + boost::uuids::to_string(identifier) + "." + topic;
        }
    }

    auto pykernel::publish_status(std::string status, nl::json parent) -> void {
        if (parent.is_null()) {
            parent = parent_header;
        }

        socket_manager_->iopub( current_session->construct_message({topic("status")},
                                                         {{"msg_type", "status"}},
                                                         std::move(parent), {},
                                                         {{"execution_state", std::move(status)}},
                                                         {}));
    }

    auto pykernel::set_parent(std::vector<std::string> identifiers, nl::json parent) -> void {
        parent_identifiers = std::move(identifiers);
        parent_header = parent;

        shell::set_parent(shell, py::module::import("json").attr("loads")(parent.dump()));
    }

    auto pykernel::init_metadata() -> nl::json {
        return {{"started", (boost::locale::format("{1,ftime='%FT%T%Ez'}") %
                             boost::locale::date_time())
                                .str(std::locale())},
                {"dependencies_met", true},
                {"engine", boost::uuids::to_string(identifier)}};
    }

    auto pykernel::finish_metadata(nl::json& metadata, execute_ok_reply reply) {
        boost::ignore_unused(reply);
        metadata["status"] = "ok";
    }

    auto pykernel::finish_metadata(nl::json& metadata, execute_error_reply reply) {
        metadata["status"] = "error";

        if (reply.ename() == "UnmetDependency") {
            metadata["dependencies_met"] = true;
        }

        metadata["engine_info"] = {{"engine_uuid", boost::uuids::to_string(identifier)},
                                   {"engine_id", engine_identifier}};
    }

    auto pykernel::finish_metadata(nl::json& metadata, apply_ok_reply reply) {
        boost::ignore_unused(reply);
        metadata["status"] = "ok";
    }

    auto pykernel::finish_metadata(nl::json& metadata, apply_error_reply reply) {
        metadata["status"] = "error";

        if (reply.ename() == "UnmetDependency") {
            metadata["dependencies_met"] = true;
        }

        metadata["engine_info"] = {{"engine_uuid", boost::uuids::to_string(identifier)},
                                   {"engine_id", engine_identifier}};
    }

    auto pykernel::do_execute(std::string code,
                              bool silent,
                              bool store_history,
                              nl::json user_expressions,
                              bool allow_stdin) -> boost::variant<execute_ok_reply,
                                                                  execute_error_reply> {
        log_.info("auto pykernel::do_execute 0");
        // Scope guard performing the temporary monkey patching of input and
        // getpass with a function sending input_request messages.
        input_redirection input_guard(socket_manager_,
                                      current_session,
                                      parent_identifiers,
                                      parent_header,
                                      allow_stdin);
        log_.info("auto pykernel::do_execute 1");
        auto result = shell.attr("run_cell")(std::move(code),
                                             store_history,
                                             silent);
        log_.info("auto pykernel::do_execute 2");
        if (py::hasattr(result, "execution_count")) {
            execution_count = result.attr("execution_count").cast<std::size_t>() - 1;
        }
        log_.info("auto pykernel::do_execute 3");
        py::module::import("sys")
            .attr("displayhook")
            .attr("set_execution_count")(execution_count);
        log_.info("auto pykernel::do_execute 4");
        auto json = py::module::import("json");
        auto json_loads = json.attr("loads");
        auto json_dumps = json.attr("dumps");
        auto payload = nl::json::parse(json_dumps(shell.attr("payload_manager")
                                                      .attr("read_payload")())
                                           .cast<std::string>());
        log_.info("auto pykernel::do_execute 5");
        shell
            .attr("payload_manager")
            .attr("clear_payload")();
        log_.info("auto pykernel::do_execute 6");
        if (result.attr("success").cast<bool>()) {
            user_expressions = nl::json::parse(json_dumps(shell.attr("user_expressions")(json_loads(user_expressions.dump()))).cast<std::string>());
            execute_ok_reply reply;

            reply.execution_count(execution_count)
                .payload(std::move(payload))
                .user_expressions(std::move(user_expressions));

            return reply;
        } else {
            auto error = shell.attr("_user_obj_error")().cast<py::dict>();
            auto ename = error["ename"].cast<std::string>();
            auto evalue = error["evalue"].cast<std::string>();
            auto traceback = error["traceback"].cast<std::vector<std::string>>();

            execute_error_reply reply;

            reply.execution_count(execution_count)
                .payload(std::move(payload))
                .ename(std::move(ename))
                .evalue(std::move(evalue))
                .traceback(std::move(traceback));

            return reply;
        }
    }

    auto pykernel::execute_request(
        const std::string& socket_type,
        std::vector<std::string> identifiers,
        nl::json parent) -> void {
        auto content = std::move(parent["content"]);
        auto code = content["code"].get<std::string>();
        auto silent = content["silent"].get<boost::optional<bool>>();

        if (!silent) {
            silent = false;
        }

        if (!*silent) {
            auto execution_count = shell.attr("execution_count").cast<std::size_t>();

            socket_manager_->iopub(current_session->construct_message({topic("execute_input")},
                                                                                {{"msg_type", "execute_input"}},
                                                                                parent,
                                                                                {},
                                                                                {{"code", code},
                                                                                 {"execution_count", execution_count}},
                                                                                {}));
        }

        auto store_history = content["store_history"].get<boost::optional<bool>>();

        if (!store_history) {
            store_history = !*silent;
        }

        auto allow_stdin = content["allow_stdin"].get<boost::optional<bool>>();

        if (!allow_stdin) {
            allow_stdin = true;
        }

        auto user_expressions = content["user_expressions"];
        auto metadata = init_metadata();
        auto reply = do_execute(std::move(code),
                                *silent,
                                *store_history,
                                std::move(user_expressions),
                                *allow_stdin);
        auto sys = py::module::import("sys");

        sys.attr("stdout").attr("flush")();
        sys.attr("stderr").attr("flush")();

        if (auto* reply_ok = boost::get<execute_ok_reply>(&reply)) {
            reply_ok->identifiers(std::move(identifiers))
                .parent(std::move(parent))
                .metadata(std::move(metadata));

            finish_metadata(metadata, *reply_ok);

            socket_manager_->socket(socket_type, current_session->construct_message(std::move(*reply_ok)));
            return;
        }

        if (!*silent) {
            auto stop_on_error = content["stop_on_error"].get<boost::optional<bool>>();

            if (!stop_on_error) {
                stop_on_error = true;
            }

            if (*stop_on_error) {
                abort_all = true;
            }
        }

        auto& reply_error = boost::get<execute_error_reply>(reply);

        reply_error.identifiers(std::move(identifiers))
            .parent(std::move(parent))
            .metadata(std::move(metadata));

        finish_metadata(metadata, reply_error);

        socket_manager_->socket(socket_type, current_session->construct_message(std::move(reply_error)));
    }

    auto pykernel::do_inspect(std::string code,
                              std::size_t cursor_pos,
                              std::size_t detail_level) -> nl::json {
        using data_type = std::unordered_map<std::string, std::string>;

        auto found = false;
        data_type data;
        auto name = py::module::import("IPython.utils.tokenutil").attr("token_at_cursor")(std::move(code), cursor_pos);

        try {
            data = shell.attr("object_inspect_mime")(name, detail_level).cast<data_type>();

            if (!shell.attr("enable_html_pager").cast<bool>()) {
                data.erase("text/html");
            }

            found = true;
        } catch (const pybind11::error_already_set& error) {
        }

        return {{"status", "ok"},
                {"found", found},
                {"data", std::move(data)},
                {"metadata", nl::json::object()}};
    }

    auto pykernel::inspect_request(
        const std::string& socket_type,
        std::vector<std::string> identifiers,
        nl::json parent) -> void {
        auto content = std::move(parent["content"]);

        socket_manager_->socket(socket_type, current_session->construct_message(std::move(identifiers),
                                                                                {{"msg_type", "inspect_reply"}},
                                                                                std::move(parent),
                                                                                {},
                                                                                do_inspect(std::move(content["code"]),
                                                                                           content["cursor_pos"].get<std::size_t>(),
                                                                                           content["detail_level"].get<std::size_t>()),
                                                                                {}));
    }

    auto pykernel::do_complete(std::string code,
                               std::size_t cursor_pos) -> nl::json {
        auto completer = py::module::import("IPython.core.completer");
        auto provisionalcompleter = completer.attr("provisionalcompleter");
        auto completer_completions = shell.attr("Completer")
                                         .attr("completions");
        auto rectify_completions = completer.attr("rectify_completions");
        auto locals = py::dict("provisionalcompleter"_a = std::move(provisionalcompleter),
                               "completer_completions"_a = std::move(completer_completions),
                               "rectify_completions"_a = std::move(rectify_completions),
                               "code"_a = std::move(code),
                               "cursor_pos"_a = cursor_pos);

        py::exec(R"(
            with provisionalcompleter():
                completions = completer_completions(code, cursor_pos)
                completions = list(rectify_completions(code, completions))
                completions = [{'start': completion.start,
                                'end': completion.end, 'text': completion.text,
                                'type': completion.type}
                               for completion in completions]
        )",
                 py::globals(),
                 locals);

        auto py_completions = locals["completions"].cast<std::vector<py::dict>>();
        std::vector<nl::json> matches;
        std::vector<nl::json> completions;

        matches.reserve(py_completions.size());
        completions.reserve(py_completions.size());

        auto cursor_start = cursor_pos;
        auto is_first = true;

        for (const auto& py_completion : py_completions) {
            auto start = py_completion["start"].cast<std::size_t>();
            auto end = py_completion["end"].cast<std::size_t>();

            if (is_first) {
                cursor_start = start;
                cursor_pos = end;
                is_first = false;
            }

            auto text = py_completion["text"].cast<std::string>();

            matches.push_back(text);
            completions.push_back({{"start", start},
                                   {"end", end},
                                   {"text", std::move(text)},
                                   {"type", py_completion["type"].cast<std::string>()}});
        }

        return {{"matches", std::move(matches)},
                {"cursor_start", cursor_start},
                {"cursor_end", cursor_pos},
                {"metadata", {{"_jupyter_types_experimental", std::move(completions)}}},
                {"status", "ok"}};
    }

    auto pykernel::complete_request(const std::string& socket_type,
                                    std::vector<std::string> identifiers,
                                    nl::json parent) -> void {
        auto content = std::move(parent["content"]);

        socket_manager_->socket(socket_type, current_session->construct_message(std::move(identifiers),
                                                                                {{"msg_type", "complete_reply"}},
                                                                                std::move(parent),
                                                                                {},
                                                                                do_complete(content["code"].get<std::string>(),
                                                                                            content["cursor_pos"].get<std::size_t>()),
                                                                                {}));
    }

    auto pykernel::history_request(bool output,
                                   bool raw,
                                   const std::string& hist_access_type,
                                   std::int64_t session,
                                   std::size_t start,
                                   boost::optional<std::size_t> stop,
                                   boost::optional<std::size_t> n,
                                   const boost::optional<std::string>& pattern,
                                   bool unique) -> nl::json {
        using history_type = std::vector<std::tuple<std::size_t,
                                                    std::size_t,
                                                    boost::variant<std::string,
                                                                   std::tuple<std::string,
                                                                              boost::optional<std::string>>>>>;

        history_type history;

        if (hist_access_type == "tail") {
            history = py::list(shell.attr("history_manager")
                                   .attr("get_tail")(n,
                                                     raw,
                                                     output,
                                                     "include_latest"_a = true))
                          .cast<history_type>();
        } else if (hist_access_type == "range") {
            history =
                py::list(shell.attr("history_manager")
                             .attr("get_range")(session,
                                                start,
                                                stop,
                                                raw,
                                                output))
                    .cast<history_type>();
        } else if (hist_access_type == "range") {
            history = py::list(shell.attr("history_manager")
                                   .attr("search")(pattern,
                                                   raw,
                                                   output,
                                                   n,
                                                   unique))
                          .cast<history_type>();
        }

        return {{"status", "ok"},
                {"history", std::move(history)}};
    }

    auto pykernel::do_is_complete(std::string code) -> nl::json {
        boost::optional<std::string> indent;
        auto result = shell.attr("input_transformer_manager")
                          .attr("check_complete")(std::move(code))
                          .cast<std::tuple<std::string,
                                           boost::optional<std::size_t>>>();
        auto status = std::get<0>(result);

        if (status == "incomplete") {
            indent = std::string(*std::get<1>(result),
                                 ' ');
        }

        return {{"status", std::move(status)},
                {"indent", std::move(indent)}};
    }

    auto pykernel::is_complete_request(const std::string& socket_type,
                                       std::vector<std::string> identifiers,
                                       nl::json parent) -> void {
        auto code = parent["content"]["code"].get<std::string>();

        socket_manager_->socket(socket_type, current_session->construct_message(std::move(identifiers),
                                                                                {{"msg_type", "is_complete_reply"}},
                                                                                std::move(parent),
                                                                                {},
                                                                                do_is_complete(std::move(code)),
                                                                                {}));
    }

    auto pykernel::do_kernel_info() -> nl::json {
        auto sys = py::module::import("sys");

        return {{"protocol_version", "5.3"},
                {"implementation", "ipython"},
                {"implementation_version", "ipython"},
                {"language_info",
                 {{"name", "python"},
                  {"version", sys.attr("version")
                                  .attr("split")()
                                  .cast<std::vector<std::string>>()[0]},
                  {"mimetype", "text/x-python"},
                  {"codemirror_mode",
                   {{"name", "ipython"},
                    {"version",
                     sys.attr("version_info").attr("major").cast<std::size_t>()}}},
                  {"pygments_lexer", "ipython3"},
                  {"nbconvert_exporter", "python"},
                  {"file_extension", ".py"}}},
                {"banner", shell.attr("banner").cast<std::string>()},
                {"help_links", ""}};
    }

    auto pykernel::kernel_info_request(const std::string& socket_type,
                                       std::vector<std::string> identifiers,
                                       nl::json parent) -> void {
        auto d= std::move(parent);
        log_.info(fmt::format("pykernel::kernel_info_request:       {}",d.dump(4)));
        socket_manager_->socket(socket_type, current_session->construct_message(std::move(identifiers),
                                                                                {{"msg_type", "kernel_info_reply"}},
                                                                                std::move(d),
                                                                                {},
                                                                                do_kernel_info(),
                                                                                {}));
    }

    auto pykernel::do_shutdown(bool restart) -> nl::json {
        return {{"restart", restart}};
    }

    auto pykernel::shutdown_request(const std::string& socket_type,
                                    std::vector<std::string> identifiers,
                                    nl::json parent) -> void {
        auto content = do_shutdown(parent["content"]["restart"]);

        socket_manager_->socket(socket_type, current_session->construct_message(std::move(identifiers),
                                                                                {{"msg_type", "shutdown_reply"}},
                                                                                parent,
                                                                                {},
                                                                                content,
                                                                                {}));
        socket_manager_->iopub( current_session->construct_message({topic("shutdown")},
                                                                            {{"msg_type", "shutdown_reply"}},
                                                                            std::move(parent),
                                                                            {},
                                                                            std::move(content),
                                                                            {}));
    }

    auto pykernel::do_interrupt() -> nl::json {
        return nl::json::object();
    }

    auto pykernel::interrupt_request(const std::string& socket_type, std::vector<std::string> identifiers, nl::json parent) -> void {
        socket_manager_->socket(socket_type, current_session->construct_message(std::move(identifiers),
                                                                                {{"msg_type", "interrupt_reply"}},
                                                                                std::move(parent),
                                                                                {},
                                                                                do_interrupt(),
                                                                                {}));
    }

    auto pykernel::do_apply(std::string msg_id,
                            std::vector<std::string> buffers) -> boost::variant<apply_ok_reply,
                                                                                apply_error_reply> {
        auto serialize = py::module::import("ipyparallel").attr("serialize");
        auto locals = shell.attr("user_ns")
                          .cast<py::dict>();

        std::vector<py::bytes> buffers_raw;

        buffers_raw.reserve(buffers.size());

        for (const auto& buffer : buffers) {
            buffers_raw.push_back(py::bytes(std::move(buffer)));
        }

        auto execute = serialize.attr("unpack_apply_message")(buffers_raw,
                                                              locals,
                                                              "copy"_a = false)
                           .cast<std::tuple<py::object,
                                            py::args,
                                            py::kwargs>>();

        std::replace(msg_id.begin(), msg_id.end(), '-', '_');

        auto prefix = "_" + std::move(msg_id) + "_";
        auto shell_name = prefix + "shell";
        auto function_name = prefix + "f";
        auto arg_name = prefix + "args";
        auto kwarg_name = prefix + "kwargs";
        auto success_name = prefix + "success";
        auto result_name = std::move(prefix) + "result";
        auto code = "try:\n    " + result_name + " = " + function_name + "(*" + arg_name + ", **" + kwarg_name + ")\nexcept BaseException as exception:\n    " + result_name + " = " + shell_name + "._user_obj_error()\n    " + success_name + " = False";
        auto globals = shell.attr("user_global_ns");
        auto failed = false;

        apply_locals_guard guard(locals,
                                 std::move(shell_name),
                                 std::move(function_name),
                                 std::move(arg_name),
                                 std::move(kwarg_name),
                                 success_name,
                                 result_name,
                                 shell,
                                 std::move(std::get<0>(execute)),
                                 std::move(std::get<1>(execute)),
                                 std::move(std::get<1>(execute)));

        py::exec(std::move(code),
                 std::move(globals),
                 locals);

        auto result = locals[result_name.c_str()];

        if (locals[success_name.c_str()].cast<bool>()) {
            apply_ok_reply reply;

            reply.buffers(serialize.attr("serialize_object")(result)
                              .cast<std::vector<std::string>>());

            return reply;
        }

        auto error = result.cast<py::dict>();
        auto ename = error["ename"].cast<std::string>();
        auto evalue = error["evalue"].cast<std::string>();
        auto traceback = error["traceback"].cast<std::vector<std::string>>();
        engine_info_t engine_info;

        engine_info.kernel_identifier(identifier)
            .engine_identifier(engine_identifier);

        apply_error_reply reply;

        reply.ename(std::move(ename))
            .evalue(std::move(evalue))
            .traceback(std::move(traceback))
            .engine_info(std::move(engine_info));

        return reply;
    }

    auto pykernel::apply_request(
        const std::string& socket_type,
        std::vector<std::string> identifiers,
        nl::json parent,
        std::vector<std::string> buffers) -> void {
        auto metadata = init_metadata();
        auto reply = do_apply(parent["header"]["msg_id"].get<std::string>(),
                              std::move(buffers));
        auto sys = py::module::import("sys");

        sys.attr("stdout").attr("flush")();
        sys.attr("stderr").attr("flush")();

        if (auto* reply_ok = boost::get<apply_ok_reply>(&reply)) {
            reply_ok->identifiers(std::move(identifiers))
                .parent(std::move(parent))
                .metadata(std::move(metadata));

            finish_metadata(metadata, *reply_ok);

            socket_manager_->socket(socket_type, current_session->construct_message(std::move(*reply_ok)));

            return;
        }

        auto& reply_error = boost::get<apply_error_reply>(reply);

        apply_error_broadcast broadcast_error;

        broadcast_error.identifiers({topic("error")})
            .parent(parent)
            .ename(reply_error.ename())
            .evalue(reply_error.evalue())
            .traceback(reply_error.traceback())
            .engine_info(reply_error.engine_info());

        socket_manager_->iopub( current_session->construct_message(std::move(broadcast_error)));

        reply_error.identifiers(std::move(identifiers))
            .parent(std::move(parent))
            .metadata(std::move(metadata));

        finish_metadata(metadata, reply_error);
        socket_manager_->socket(socket_type, current_session->construct_message(std::move(reply_error)));
    }

    auto pykernel::do_clear() -> nl::json {
        shell.attr("reset")(false);

        return {{"status", "ok"}};
    }

    auto pykernel::clear_request(
        const std::string& socket_type,
        std::vector<std::string> identifiers,
        nl::json parent) -> void {
        socket_manager_->socket(socket_type, current_session->construct_message(std::move(identifiers),
                                                                                {{"msg_type", "clear_reply"}},
                                                                                std::move(parent),
                                                                                {},
                                                                                do_clear(),
                                                                                {}));
    }

    auto pykernel::do_abort(boost::optional<std::vector<std::string>> identifiers) -> nl::json {
        if (identifiers) {
            aborted.insert(std::make_move_iterator(identifiers->cbegin()),
                           std::make_move_iterator(identifiers->cend()));
        } else {
            abort_all = true;
        }

        return {{"status", "ok"}};
    }

    auto pykernel::abort_request(
        const std::string& socket_type,
        std::vector<std::string> identifiers,
        nl::json parent) -> void {
        auto msg_ids = parent["content"]["msg_ids"].get<boost::optional<std::vector<std::string>>>();

        socket_manager_->socket(socket_type, current_session->construct_message(std::move(identifiers),
                                                                                {{"msg_type", "abort_reply"}},
                                                                                std::move(parent),
                                                                                {},
                                                                                do_abort(std::move(msg_ids)),
                                                                                {}));
    }

    auto pykernel::abort_reply(
        const std::string& socket_type,
        std::vector<std::string> identifiers,
        nl::json parent) -> void {
        auto msg_type = parent["header"]["msg_type"].get<std::string>();
        log_.info(fmt::format("auto pykernel::abort_reply msg type :  {}",std::string(std::make_move_iterator(std::make_reverse_iterator(msg_type.crend())),
                                                                                      std::make_move_iterator(std::make_reverse_iterator(std::find(msg_type.crbegin(),
                                                                                                                                                   msg_type.crend(),
                                                                                                                                                   '_'))))));
        socket_manager_->socket(socket_type, current_session->construct_message(std::move(identifiers),
                                                                                {{"msg_type", std::string(std::make_move_iterator(std::make_reverse_iterator(msg_type.crend())),
                                                                                                          std::make_move_iterator(std::make_reverse_iterator(std::find(msg_type.crbegin(),
                                                                                                                                                                       msg_type.crend(),
                                                                                                                                                                       '_'))))}},
                                                                                std::move(parent),
                                                                                {{"status", "aborted"},
                                                                                 {"engine", boost::uuids::to_string(identifier)}},
                                                                                {{"status", "aborted"}},
                                                                                {}));
    }

    auto pykernel::dispatch_shell(std::vector<std::string> msgs) -> void {
        std::vector<std::string> identifiers;
        nl::json header;
        nl::json parent_header;
        nl::json metadata;
        nl::json content;
        std::vector<std::string> buffers;
        std::string socket_type = "shell";
        if (!current_session->parse_message(std::move(msgs),
                                            identifiers,
                                            header,
                                            parent_header,
                                            metadata,
                                            content,
                                            buffers)) {
            throw std::runtime_error("pre_dispatch_control parse_message");
        }

        auto msg_type = header["msg_type"].get<std::string>();
        auto msg_id = header["msg_id"].get<std::string>();
        nl::json parent{{"header", std::move(header)},
                        {"parent_header", std::move(parent_header)},
                        {"metadata", std::move(metadata)},
                        {"content", std::move(content)}};

        set_parent(identifiers, parent);
        publish_status("busy", {});

        if (abort_all) {
            abort_reply(socket_type,
                        std::move(identifiers),
                        std::move(parent));
            publish_status("idle", {});
            log_.info("pykernel::dispatch_shell  if (abort_all) { ");
            return;
        }

        auto abort = std::find(aborted.cbegin(),
                               aborted.cend(),
                               std::move(msg_id));

        if (abort != aborted.cend()) {
            aborted.erase(abort);
            abort_reply(socket_type,
                        std::move(identifiers),
                        std::move(parent));
            publish_status("idle", {});
            log_.info("pykernel::dispatch_shell   if (abort != aborted.cend()) { ");
            return;
        }

        auto resume = true;

        if (msg_type == "execute_request") {
            execute_request(socket_type,
                            std::move(identifiers),
                            std::move(parent));
        } else if (msg_type == "inspect_request") {
            inspect_request(socket_type,
                            std::move(identifiers),
                            std::move(parent));
        } else if (msg_type == "complete_request") {
            complete_request(socket_type,
                             std::move(identifiers),
                             std::move(parent));
        } else if (msg_type == "is_complete_request") {
            is_complete_request(socket_type,
                                std::move(identifiers),
                                std::move(parent));
        } else if (msg_type == "kernel_info_request") {
            kernel_info_request(socket_type,
                                std::move(identifiers),
                                std::move(parent));
        } else if (msg_type == "shutdown_request") {
            shutdown_request(socket_type,
                             std::move(identifiers),
                             std::move(parent));
            resume = false;
        } else if (msg_type == "interrupt_request") {
            interrupt_request(socket_type,
                              std::move(identifiers),
                              std::move(parent));
            resume = false;
        } else if (msg_type == "apply_request") {
            apply_request(socket_type,
                          std::move(identifiers),
                          std::move(parent),
                          std::move(buffers));
        } else {
            log_.info(fmt::format("Msg Type: {}",msg_type));
        }

        auto sys = py::module::import("sys");

        sys.attr("stdout").attr("flush")();
        sys.attr("stderr").attr("flush")();

        log_.info(fmt::format("Resume: {}",resume));
        if (resume) {
            log_.info(" auto pykernel::dispatch_shell idle");
            publish_status("idle", {});
        }
    }

    auto pykernel::dispatch_control(std::vector<std::string> msgs) -> void {
        std::vector<std::string> identifiers;
        nl::json header;
        nl::json parent_header;
        nl::json metadata;
        nl::json content;
        std::vector<std::string> buffers;
        std::string socket_type = "control";
        if (!current_session->parse_message(std::move(msgs),
                                            identifiers,
                                            header,
                                            parent_header,
                                            metadata,
                                            content,
                                            buffers)) {
            throw std::runtime_error("pre_dispatch_control parse_message");
        }

        auto msg_type = header["msg_type"].get<std::string>();
        nl::json parent{{"header", std::move(header)},
                        {"parent_header", std::move(parent_header)},
                        {"metadata", std::move(metadata)},
                        {"content", std::move(content)}};

        set_parent(identifiers, parent);
        publish_status("busy", {});

        if (abort_all) {
            abort_reply(socket_type,
                        std::move(identifiers),
                        std::move(parent));
            publish_status("idle", {});
            return;
        }

        auto resume = true;

        if (msg_type == "execute_request") {
            execute_request(socket_type,
                            std::move(identifiers),
                            std::move(parent));
        } else if (msg_type == "inspect_request") {
            inspect_request(socket_type,
                            std::move(identifiers),
                            std::move(parent));
        } else if (msg_type == "complete_request") {
            complete_request(socket_type,
                             std::move(identifiers),
                             std::move(parent));
        } else if (msg_type == "is_complete_request") {
            is_complete_request(socket_type,
                                std::move(identifiers),
                                std::move(parent));
        } else if (msg_type == "kernel_info_request") {
            kernel_info_request(socket_type,
                                std::move(identifiers),
                                std::move(parent));
        } else if (msg_type == "shutdown_request") {
            shutdown_request(socket_type,
                             std::move(identifiers),
                             std::move(parent));
            resume = false;
        } else if (msg_type == "interrupt_request") {
            interrupt_request(socket_type,
                              std::move(identifiers),
                              std::move(parent));
            resume = false;
        } else if (msg_type == "apply_request") {
            apply_request(socket_type,
                          std::move(identifiers),
                          std::move(parent),
                          std::move(buffers));
        } else if (msg_type == "clear_request") {
            clear_request(socket_type,
                          std::move(identifiers),
                          std::move(parent));
        } else if (msg_type == "abort_request") {
            abort_request(socket_type,
                          std::move(identifiers),
                          std::move(parent));
        } else {
            log_.error(fmt::format("Msg Type: {}",msg_type));
        }

        auto sys = py::module::import("sys");

        sys.attr("stdout").attr("flush")();
        sys.attr("stderr").attr("flush")();

        log_.info(fmt::format("Resume: {}",resume));
        if (resume) {
            log_.info("auto pykernel::dispatch_control( idle");
            publish_status("idle", {});
        }
    }

    auto pykernel::stop_session() -> void {
        abort_all = false;
    }

}}
} // namespace components::detail::jupyter
