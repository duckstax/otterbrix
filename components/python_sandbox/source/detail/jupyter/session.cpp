#include <detail/jupyter/session.hpp>

#include <algorithm>
#include <iostream>
#include <iterator>
#include <memory>
#include <utility>

#include <pwd.h>
#include <unistd.h>

#include <boost/locale/date_time.hpp>
#include <boost/locale/format.hpp>
#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/uuid_io.hpp>

#include <zmq.hpp>
#include <zmq_addon.hpp>

namespace components { namespace detail { namespace jupyter {

    auto execute_ok_reply::identifiers() const -> const std::vector<std::string>& {
        return identifiers_;
    }

    auto execute_ok_reply::identifiers(std::vector<std::string> identifiers) -> execute_ok_reply& {
        identifiers_ = identifiers;

        return *this;
    }

    auto execute_ok_reply::parent() const -> const nl::json& {
        return parent_;
    }

    auto execute_ok_reply::parent(nl::json parent) -> execute_ok_reply& {
        parent_ = parent;

        return *this;
    }

    auto execute_ok_reply::metadata() const -> const nl::json& {
        return metadata_;
    }

    auto execute_ok_reply::metadata(nl::json metadata) -> execute_ok_reply& {
        metadata_ = metadata;

        return *this;
    }

    auto execute_ok_reply::execution_count() const -> std::size_t {
        return execution_count_;
    }

    auto execute_ok_reply::execution_count(std::size_t execution_count) -> execute_ok_reply& {
        execution_count_ = execution_count;

        return *this;
    }

    auto execute_ok_reply::payload() const -> const nl::json& {
        return payload_;
    }

    auto execute_ok_reply::payload(nl::json payload) -> execute_ok_reply& {
        payload_ = payload;

        return *this;
    }

    auto execute_ok_reply::user_expressions() const -> const nl::json& {
        return user_expressions_;
    }

    auto execute_ok_reply::user_expressions(nl::json user_expressions) -> execute_ok_reply& {
        user_expressions_ = user_expressions;

        return *this;
    }

    auto execute_error_reply::identifiers() const -> const std::vector<std::string>& {
        return identifiers_;
    }

    auto execute_error_reply::identifiers(std::vector<std::string> identifiers) -> execute_error_reply& {
        identifiers_ = identifiers;

        return *this;
    }

    auto execute_error_reply::parent() const -> const nl::json& {
        return parent_;
    }

    auto execute_error_reply::parent(nl::json parent) -> execute_error_reply& {
        parent_ = parent;

        return *this;
    }

    auto execute_error_reply::metadata() const -> const nl::json& {
        return metadata_;
    }

    auto execute_error_reply::metadata(nl::json metadata) -> execute_error_reply& {
        metadata_ = metadata;

        return *this;
    }

    auto execute_error_reply::execution_count() const -> std::size_t {
        return execution_count_;
    }

    auto execute_error_reply::execution_count(std::size_t execution_count) -> execute_error_reply& {
        execution_count_ = execution_count;

        return *this;
    }

    auto execute_error_reply::payload() const -> const nl::json& {
        return payload_;
    }

    auto execute_error_reply::payload(nl::json payload) -> execute_error_reply& {
        payload_ = payload;

        return *this;
    }

    auto execute_error_reply::ename() const -> const std::string& {
        return ename_;
    }

    auto execute_error_reply::ename(std::string ename) -> execute_error_reply& {
        ename_ = ename;

        return *this;
    }

    auto execute_error_reply::evalue() const -> const std::string& {
        return evalue_;
    }

    auto execute_error_reply::evalue(std::string evalue) -> execute_error_reply& {
        evalue_ = evalue;

        return *this;
    }

    auto execute_error_reply::traceback() const -> const std::vector<std::string>& {
        return traceback_;
    }

    auto execute_error_reply::traceback(std::vector<std::string> traceback) -> execute_error_reply& {
        traceback_ = traceback;

        return *this;
    }

    auto apply_ok_reply::identifiers() const -> const std::vector<std::string>& {
        return identifiers_;
    }

    auto apply_ok_reply::identifiers(std::vector<std::string> identifiers) -> apply_ok_reply& {
        identifiers_ = identifiers;

        return *this;
    }

    auto apply_ok_reply::parent() const -> const nl::json& {
        return parent_;
    }

    auto apply_ok_reply::parent(nl::json parent) -> apply_ok_reply& {
        parent_ = parent;

        return *this;
    }

    auto apply_ok_reply::metadata() const -> const nl::json& {
        return metadata_;
    }

    auto apply_ok_reply::metadata(nl::json metadata) -> apply_ok_reply& {
        metadata_ = metadata;

        return *this;
    }

    auto apply_ok_reply::buffers() const -> const std::vector<std::string>& {
        return buffers_;
    }

    auto apply_ok_reply::buffers(std::vector<std::string> buffers) -> apply_ok_reply& {
        buffers_ = buffers;

        return *this;
    }

    auto engine_info_t::kernel_identifier() const -> const boost::uuids::uuid& {
        return kernel_identifier_;
    }

    auto engine_info_t::kernel_identifier(boost::uuids::uuid kernel_identifier) -> engine_info_t& {
        kernel_identifier_ = kernel_identifier;

        return *this;
    }

    auto engine_info_t::engine_identifier() const -> std::uint64_t {
        return engine_identifier_;
    }

    auto engine_info_t::engine_identifier(std::uint64_t engine_identifier) -> engine_info_t& {
        engine_identifier_ = engine_identifier;

        return *this;
    }

    auto apply_error_reply::identifiers() const -> const std::vector<std::string>& {
        return identifiers_;
    }

    auto apply_error_reply::identifiers(std::vector<std::string> identifiers) -> apply_error_reply& {
        identifiers_ = identifiers;

        return *this;
    }

    auto apply_error_reply::parent() const -> const nl::json& {
        return parent_;
    }

    auto apply_error_reply::parent(nl::json parent) -> apply_error_reply& {
        parent_ = parent;

        return *this;
    }

    auto apply_error_reply::metadata() const -> const nl::json& {
        return metadata_;
    }

    auto apply_error_reply::metadata(nl::json metadata) -> apply_error_reply& {
        metadata_ = metadata;

        return *this;
    }

    auto apply_error_reply::ename() const -> const std::string& {
        return ename_;
    }

    auto apply_error_reply::ename(std::string ename) -> apply_error_reply& {
        ename_ = ename;

        return *this;
    }

    auto apply_error_reply::evalue() const -> const std::string& {
        return evalue_;
    }

    auto apply_error_reply::evalue(std::string evalue) -> apply_error_reply& {
        evalue_ = evalue;

        return *this;
    }

    auto apply_error_reply::traceback() const -> const std::vector<std::string>& {
        return traceback_;
    }

    auto apply_error_reply::traceback(std::vector<std::string> traceback) -> apply_error_reply& {
        traceback_ = traceback;

        return *this;
    }

    auto apply_error_reply::engine_info() const -> const engine_info_t& {
        return engine_info_;
    }

    auto apply_error_reply::engine_info(engine_info_t engine_info) -> apply_error_reply& {
        engine_info_ = engine_info;

        return *this;
    }

    auto apply_error_broadcast::identifiers() const -> const std::vector<std::string>& {
        return identifiers_;
    }

    auto apply_error_broadcast::identifiers(std::vector<std::string> identifiers) -> apply_error_broadcast& {
        identifiers_ = identifiers;

        return *this;
    }

    auto apply_error_broadcast::parent() const -> const nl::json& {
        return parent_;
    }

    auto apply_error_broadcast::parent(nl::json parent) -> apply_error_broadcast& {
        parent_ = parent;

        return *this;
    }

    auto apply_error_broadcast::ename() const -> const std::string& {
        return ename_;
    }

    auto apply_error_broadcast::ename(std::string ename) -> apply_error_broadcast& {
        ename_ = ename;

        return *this;
    }

    auto apply_error_broadcast::evalue() const -> const std::string& {
        return evalue_;
    }

    auto apply_error_broadcast::evalue(std::string evalue) -> apply_error_broadcast& {
        evalue_ = evalue;

        return *this;
    }

    auto apply_error_broadcast::traceback() const -> const std::vector<std::string>& {
        return traceback_;
    }

    auto apply_error_broadcast::traceback(std::vector<std::string> traceback) -> apply_error_broadcast& {
        traceback_ = traceback;

        return *this;
    }

    auto apply_error_broadcast::engine_info() const -> const engine_info_t& {
        return engine_info_;
    }

    auto apply_error_broadcast::engine_info(engine_info_t engine_info) -> apply_error_broadcast& {
        engine_info_ = engine_info;

        return *this;
    }

    session::session(std::string signature_key, std::string signature_scheme)
        : session_id(boost::uuids::random_generator()())
        , message_count(0)
        , hmac_(signature_scheme, signature_key) {}

    session::session(std::string signature_key,
                     std::string signature_scheme,
                     boost::uuids::uuid session_id)
        : session_id(std::move(session_id))
        , message_count(0)
        , hmac_(signature_scheme, signature_key) {}

    auto session::parse_message(std::vector<std::string> msgs,
                                std::vector<std::string>& identifiers,
                                nl::json& header,
                                nl::json& parent_header,
                                nl::json& metadata,
                                nl::json& content,
                                std::vector<std::string>& buffers)  -> bool {
        // See for a description of the protocol:
        // https://jupyter-client.readthedocs.io/en/stable/messaging.html#general-message-format
        auto split_position = std::find(msgs.cbegin(),
                                        msgs.cend(),
                                        std::string{delimiter});

        identifiers = std::vector<std::string>(std::make_move_iterator(msgs.cbegin()),
                                               std::make_move_iterator(split_position));

        std::vector<std::string> msgs_tail(std::make_move_iterator(split_position + 1),
                                           std::make_move_iterator(msgs.cend()));
        auto header_raw = std::move(msgs_tail[1]);
        auto parent_header_raw = std::move(msgs_tail[2]);
        auto metadata_raw = std::move(msgs_tail[3]);
        auto content_raw = std::move(msgs_tail[4]);

        std::move(std::make_move_iterator(msgs_tail.cbegin() + 5),
                  std::make_move_iterator(msgs_tail.cend()),
                  std::back_inserter(buffers));

        auto signature = compute_signature(header_raw,
                                           parent_header_raw,
                                           metadata_raw,
                                           content_raw,
                                           buffers);

        if (signature != msgs_tail[0]) {
            return false;
        }

        header = nl::json::parse(std::move(header_raw));
        parent_header = nl::json::parse(std::move(parent_header_raw));
        metadata = nl::json::parse(std::move(metadata_raw));
        content = nl::json::parse(std::move(content_raw));

        return true;
    }

    auto session::construct_message(std::vector<std::string> identifiers,
                                    nl::json header,
                                    nl::json parent,
                                    nl::json metadata,
                                    nl::json content,
                                    std::vector<std::string> buffers) -> std::vector<std::string> {
        nl::json parent_header = nl::json::object();

        if (parent.contains("header")) {
            parent_header = std::move(parent["header"]);
        }

        if (!header.contains("version")) {
            header["version"] = std::string{version};
        }

        if (!header.contains("username")) {
            std::string username = "root";
            auto user_info = getpwuid(geteuid());

            if (user_info) {
                username = user_info->pw_name;
            }

            header["username"] = std::move(username);
        }

        if (!header.contains("session")) {
            header["session"] = boost::uuids::to_string(session_id);
        }

        if (!header.contains("msg_id")) {
            header["msg_id"] = boost::uuids::to_string(session_id) + "_" +
                               std::to_string(message_count++);
        }

        if (!header.contains("date")) {
            header["date"] = (boost::locale::format("{1,ftime='%FT%T%Ez'}") %
                              boost::locale::date_time())
                                 .str(std::locale());
        }

        if (metadata.is_null()) {
            metadata = nl::json::object();
        }

        if (content.is_null()) {
            content = nl::json::object();
        }

        auto header_raw = header.dump();
        auto parent_header_raw = parent_header.dump();
        auto metadata_raw = metadata.dump();
        auto content_raw = content.dump();
        auto signature = compute_signature(header_raw,
                                           parent_header_raw,
                                           metadata_raw,
                                           content_raw,
                                           buffers);

        std::vector<std::string> msgs;

        msgs.reserve(identifiers.size() + 6);
        msgs.insert(msgs.end(),
                    std::make_move_iterator(identifiers.begin()),
                    std::make_move_iterator(identifiers.end()));
        msgs.push_back(delimiter);
        msgs.push_back(std::move(signature));
        msgs.push_back(std::move(header_raw));
        msgs.push_back(std::move(parent_header_raw));
        msgs.push_back(std::move(metadata_raw));
        msgs.push_back(std::move(content_raw));
        std::move(std::make_move_iterator(buffers.cbegin()),
                  std::make_move_iterator(buffers.cend()),
                  std::back_inserter(msgs));

        return std::move(msgs);
    }

    auto session::construct_message(execute_ok_reply reply) -> std::vector<std::string> {
        return construct_message(reply.identifiers(),
                                 {{"msg_type", "execute_reply"}},
                                 reply.parent(),
                                 reply.metadata(),
                                 {{"execution_count", reply.execution_count()},
                                  {"payload", reply.payload()},
                                  {"status", "ok"},
                                  {"user_expressions", reply.user_expressions()}},
                                 {});
    }

    auto session::construct_message(execute_error_reply reply) -> std::vector<std::string> {
        return construct_message(reply.identifiers(),
                                 {{"msg_type", "execute_reply"}},
                                 reply.parent(),
                                 reply.metadata(),
                                 {{"execution_count", reply.execution_count()},
                                  {"payload", reply.payload()},
                                  {"status", "error"},
                                  {"ename", reply.ename()},
                                  {"evalue", reply.evalue()},
                                  {"traceback", reply.traceback()}},
                                 {});
    }

    auto session::construct_message(apply_ok_reply reply) -> std::vector<std::string> {
        return construct_message(reply.identifiers(),
                                 {{"msg_type", "apply_reply"}},
                                 reply.parent(),
                                 reply.metadata(),
                                 {{"status", "ok"}},
                                 reply.buffers());
    }

    auto session::construct_message(apply_error_reply reply) -> std::vector<std::string> {
        return construct_message(reply.identifiers(),
                                 {{"msg_type", "apply_reply"}},
                                 reply.parent(),
                                 reply.metadata(),
                                 {{"status", "error"},
                                  {"ename", reply.ename()},
                                  {"evalue", reply.evalue()},
                                  {"traceback", reply.traceback()},
                                  // clang-format off
                                  {"engine_info", {{"engine_uuid", boost::uuids::to_string(reply.engine_info().kernel_identifier())},
                                                   {"engine_id", reply.engine_info().engine_identifier()},
                                                   {"method", "apply"}}}},
                                 // clang-format on
                                 {});
    }

    auto session::construct_message(apply_error_broadcast broadcast) -> std::vector<std::string> {
        return construct_message(broadcast.identifiers(),
                                 {{"msg_type", "error"}},
                                 broadcast.parent(),
                                 nl::json::object(),
                                 {{"status", "error"},
                                  {"ename", broadcast.ename()},
                                  {"evalue", broadcast.evalue()},
                                  {"traceback", broadcast.traceback()},
                                  // clang-format off
                                  {"engine_info", {{"engine_uuid", boost::uuids::to_string(broadcast.engine_info().kernel_identifier())},
                                                   {"engine_id", broadcast.engine_info().engine_identifier()},
                                                   {"method", "apply"}}}},
                                 // clang-format on
                                 {});
    }

    auto session::send(zmq::socket_t& socket, std::vector<std::string> msgs) const -> void {
        std::vector<zmq::const_buffer> msgs_for_send;

        msgs_for_send.reserve(msgs.size());

        for (const auto& msg : msgs) {
            std::cerr << msg << std::endl;
            msgs_for_send.push_back(zmq::buffer(std::move(msg)));
        }

        assert(zmq::send_multipart(socket, std::move(msgs_for_send)));
    }

    auto session::compute_signature(
        std::string header,
        std::string parent_header,
        std::string metadata,
        std::string content,
        std::vector<std::string>& buffers) -> std::string {
        return hmac_.sign(header, parent_header, metadata, content);
    }
}
}} // namespace components::detail::jupyter
