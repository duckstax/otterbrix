#include <rocketjoe/python_sandbox/detail/jupyter/session.hpp>

#include <algorithm>
#include <iostream>
#include <iterator>
#include <memory>
#include <utility>

#include <unistd.h>
#include <pwd.h>

#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <boost/locale/date_time.hpp>
#include <boost/locale/format.hpp>

#include <botan/hex.h>
#include <botan/mac.h>

#include <zmq.hpp>
#include <zmq_addon.hpp>

namespace rocketjoe { namespace services { namespace detail { namespace jupyter {

    session::session(std::string signature_key, std::string signature_scheme)
        : session_id{boost::uuids::random_generator()()}
        , message_count{0}
        , signature_key{std::move(signature_key)}
        , signature_scheme{std::move(signature_scheme)} {}

    session::session(std::string signature_key, std::string signature_scheme,
                     boost::uuids::uuid session_id)
        : session_id{std::move(session_id)}
        , message_count{0}
        , signature_key{std::move(signature_key)}
        , signature_scheme{std::move(signature_scheme)} {}

    auto session::parse_message(std::vector<std::string> msgs,
                                std::vector<std::string> &identifiers,
                                nl::json &header, nl::json &parent_header,
                                nl::json &metadata,
                                nl::json &content,
                                std::vector<std::string>& buffers) const -> bool {
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

        std::move(std::make_move_iterator(msgs_tail.cbegin() + 4),
                  std::make_move_iterator(msgs.cend()),
                  std::back_inserter(buffers));

        auto signature{compute_signature(header_raw, parent_header_raw,
                                         metadata_raw, content_raw, buffers)};

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
        nl::json metadata, nl::json content, std::vector<std::string> buffers
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
                                         metadata_raw, content_raw, buffers)};

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
        std::move(std::make_move_iterator(buffers.cbegin()),
                  std::make_move_iterator(buffers.cend()),
                  std::back_inserter(msgs));

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
                                    std::string content,
                                    std::vector<std::string>& buffers) const -> std::string {
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

        for(const auto& buffer : buffers) {
            mac->update(buffer);
        }

        return Botan::hex_encode(mac->final(), false);
    }

}}}}
