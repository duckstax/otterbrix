#pragma once

#include <string>
#include <vector>

#include <nlohmann/json.hpp>

#include <boost/smart_ptr/intrusive_ref_counter.hpp>
#include <boost/uuid/uuid.hpp>

#include <zmq.hpp>

namespace rocketjoe {
namespace services {
namespace detail {
namespace jupyter {

    namespace nl = nlohmann;

    class execute_ok_reply final {
        std::vector<std::string> identifiers_;
        nl::json parent_;
        nl::json metadata_;
        std::size_t execution_count_;
        nl::json payload_;
        nl::json user_expressions_;

    public:
        auto identifiers() const -> const std::vector<std::string>&;

        auto identifiers(std::vector<std::string> identifiers) -> execute_ok_reply&;

        auto parent() const -> const nl::json&;

        auto parent(nl::json parent) -> execute_ok_reply&;

        auto metadata() const -> const nl::json&;

        auto metadata(nl::json metadata) -> execute_ok_reply&;

        auto execution_count() const -> std::size_t;

        auto execution_count(std::size_t execution_count) -> execute_ok_reply&;

        auto payload() const -> const nl::json&;

        auto payload(nl::json payload) -> execute_ok_reply&;

        auto user_expressions() const -> const nl::json&;

        auto user_expressions(nl::json user_expressions) -> execute_ok_reply&;
    };

    class execute_error_reply final {
        std::vector<std::string> identifiers_;
        nl::json parent_;
        nl::json metadata_;
        std::size_t execution_count_;
        nl::json payload_;
        std::string ename_;
        std::string evalue_;
        std::vector<std::string> traceback_;

    public:
        auto identifiers() const -> const std::vector<std::string>&;

        auto identifiers(std::vector<std::string> identifiers)
            -> execute_error_reply&;

        auto parent() const -> const nl::json&;

        auto parent(nl::json parent) -> execute_error_reply&;

        auto metadata() const -> const nl::json&;

        auto metadata(nl::json metadata) -> execute_error_reply&;

        auto execution_count() const -> std::size_t;

        auto execution_count(std::size_t execution_count) -> execute_error_reply&;

        auto payload() const -> const nl::json&;

        auto payload(nl::json payload) -> execute_error_reply&;

        auto ename() const -> const std::string&;

        auto ename(std::string ename) -> execute_error_reply&;

        auto evalue() const -> const std::string&;

        auto evalue(std::string evalue) -> execute_error_reply&;

        auto traceback() const -> const std::vector<std::string>&;

        auto traceback(std::vector<std::string> traceback) -> execute_error_reply&;
    };

    class session final : public boost::intrusive_ref_counter<session> {
    public:
        session(std::string signature_key, std::string signature_scheme);

        session(std::string signature_key, std::string signature_scheme,
                boost::uuids::uuid session_id);

        session(const session&) = delete;

        session& operator=(const session&) = delete;

        auto parse_message(std::vector<std::string> msgs,
                           std::vector<std::string>& identifiers, nl::json& header,
                           nl::json& parent_header, nl::json& metadata,
                           nl::json& content, std::vector<std::string>& buffers) const
            -> bool;

        auto construct_message(std::vector<std::string> identifiers, nl::json header,
                               nl::json parent, nl::json metadata, nl::json content,
                               std::vector<std::string> buffers)
            -> std::vector<std::string>;

        auto construct_execute_ok_reply(execute_ok_reply reply)
            -> std::vector<std::string>;

        auto construct_execute_error_reply(execute_error_reply reply)
            -> std::vector<std::string>;

        auto send(zmq::socket_t& socket, std::vector<std::string> msgs) const -> void;

    private:
        auto compute_signature(std::string header, std::string parent_header,
                               std::string metadata, std::string content,
                               std::vector<std::string>& buffers) const
            -> std::string;

        constexpr static auto version{"5.3"};
        constexpr static auto delimiter{"<IDS|MSG>"};
        boost::uuids::uuid session_id;
        std::size_t message_count;
        std::string signature_key;
        std::string signature_scheme;
    }; // namespace jupyter

}
}
}
} // namespace rocketjoe::services::detail::jupyter
