#pragma once

#include <pybind11/embed.h>
#include <pybind11/functional.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include <nlohmann/json.hpp>

#include <boost/smart_ptr/intrusive_ref_counter.hpp>
#include <boost/uuid/uuid.hpp>

#include <zmq.hpp>

namespace rocketjoe { namespace services { namespace detail { namespace jupyter {

namespace py = pybind11;
namespace nl = nlohmann;

class session final : public boost::intrusive_ref_counter<session> {
public:
  session(std::string signature_key, std::string signature_scheme);

  session(const session &) = delete;

  session &operator=(const session &) = delete;

  auto parse_message(std::vector<std::string> msgs,
                     std::vector<std::string> &identifiers, nl::json &header,
                     nl::json &parent_header, nl::json &metadata,
                     nl::json &content) const -> bool;

  auto construct_message(std::vector<std::string> identifiers, nl::json header,
                         nl::json parent, nl::json metadata, nl::json content)
      -> std::vector<std::string>;

  auto send(zmq::socket_t &socket, std::vector<std::string> msgs) const -> void;

private:
  auto compute_signature(std::string header, std::string parent_header,
                         std::string metadata, std::string content) const
      -> std::string;

  constexpr static auto version{"5.3"};
  constexpr static auto delimiter{"<IDS|MSG>"};
  boost::uuids::uuid session_id;
  std::size_t message_count;
  std::string signature_key;
  std::string signature_scheme;
};

}}}}