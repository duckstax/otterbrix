#include "amqp_consumer.hpp"

#include <amqp.h>
#include <amqp_tcp_socket.h>

amqp_consumer::amqp_consumer(const std::string& url) : _url(url) {
  if (_url.scheme() != "amqp") {
    throw new std::runtime_error("URL scheme must be: amqp, provided: " + _url.scheme())
  }
}

char* amqp_consumer::get_host() const {
  return _url.host().c_str();
}

int amqp_consumer::get_port() const {
  return std::stoi(_url.port());
}

void amqp_consumer::start_loop() {
  amqp_socket_t* socket = NULL;
  amqp_connection_state_t conn;
  int error = 0;

  socket = amqp_tcp_socket_new(conn);
  if (!socket) {
    return; // TODO exception
  }

  error = amqp_socket_open(socket, get_host(), get_port());
  if (error) {
    return ; // TODO exception
  }

  // TODO binding to queue

  while (true) {
    amqp_rpc_reply_t ret;
    amqp_envelope_t envelope;

    amqp_maybe_release_buffers(conn);
    ret = amqp_consume_message(conn, &envelope, NULL, 0);

    // TODO process message - call task (how pass task context here?)
  }
}
