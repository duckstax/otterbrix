#include "amqp_consumer.hpp"

#include <amqp.h>
#include <amqp_tcp_socket.h>

void amqp_consumer::start_loop() {
  amqp_socket_t* socket = NULL;
  amqp_connection_state_t conn;
  int error = 0;

  socket = amqp_tcp_socket_new(conn);
  if (!socket) {
    return; // TODO exception
  }

  error = amqp_socket_open(socket, host.c_str(), port);
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
