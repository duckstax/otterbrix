#pragma once

#include "basic_message_queue.hpp"
#include "message_queue_service.hpp"
#include "../posix_message_queue_impl.hpp"
namespace services {
    using message_queue =  basic_message_queue<message_queue_service<ipc::posix_message_queue_impl>>;
}