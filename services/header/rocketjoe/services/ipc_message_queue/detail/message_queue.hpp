#pragma once

#include "basic_message_queue.hpp"
#include "message_queue_service.hpp"
#include <posix_message_queue_impl.hpp>

using message_queue =  basic_logger<logger_service<posix_message_queue_impl>>;