#include <rocketjoe/api/transport_base.hpp>

namespace RocketJoe { namespace transport {

    transport_base::transport_base(transport_type type,transport_id id ) : type_(type),id_(id) {}

}}