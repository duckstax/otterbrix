#pragma once

#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <components/session/session.hpp>

namespace otterbrix {
    using components::session::session_id_t;

    class wrapper_collection;
    using wrapper_collection_ptr = boost::intrusive_ptr<wrapper_collection>;
    class wrapper_database;
    using wrapper_database_ptr = boost::intrusive_ptr<wrapper_database>;

} // namespace otterbrix