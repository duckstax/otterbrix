#pragma once

#include <oid/oid.hpp>
#include <memory>

namespace components::document {

    struct document_id_size {
        static constexpr uint size_timestamp = 4;
        static constexpr uint size_random = 5;
        static constexpr uint size_increment = 3;
    };

    using document_id_t = oid::oid_t<document_id_size>;

}