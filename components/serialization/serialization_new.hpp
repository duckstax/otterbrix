#pragma once

#include <components/serialization/stream/base.hpp>
#include <components/serialization/traits.hpp>
#include <core/pmr.hpp>

#include <boost/core/ignore_unused.hpp>
#include <map>
#include <vector>

namespace components::serialization::experimental {

    template<class Storage>
    void serialize(stream::stream<Storage>& ar, std::int32_t data) {
        serialize(ar, data, 0);
    }

} // namespace components::serialization