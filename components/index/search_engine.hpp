#pragma once

#include "index_engine.hpp"

namespace components::index {

    class stat_entry {

    };

    class stat {
    private:
        std::list<stat_entry> stat_;
    };

    class stat_engine {
        std::unordered_map<int64_t,std::unique_ptr<stat>> storage_;
    };


}