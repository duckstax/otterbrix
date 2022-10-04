#include "aggregate.hpp"

#include <magic_enum.hpp>

namespace components::ql {

    std::string to_string(aggregate_steps_statement statement) {
        auto result = magic_enum::enum_name(statement); //memory leak
        return {result.data(), result.size()};
    }

    aggregate_steps_statement from_string(std::string statement) {
        return magic_enum::enum_cast<aggregate_steps_statement>(statement).value();
    }

} // namespace components::ql
