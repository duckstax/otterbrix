#pragma once

#include <memory>

#include "ql_statement.hpp"

namespace components::ql {

    class result_translator {

    };

    using result_translator_ptr = std::unique_ptr<result_translator>;

    auto ql_translator(const ql_statement_t&) -> result_translator_ptr;

}