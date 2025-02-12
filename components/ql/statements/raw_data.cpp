#include "raw_data.hpp"

namespace components::ql {

    raw_data_t::raw_data_t(const std::pmr::vector<document_ptr>& documents)
        : ql_statement_t(statement_type::raw_data, "", "")
        , documents_(documents) {}

    raw_data_t::raw_data_t(std::pmr::vector<document_ptr>&& documents)
        : ql_statement_t(statement_type::raw_data, "", "")
        , documents_(std::move(documents)) {}

} // namespace components::ql