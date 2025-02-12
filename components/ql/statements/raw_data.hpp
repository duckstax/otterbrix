#pragma once

#include <components/document/msgpack/msgpack_encoder.hpp>
#include <components/ql/ql_statement.hpp>
#include <memory_resource>

namespace components::ql {

    struct raw_data_t : ql_statement_t {
        explicit raw_data_t(const std::pmr::vector<document_ptr>& documents);
        explicit raw_data_t(std::pmr::vector<document_ptr>&& documents);
        explicit raw_data_t(std::pmr::memory_resource* resource)
            : documents_(resource) {}
        raw_data_t(const raw_data_t&) = default;
        raw_data_t& operator=(const raw_data_t&) = default;
        raw_data_t(raw_data_t&&) = default;
        raw_data_t& operator=(raw_data_t&&) = default;
        ~raw_data_t() = default;
        std::pmr::vector<document_ptr> documents_;
    };

} // namespace components::ql