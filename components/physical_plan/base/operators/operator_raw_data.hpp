#pragma once

#include "operator.hpp"
#include <components/document/document.hpp>

namespace services::base::operators {

    class operator_raw_data_t final : public read_only_operator_t {
    public:
        explicit operator_raw_data_t(std::pmr::vector<document_ptr>&& documents);
        explicit operator_raw_data_t(const std::pmr::vector<document_ptr>& documents);
        explicit operator_raw_data_t(components::vector::data_chunk_t&& chunk);
        explicit operator_raw_data_t(const components::vector::data_chunk_t& chunk);

    private:
        void on_execute_impl(components::pipeline::context_t*) final;
    };

} // namespace services::base::operators
