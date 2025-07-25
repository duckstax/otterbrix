#pragma once

#include "../namespace_storage.hpp"
#include "../table_metadata.hpp"
#include "metadata_diff.hpp"
#include <map>

namespace components::catalog {
    class transaction_scope;

    class metadata_transaction {
    public:
        enum class State
        {
            ACTIVE,
            COMMITTED,
            ABORTED
        };

        metadata_transaction& add_column(const std::string& name,
                                         const components::types::complex_logical_type& type,
                                         bool required = false,
                                         const std::pmr::string& doc = "");
        metadata_transaction& delete_column(const std::string& name);
        metadata_transaction& rename_column(const std::string& name, const std::string& new_name);
        metadata_transaction& update_column_type(const std::string& name,
                                                 const components::types::complex_logical_type& new_type);
        metadata_transaction& update_column_doc(const std::string& name, const std::pmr::string& doc);
        metadata_transaction& make_optional(const std::string& name);
        metadata_transaction& make_required(const std::string& name);
        metadata_transaction& update_description(const std::pmr::string& desc);

        metadata_transaction& savepoint(const std::pmr::string& name);
        metadata_transaction& rollback_to_savepoint(const std::pmr::string& name);

        [[nodiscard]] State state() const;

    private:
        metadata_transaction(std::pmr::memory_resource* resource);

        void ensure_active();

        template<typename F, typename = std::enable_if_t<std::is_invocable_v<F, metadata_diff>>>
        void commit(F&& fun) {
            ensure_active();

            metadata_diff_.use_schema_diff(std::move(schema_diff_));
            if (metadata_diff_.has_changes()) {
                fun(std::move(metadata_diff_));
            }

            state_ = State::COMMITTED;
        }

        void abort();

        State state_ = State::ACTIVE;
        metadata_diff metadata_diff_;
        schema_diff schema_diff_;
        std::pmr::map<std::pmr::string, std::pair<metadata_diff, schema_diff>> savepoints_;

        friend class transaction_scope;
    };
} // namespace components::catalog
