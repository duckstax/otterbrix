#include "metadata_transaction.hpp"

namespace components::catalog {
    metadata_transaction::metadata_transaction(std::pmr::memory_resource* resource)
        : metadata_diff_(resource)
        , schema_diff_(resource)
        , savepoints_(resource) {}

    void metadata_transaction::ensure_active() {
        if (state_ != State::ACTIVE) {
            throw commit_failed_exception("Transaction is not active");
        }
    }

    metadata_transaction& metadata_transaction::add_column(const std::string& name,
                                                           const components::types::complex_logical_type& type,
                                                           bool required,
                                                           const std::pmr::string& doc) {
        ensure_active();
        schema_diff_.add_column(name, type, required, doc);
        return *this;
    }

    metadata_transaction& metadata_transaction::delete_column(const std::string& name) {
        ensure_active();
        schema_diff_.delete_column(name);
        return *this;
    }

    metadata_transaction& metadata_transaction::rename_column(const std::string& name, const std::string& new_name) {
        ensure_active();
        schema_diff_.rename_column(name, new_name);
        return *this;
    }

    metadata_transaction&
    metadata_transaction::update_column_type(const std::string& name,
                                             const components::types::complex_logical_type& new_type) {
        ensure_active();
        schema_diff_.update_column_type(name, new_type);
        return *this;
    }

    metadata_transaction& metadata_transaction::update_column_doc(const std::string& name,
                                                                  const std::pmr::string& doc) {
        ensure_active();
        schema_diff_.update_column_doc(name, doc);
        return *this;
    }

    metadata_transaction& metadata_transaction::make_optional(const std::string& name) {
        ensure_active();
        schema_diff_.make_optional(name);
        return *this;
    }

    metadata_transaction& metadata_transaction::make_required(const std::string& name) {
        ensure_active();
        schema_diff_.make_required(name);
        return *this;
    }

    metadata_transaction& metadata_transaction::update_description(const std::pmr::string& desc) {
        ensure_active();
        metadata_diff_.update_description(desc);
        return *this;
    }

    metadata_transaction& metadata_transaction::savepoint(const std::pmr::string& name) {
        ensure_active();
        savepoints_.emplace(name, std::make_pair(metadata_diff_, schema_diff_));
        return *this;
    }

    metadata_transaction& metadata_transaction::rollback_to_savepoint(const std::pmr::string& name) {
        ensure_active();
        auto it = savepoints_.find(name);
        if (it == savepoints_.end()) {
            throw std::runtime_error("Savepoint not found");
        }

        metadata_diff_ = it->second.first;
        schema_diff_ = it->second.second;
        return *this;
    }

    metadata_transaction::State metadata_transaction::state() const { return state_; }

    void metadata_transaction::abort() {
        if (state_ == State::ACTIVE) {
            state_ = State::ABORTED;
        }
    }
} // namespace components::catalog
