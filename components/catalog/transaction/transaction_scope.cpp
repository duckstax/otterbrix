#include "transaction_scope.hpp"

namespace components::catalog {
    transaction_scope::transaction_scope(std::pmr::memory_resource* resource,
                                         std::weak_ptr<transaction_list> transactions,
                                         const table_id& id,
                                         namespace_storage* ns_storage)

        : id(id)
        , error_()
        , transaction_list_(transactions)
        , ns_storage(ns_storage) {
        transaction_ = std::unique_ptr<metadata_transaction>(new metadata_transaction(resource));
    }

    transaction_scope::transaction_scope(std::pmr::memory_resource* resource, catalog_error error)
        : id(table_id(nullptr, table_namespace_t{}, ""))
        , error_(std::move(error))
        , transaction_list_()
        , ns_storage(nullptr) {
        transaction_ = std::unique_ptr<metadata_transaction>(new metadata_transaction(resource, error_));
    }

    transaction_scope::~transaction_scope() {
        if (!is_committed && !is_aborted) {
            abort();
        }
    }

    transaction_scope::transaction_scope(transaction_scope&& other) noexcept
        : is_committed(other.is_committed)
        , is_aborted(other.is_aborted)
        , id(std::move(other.id))
        , error_(std::move(other.error_))
        , transaction_list_(std::move(other.transaction_list_))
        , ns_storage(other.ns_storage)
        , transaction_(std::move(other.transaction_)) {
        other.is_aborted = true;
    }

    transaction_scope& transaction_scope::operator=(transaction_scope&& other) noexcept {
        if (this != &other) {
            if (!is_committed && !is_aborted) {
                abort();
            }

            is_committed = other.is_committed;
            is_aborted = other.is_aborted;
            id = std::move(other.id);
            error_ = std::move(other.error_);
            transaction_list_ = std::move(other.transaction_list_);
            ns_storage = other.ns_storage;
            transaction_ = std::move(other.transaction_);

            other.is_aborted = true;
        }
        return *this;
    }

    metadata_transaction& transaction_scope::transaction() { return *transaction_; }

    const catalog_error& transaction_scope::error() const { return error_; }

    void transaction_scope::commit() {
        if (is_committed || is_aborted) {
            error_ = catalog_error(transaction_mistake_t::TRANSACTION_FINALIZED, "Transaction already finalized!");
        }

        if (transaction_list_.expired()) {
            error_ = catalog_error(transaction_mistake_t::COMMIT_FAILED, "Transaction list has been destroyed!");
        }

        if (!error_ && !!ns_storage) {
            auto list = transaction_list_.lock();
            error_ = transaction_->commit([&](metadata_diff&& diff) {
                auto& info = ns_storage->get_namespace_info(id.get_namespace()).tables;

                if (auto it = info.find(id.table_name()); it != info.end()) {
                    auto new_diff = diff.apply(it->second);
                    it->second = new_diff;
                    return catalog_error();
                }

                return catalog_error(transaction_mistake_t::COMMIT_FAILED, "Table not found!");
            });

            list->remove_transaction(id);

            if (!error_) {
                is_committed = true;
                return;
            }
        }

        abort();
    }

    void transaction_scope::abort() {
        if (is_committed) {
            error_ = catalog_error(transaction_mistake_t::TRANSACTION_FINALIZED, "Transaction already committed!");
            return;
        }

        if (is_aborted) {
            return;
        }

        if (!transaction_list_.expired()) {
            transaction_list_.lock()->remove_transaction(id);
        }

        if (transaction_) {
            transaction_->abort();
        }

        is_aborted = true;
    }
} // namespace components::catalog
