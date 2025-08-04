#include "transaction_scope.hpp"

namespace components::catalog {
    transaction_scope::transaction_scope(std::weak_ptr<transaction_list> transactions,
                                         const components::catalog::table_id& id,
                                         components::catalog::namespace_storage& ns_storage,
                                         std::pmr::memory_resource* resource)

        : id(id)
        , transaction_list_(transactions)
        , ns_storage(ns_storage) {
        transaction_ = std::unique_ptr<metadata_transaction>(new metadata_transaction(resource));
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
            return;
        }

        if (transaction_list_.expired()) {
            error_ = catalog_error(transaction_mistake_t::COMMIT_FAILED, "Transaction list has been destroyed!");
            return;
        }

        auto list = transaction_list_.lock();
        error_ = transaction_->commit([&](metadata_diff&& diff) {
            auto& info = ns_storage.get().get_namespace_info(id.get_namespace()).tables;

            if (auto it = info.find(id.table_name()); it != info.end()) {
                auto new_diff = diff.apply(it->second);
                it->second = new_diff;
                return catalog_error();
            }

            return catalog_error(transaction_mistake_t::COMMIT_FAILED, "Table not found!");
        });

        (!!error_) ? (is_aborted = true) : (is_committed = true);
        list->remove_transaction(id);
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
