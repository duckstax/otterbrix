#include "transaction_list.hpp"

namespace components::catalog {
    transaction_list::transaction_list(std::pmr::memory_resource* resource)
        : active_transactions_(resource) {}

    void transaction_list::add_transaction(const table_id& id) { active_transactions_.insert(id); }

    void transaction_list::remove_transaction(const table_id& id) { active_transactions_.erase(id); }

    bool transaction_list::has_active_transactions(const table_id& id) {
        return active_transactions_.find(id) != active_transactions_.end();
    }
} // namespace components::catalog
