#include "transaction_list.hpp"

namespace components::catalog {
    transaction_list::transaction_list(std::pmr::memory_resource* resource)
        : active_transactions(resource) {}

    void transaction_list::add_transaction(const table_id& id) { active_transactions.insert(id); }

    void transaction_list::remove_transaction(const table_id& id) { active_transactions.erase(id); }

    bool transaction_list::has_active_transactions(const table_id& id) {
        return active_transactions.find(id) != active_transactions.end();
    }
} // namespace components::catalog
