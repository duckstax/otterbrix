#include "catalog/table_id.hpp"

#include <unordered_set>

namespace components::catalog {
    struct transaction_list {
        transaction_list(std::pmr::memory_resource* resource);

        void add_transaction(const table_id& id);
        void remove_transaction(const table_id& id);

        bool has_active_transactions(const table_id& id);

    private:
        std::pmr::unordered_set<table_id> active_transactions;
    };
} // namespace components::catalog
