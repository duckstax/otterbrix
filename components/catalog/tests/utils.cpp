#include "utils.hpp"

using namespace components::types;
using namespace components::catalog;

namespace test {
    void create_single_column_table(const collection_full_name_t& name,
                                    complex_logical_type log_t,
                                    catalog& cat,
                                    std::pmr::memory_resource* resource) {
        log_t.set_alias(name.collection);
        schema sch(resource, create_struct({log_t}, {field_description(1, true, "test")}), {1});
        cat.create_table({resource, name}, {resource, sch});
    }
} // namespace test
