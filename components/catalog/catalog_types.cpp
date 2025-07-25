#include "catalog_types.hpp"

namespace components::catalog {
    types::complex_logical_type
    create_list(field_id_t field_id, const types::complex_logical_type& type, bool required) {
        return types::complex_logical_type(
            types::logical_type::LIST,
            std::make_unique<types::list_logical_type_extention>(field_id, type, required));
    }

    types::complex_logical_type create_struct(const std::vector<types::complex_logical_type>& columns,
                                              const std::vector<types::field_description>& descriptions) {
        return types::complex_logical_type(
            types::logical_type::STRUCT,
            std::make_unique<types::struct_logical_type_extention>(columns, descriptions));
    }

    types::complex_logical_type create_map(field_id_t key_id,
                                           const types::complex_logical_type& key,
                                           field_id_t value_id,
                                           const types::complex_logical_type& value,
                                           bool value_required) {
        return types::complex_logical_type(
            types::logical_type::MAP,
            std::make_unique<types::map_logical_type_extention>(key_id, key, value_id, value, value_required));
    }

    types::map_logical_type_extention& to_map(const types::complex_logical_type& type) {
        assert(type.type() == types::logical_type::MAP);
        assert(type.extention()->type() == types::logical_type_extention::extention_type::MAP);
        return static_cast<types::map_logical_type_extention&>(*type.extention());
    }

    types::struct_logical_type_extention& to_struct(const types::complex_logical_type& type) {
        assert(type.type() == types::logical_type::STRUCT);
        assert(type.extention()->type() == types::logical_type_extention::extention_type::STRUCT);
        return static_cast<types::struct_logical_type_extention&>(*type.extention());
    }

    types::list_logical_type_extention& to_list(const types::complex_logical_type& type) {
        assert(type.type() == types::logical_type::LIST);
        assert(type.extention()->type() == types::logical_type_extention::extention_type::LIST);
        return static_cast<types::list_logical_type_extention&>(*type.extention());
    }
} // namespace components::catalog
