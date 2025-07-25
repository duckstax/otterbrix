#include "schema_diff.hpp"

using namespace components::types;

namespace components::catalog {
    schema_diff::schema_diff(std::pmr::memory_resource* resource)
        : updates(resource)
        , renames(resource)
        , deleted_columns(resource)
        , new_primary_key()
        , resource(resource) {}

    schema_diff::diff_info::diff_info(schema_diff::diff_info_type info_type,
                                      types::field_description desc,
                                      types::complex_logical_type type)
        : info(1 << info_type)
        , entry{type, desc} {}

    void schema_diff::add_column(const std::string& name,
                                 const types::complex_logical_type& type,
                                 bool required,
                                 const std::pmr::string& doc) {
        types::complex_logical_type named = type;
        named.set_alias(name);
        added_columns.emplace_back(struct_entry{std::move(named), field_description(0, required, doc.c_str())});
    }

    void schema_diff::delete_column(const std::string& name) { deleted_columns.emplace(name); }

    void schema_diff::rename_column(const std::string& name, const std::string& new_name) {
        if (auto it = updates.find(name); it != updates.end()) {
            it->second.info.set(diff_info_type::UPDATE_NAME);
            it->second.entry.type.set_alias(new_name);
            return;
        }

        auto type = complex_logical_type();
        type.set_alias(new_name);
        renames.emplace(name, new_name);
        updates.emplace(new_name, diff_info(diff_info_type::UPDATE_NAME, field_description(), std::move(type)));
    }

    void schema_diff::update_column_type(const std::string& name, const types::complex_logical_type& new_type) {
        if (auto it = updates.find(name); it != updates.end()) {
            const auto& old_alias = it->second.entry.type.alias();
            it->second.info.set(diff_info_type::UPDATE_TYPE);
            it->second.entry.type = new_type;
            it->second.entry.type.set_alias(old_alias);
            return;
        }

        updates.emplace(name, diff_info(diff_info_type::UPDATE_TYPE, field_description(), new_type));
    }

    void schema_diff::update_column_doc(const std::string& name, const std::pmr::string& doc) {
        if (auto it = updates.find(name); it != updates.end()) {
            it->second.info.set(diff_info_type::UPDATE_DOC);
            it->second.entry.desc.doc = doc;
            return;
        }

        updates.emplace(name, diff_info(diff_info_type::UPDATE_DOC, field_description(0, false, doc.c_str())));
    }

    void schema_diff::make_optional(const std::string& name) {
        if (auto it = updates.find(name); it != updates.end()) {
            it->second.info.set(diff_info_type::UPDATE_OPTIONAL);
            it->second.entry.desc.required = false;
            return;
        }

        updates.emplace(name, diff_info(diff_info_type::UPDATE_OPTIONAL, field_description(0, false)));
    }

    void schema_diff::make_required(const std::string& name) {
        if (auto it = updates.find(name); it != updates.end()) {
            it->second.info.set(diff_info_type::UPDATE_OPTIONAL);
            it->second.entry.desc.required = true;
            return;
        }

        updates.emplace(name, diff_info(diff_info_type::UPDATE_OPTIONAL, field_description(0, true)));
    }

    void schema_diff::update_primary_key(const std::pmr::vector<field_id_t>& primary_key) {
        new_primary_key.emplace(primary_key);
    }

    bool schema_diff::has_changes() const {
        return !updates.empty() || !added_columns.empty() || !deleted_columns.empty() || new_primary_key;
    }

    schema schema_diff::apply(const schema& base_schema) const {
        size_t sz = base_schema.columns().size();

        std::vector<types::complex_logical_type> new_columns;
        std::vector<types::field_description> new_desc;
        new_columns.reserve(sz + added_columns.size());
        new_desc.reserve(sz + added_columns.size());

        for (size_t i = 0; i < sz; ++i) {
            const auto& column = base_schema.columns()[i];
            const auto& desc = base_schema.descriptions()[i];
            if (deleted_columns.find(column.alias()) != deleted_columns.end()) {
                continue;
            }

            new_columns.push_back(column);
            new_desc.push_back(desc);

            if (!do_update(new_columns, new_desc, column.alias())) {
                if (const auto& rename = renames.find(column.alias()); rename != renames.end()) {
                    do_update(new_columns, new_desc, rename->second);
                }
            }
        }

        for (const auto& entry : added_columns) {
            new_columns.push_back(entry.type);
            new_desc.push_back(entry.desc);
        }

        return schema(resource,
                      create_struct(new_columns, new_desc),
                      new_primary_key.value_or(base_schema.primary_key()));
    }

    bool schema_diff::do_update(std::vector<types::complex_logical_type>& new_columns,
                                std::vector<types::field_description>& new_desc,
                                const std::string& name) const {
        if (const auto& it = updates.find(name); it != updates.end()) {
            auto& info = it->second;
            for (auto diff_type : magic_enum::enum_values<diff_info_type>()) {
                if (!info.info.test(diff_type)) {
                    continue;
                }

                switch (diff_type) {
                    case diff_info_type::UPDATE_TYPE:
                        new_columns.back() = info.entry.type;
                        break;
                    case diff_info_type::UPDATE_NAME: {
                        // if type is being updated - do nothing, correct alias is already set
                        if (!info.info.test(UPDATE_TYPE)) {
                            new_columns.back().set_alias(info.entry.type.alias());
                        }
                        break;
                    }
                    case diff_info_type::UPDATE_DOC:
                        new_desc.back().doc = std::move(info.entry.desc.doc);
                        break;
                    case diff_info_type::UPDATE_OPTIONAL:
                        new_desc.back().required = info.entry.desc.required;
                        break;
                }
            }

            return true;
        }

        return false;
    }
} // namespace components::catalog
