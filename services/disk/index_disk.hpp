#pragma once

#include <components/document/document_id.hpp>
#include <components/document/document.hpp>
#include <components/document/wrapper_value.hpp>
#include <filesystem>

namespace rocksdb {
    class DB;
} // namespace rocksdb

namespace services::disk {

    class base_comparator;

    class index_disk_t {
        using document_id_t = components::document::document_id_t;
        using wrapper_value_t = document::wrapper_value_t;
        using path_t = std::filesystem::path;

    public:
        enum class compare {
            str,
            int8,
            int16,
            int32,
            int64,
            uint8,
            uint16,
            uint32,
            uint64,
            float32,
            float64,
            bool8
        };
        using result = std::vector<document_id_t>;

        index_disk_t(const path_t& path, compare compare_type);
        ~index_disk_t();

        void insert(const wrapper_value_t& key, const document_id_t& value);
        void remove(wrapper_value_t key);
        void remove(const wrapper_value_t& key, const document_id_t& doc);
        void find(const wrapper_value_t& value, result &res) const;
        result find(const wrapper_value_t& value) const;
        void lower_bound(const wrapper_value_t& value, result &res) const;
        result lower_bound(const wrapper_value_t& value) const;
        void upper_bound(const wrapper_value_t& value, result &res) const;
        result upper_bound(const wrapper_value_t& value) const;

        void drop();

    private:
        std::unique_ptr<rocksdb::DB> db_;
        std::unique_ptr<base_comparator> comparator_;
    };

} // namespace services::disk
