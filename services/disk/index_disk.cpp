#include "index_disk.hpp"
#include <rocksdb/db.h>

namespace services::disk {

    class base_comparator : public rocksdb::Comparator {
    public:
        virtual rocksdb::Slice slice(const document::wrapper_value_t& value) const = 0;
    };

    template<class T>
    class comparator final : public base_comparator {
    public:
        comparator() = default;

    private:
        int Compare(const rocksdb::Slice& a, const rocksdb::Slice& b) const final {
            return a.compare(b);
        }

        rocksdb::Slice slice(const T& value) const {
            return rocksdb::Slice{reinterpret_cast<const char*>(&value), sizeof(T)};
        }

        rocksdb::Slice slice(const document::wrapper_value_t& value) const final {
            return rocksdb::Slice{value->as_string()};
        }

        const char* Name() const final {
            return "comparator";
        }

        void FindShortestSeparator(std::string*, const rocksdb::Slice&) const final {}

        void FindShortSuccessor(std::string*) const final {}
    };

    template<>
    rocksdb::Slice comparator<std::string>::slice(const document::wrapper_value_t& value) const {
        return rocksdb::Slice{value->as_string()};
    }

    template<>
    int comparator<std::string>::Compare(const rocksdb::Slice& a, const rocksdb::Slice& b) const {
        if (a.empty() && b.empty()) return 0;
        if (!a.data()) return -1;
        if (!b.data()) return 1;
        return a.compare(b);
    }

    template<>
    int comparator<double>::Compare(const rocksdb::Slice& a, const rocksdb::Slice& b) const {
        auto a_ = *reinterpret_cast<const double*>(a.data());
        auto b_ = *reinterpret_cast<const double*>(b.data());
        if (a_ < b_)
            return -1;
        if (a_ > b_)
            return 1;
        return 0;
    }

    template<>
    int comparator<float>::Compare(const rocksdb::Slice& a, const rocksdb::Slice& b) const {
        auto a_ = *reinterpret_cast<const float*>(a.data());
        auto b_ = *reinterpret_cast<const float*>(b.data());
        if (a_ < b_)
            return -1;
        if (a_ > b_)
            return 1;
        return 0;
    }

#define ADD_TYPE_SLICE(TYPE, FUNC)                                                         \
    template<>                                                                             \
    rocksdb::Slice comparator<TYPE>::slice(const document::wrapper_value_t& value) const { \
        static TYPE v;                                                                     \
        v = TYPE(value->FUNC());                                                           \
        return slice(v);                                                                   \
    }

    ADD_TYPE_SLICE(int8_t, as_int)
    ADD_TYPE_SLICE(int16_t, as_int)
    ADD_TYPE_SLICE(int32_t, as_int)
    ADD_TYPE_SLICE(int64_t, as_int)
    ADD_TYPE_SLICE(uint8_t, as_unsigned)
    ADD_TYPE_SLICE(uint16_t, as_unsigned)
    ADD_TYPE_SLICE(uint32_t, as_unsigned)
    ADD_TYPE_SLICE(uint64_t, as_unsigned)
    ADD_TYPE_SLICE(float, as_float)
    ADD_TYPE_SLICE(double, as_double)
    ADD_TYPE_SLICE(bool, as_bool)

    std::unique_ptr<base_comparator> make_comparator(components::ql::index_compare compare_type) {
        switch (compare_type) {
            case components::ql::index_compare::str:
                return std::make_unique<comparator<std::string>>();
            case components::ql::index_compare::int8:
                return std::make_unique<comparator<int8_t>>();
            case components::ql::index_compare::int16:
                return std::make_unique<comparator<int16_t>>();
            case components::ql::index_compare::int32:
                return std::make_unique<comparator<int32_t>>();
            case components::ql::index_compare::int64:
                return std::make_unique<comparator<int64_t>>();
            case components::ql::index_compare::uint8:
                return std::make_unique<comparator<uint8_t>>();
            case components::ql::index_compare::uint16:
                return std::make_unique<comparator<uint16_t>>();
            case components::ql::index_compare::uint32:
                return std::make_unique<comparator<uint32_t>>();
            case components::ql::index_compare::uint64:
                return std::make_unique<comparator<uint64_t>>();
            case components::ql::index_compare::float32:
                return std::make_unique<comparator<float>>();
            case components::ql::index_compare::float64:
                return std::make_unique<comparator<double>>();
            case components::ql::index_compare::bool8:
                return std::make_unique<comparator<bool>>();
        }
        return std::make_unique<comparator<std::string>>();
    }

    rocksdb::Slice to_slice(const index_disk_t::result& values) {
        return rocksdb::Slice{reinterpret_cast<const char*>(values.data()), values.size() * components::document::document_id_t::size};
    }

    void from_slice(const rocksdb::Slice &slice, index_disk_t::result &docs) {
        auto size = docs.size();
        docs.resize(size + slice.size() / components::document::document_id_t::size);
        std::memcpy(docs.data() + size, slice.data(), slice.size());
    }

    index_disk_t::index_disk_t(const path_t& path, components::ql::index_compare compare_type)
        : path_(path)
        , db_(nullptr)
        , comparator_(make_comparator(compare_type)) {
        rocksdb::Options options;
        options.OptimizeLevelStyleCompaction();
        options.create_if_missing = true;
        options.comparator = comparator_.get();
        rocksdb::DB* db;
        if (!std::filesystem::is_directory(path)) {
            std::filesystem::create_directories(path);
        }
        auto status = rocksdb::DB::Open(options, path.string(), &db);
        if (status.ok()) {
            db_.reset(db);
        } else {
            throw std::runtime_error("db open failed");
        }
    }

    index_disk_t::~index_disk_t() = default;

    void index_disk_t::insert(const wrapper_value_t& key, const document_id_t& value) {
        auto values = find(key);
        if (std::find(values.begin(), values.end(), value) == values.end()) {
            values.push_back(value);
            db_->Put(rocksdb::WriteOptions(), comparator_->slice(key), to_slice(values));
        }
    }

    void index_disk_t::remove(wrapper_value_t key) {
        db_->Delete(rocksdb::WriteOptions(), comparator_->slice(key));
    }

    void index_disk_t::remove(const wrapper_value_t& key, const document_id_t& doc) {
        auto values = find(key);
        if (!values.empty()) {
            values.erase(std::remove(values.begin(), values.end(), doc), values.end());
            if (values.empty()) {
                remove(key);
            } else {
                db_->Put(rocksdb::WriteOptions(), comparator_->slice(key), to_slice(values));
            }
        }
    }

    void index_disk_t::find(const wrapper_value_t& value, result &res) const {
        rocksdb::PinnableSlice slice;
        auto status = db_->Get(rocksdb::ReadOptions(), db_->DefaultColumnFamily(), comparator_->slice(value), &slice);
        if (!status.IsNotFound()) {
            from_slice(slice, res);
        }
    }

    index_disk_t::result index_disk_t::find(const wrapper_value_t& value) const {
        index_disk_t::result res;
        find(value, res);
        return res;
    }

    void index_disk_t::lower_bound(const wrapper_value_t& value, result &res) const {
        rocksdb::ReadOptions options;
        auto upper_bound = comparator_->slice(value);
        options.iterate_upper_bound = &upper_bound;
        rocksdb::Iterator* it = db_->NewIterator(options);
        for (it->SeekToFirst(); it->Valid(); it->Next()) {
            from_slice(it->value(), res);
        }
        delete it;
    }

    index_disk_t::result index_disk_t::lower_bound(const wrapper_value_t& value) const {
        index_disk_t::result res;
        lower_bound(value, res);
        return res;
    }

    void index_disk_t::upper_bound(const wrapper_value_t& value, result &res) const {
        rocksdb::ReadOptions options;
        auto lower_bound = comparator_->slice(value);
        options.iterate_lower_bound = &lower_bound;
        rocksdb::Iterator* it = db_->NewIterator(options);
        it->SeekToFirst();
        if (it->key() == lower_bound) {
            it->Next();
        }
        for (; it->Valid(); it->Next()) {
            from_slice(it->value(), res);
        }
        delete it;
    }

    index_disk_t::result index_disk_t::upper_bound(const wrapper_value_t& value) const {
        index_disk_t::result res;
        upper_bound(value, res);
        return res;
    }

    void index_disk_t::drop() {
        db_.release();
        std::filesystem::remove_all(path_);
    }

} // namespace services::disk
