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

    std::unique_ptr<base_comparator> make_comparator(index_disk::compare compare_type) {
        switch (compare_type) {
            case index_disk::compare::str:
                return std::make_unique<comparator<std::string>>();
            case index_disk::compare::int8:
                return std::make_unique<comparator<int8_t>>();
            case index_disk::compare::int16:
                return std::make_unique<comparator<int16_t>>();
            case index_disk::compare::int32:
                return std::make_unique<comparator<int32_t>>();
            case index_disk::compare::int64:
                return std::make_unique<comparator<int64_t>>();
            case index_disk::compare::uint8:
                return std::make_unique<comparator<uint8_t>>();
            case index_disk::compare::uint16:
                return std::make_unique<comparator<uint16_t>>();
            case index_disk::compare::uint32:
                return std::make_unique<comparator<uint32_t>>();
            case index_disk::compare::uint64:
                return std::make_unique<comparator<uint64_t>>();
            case index_disk::compare::float32:
                return std::make_unique<comparator<float>>();
            case index_disk::compare::float64:
                return std::make_unique<comparator<double>>();
            case index_disk::compare::bool8:
                return std::make_unique<comparator<bool>>();
        }
        return std::make_unique<comparator<std::string>>();
    }

    rocksdb::Slice to_slice(const components::document::document_id_t& value) {
        return rocksdb::Slice{reinterpret_cast<const char*>(value.data()), components::document::document_id_t::size};
    }


    index_disk::index_disk(const path_t& path, compare compare_type)
        : db_(nullptr)
        , comparator_(make_comparator(compare_type)) {
        rocksdb::Options options;
        options.OptimizeLevelStyleCompaction();
        options.create_if_missing = true;
        options.comparator = comparator_.get();
        rocksdb::DB* db;
        auto status = rocksdb::DB::Open(options, path.string(), &db);
        if (status.ok()) {
            db_.reset(db);
        } else {
            throw std::runtime_error("db open failed");
        }
    }

    index_disk::~index_disk() = default;

    void index_disk::insert(const wrapper_value_t& key, const document_id_t& value) {
        db_->Put(rocksdb::WriteOptions(), comparator_->slice(key), to_slice(value));
    }

    void index_disk::remove(wrapper_value_t key) {
        db_->Delete(rocksdb::WriteOptions(), comparator_->slice(key));
    }

    index_disk::result index_disk::find(const wrapper_value_t& value) const {
        index_disk::result res;
        rocksdb::PinnableSlice slice;
        auto status = db_->Get(rocksdb::ReadOptions(), db_->DefaultColumnFamily(), comparator_->slice(value), &slice);
        if (!status.IsNotFound()) {
            res.emplace_back(reinterpret_cast<const oid::byte_t*>(slice.data()));
        }
        return res;
    }

    index_disk::result index_disk::lower_bound(const wrapper_value_t& value) const {
        index_disk::result res;
        rocksdb::ReadOptions options;
        auto upper_bound = comparator_->slice(value);
        options.iterate_upper_bound = &upper_bound;
        rocksdb::Iterator* it = db_->NewIterator(options);
        for (it->SeekToFirst(); it->Valid(); it->Next()) {
            res.emplace_back(reinterpret_cast<const oid::byte_t*>(it->value().data()));
        }
        delete it;
        return res;
    }

    index_disk::result index_disk::upper_bound(const wrapper_value_t& value) const {
        index_disk::result res;
        rocksdb::ReadOptions options;
        auto lower_bound = comparator_->slice(value);
        options.iterate_lower_bound = &lower_bound;
        rocksdb::Iterator* it = db_->NewIterator(options);
        it->SeekToFirst();
        if (it->key() == lower_bound) {
            it->Next();
        }
        for (; it->Valid(); it->Next()) {
            res.emplace_back(reinterpret_cast<const oid::byte_t*>(it->value().data()));
        }
        delete it;
        return res;
    }

} // namespace services::disk
