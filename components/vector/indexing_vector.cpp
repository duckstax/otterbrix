#include "indexing_vector.hpp"

namespace components::vector {

    indexing_data::indexing_data(std::pmr::memory_resource* resource, size_t count)
        : data(new (resource->allocate(count * sizeof(uint32_t), alignof(uint32_t))) uint32_t[count],
               core::pmr::array_deleter_t(resource, count, alignof(uint32_t))) {}

    indexing_vector_t::indexing_vector_t(uint32_t* indexing) noexcept
        : data_(nullptr)
        , indexing_(indexing) {}

    indexing_vector_t::indexing_vector_t(std::pmr::memory_resource* resource, uint64_t count)
        : data_(std::make_shared<indexing_data>(resource, count))
        , indexing_(data_->data.get()) {}

    indexing_vector_t::indexing_vector_t(std::pmr::memory_resource* resource, uint64_t start, uint64_t count)
        : data_(std::make_shared<indexing_data>(resource, count))
        , indexing_(data_->data.get()) {
        for (uint64_t i = 0; i < count; i++) {
            set_index(i, start + i);
        }
    }

    indexing_vector_t::indexing_vector_t(std::shared_ptr<indexing_data> data) noexcept
        : data_(std::move(data))
        , indexing_(data_->data.get()) {}

    indexing_vector_t::indexing_vector_t(const indexing_vector_t& other) {
        data_ = other.data_;
        indexing_ = other.indexing_;
    }

    indexing_vector_t& indexing_vector_t::operator=(const indexing_vector_t& other) {
        data_ = other.data_;
        indexing_ = other.indexing_;
        return *this;
    }

    indexing_vector_t::indexing_vector_t(indexing_vector_t&& other) noexcept {
        indexing_ = other.indexing_;
        other.indexing_ = nullptr;
        data_ = std::move(other.data_);
    }

    indexing_vector_t& indexing_vector_t::operator=(indexing_vector_t&& other) noexcept {
        indexing_ = other.indexing_;
        other.indexing_ = nullptr;
        data_ = std::move(other.data_);
        return *this;
    }

    void indexing_vector_t::reset(uint64_t count) {
        data_ = std::make_shared<indexing_data>(resource(), count);
        indexing_ = data_->data.get();
    }

    void indexing_vector_t::reset(uint32_t* indexing) {
        data_.reset();
        indexing_ = indexing;
    }

    void indexing_vector_t::set_index(uint64_t index, uint64_t location) { indexing_[index] = location; }

    void indexing_vector_t::swap(uint64_t i, uint64_t j) noexcept { std::swap(indexing_[i], indexing_[j]); }

    uint32_t indexing_vector_t::get_index(uint64_t index) const { return indexing_ ? indexing_[index] : index; }

    uint32_t* indexing_vector_t::data() noexcept { return indexing_; }

    const uint32_t* indexing_vector_t::data() const noexcept { return indexing_; }

    std::shared_ptr<indexing_data> indexing_vector_t::slice(std::pmr::memory_resource* resource,
                                                            const indexing_vector_t& indexing,
                                                            uint64_t count) const {
        auto data = std::make_unique<indexing_data>(resource, count);
        auto result_ptr = data->data.get();
        for (uint64_t i = 0; i < count; i++) {
            result_ptr[i] = get_index(indexing.get_index(i));
        }
        return data;
    }

    uint32_t& indexing_vector_t::operator[](uint64_t index) const { return indexing_[index]; }

    bool indexing_vector_t::is_valid() const noexcept { return indexing_; }

} // namespace components::vector