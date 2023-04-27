#include "array.hpp"
#include <vector>
#include <components/document/internal/heap.hpp>
#include <components/document/support/varint.hpp>

namespace document::impl {

    namespace internal {

        class heap_array_t : public heap_collection_t {
            std::vector<value_slot_t> items_;

        public:
            explicit heap_array_t(uint32_t initial_count)
                : heap_collection_t(tag_array)
                , items_(initial_count) {}

            explicit heap_array_t(const array_t* array);

            array_t* array() const {
                return reinterpret_cast<array_t*>(const_cast<value_t*>(as_value()));
            }

            uint32_t count() const {
                return uint32_t(items_.size());
            }

            bool empty() const {
                return items_.empty();
            }

            const value_t* get(uint32_t index) {
                if (index >= count()) {
                    return nullptr;
                }
                return items_[index].as_value();
            }

            void resize(uint32_t new_size) {
                if (new_size == count()) {
                    return;
                }
                items_.resize(new_size, value_slot_t(nullptr));
                set_changed(true);
            }

            void insert(uint32_t where, uint32_t n) {
                _throw_if(where > count(), error_code::out_of_range, "insert position is past end of array");
                if (n == 0) {
                    return;
                }
                items_.insert(items_.begin() + where, n, value_slot_t(nullptr));
                set_changed(true);
            }

            void remove(uint32_t where, uint32_t n) {
                _throw_if(where + n > count(), error_code::out_of_range, "remove range is past end of array");
                if (n == 0) {
                    return;
                }
                auto at = items_.begin() + where;
                items_.erase(at, at + n);
                set_changed(true);
            }

            void copy_children(copy_flags flags) {
                for (auto& entry : items_) {
                    entry.copy_value(flags);
                }
            }

            value_slot_t& slot(uint32_t index) {
                set_changed(true);
                return items_[index];
            }

            value_slot_t& appending() {
                set_changed(true);
                items_.emplace_back();
                return items_.back();
            }
        };

        heap_array_t* heap_array(const array_t* array) {
            return reinterpret_cast<heap_array_t*>(internal::heap_collection_t::as_heap_value(array));
        }

        heap_array_t::heap_array_t(const array_t* array)
            : heap_collection_t(tag_array)
            , items_(array ? array->count() : 0) {
            if (array) {
                auto ha = heap_array(array);
                items_ = ha->items_;
            }
        }

    } // namespace internal

    using namespace internal;

    array_t::iterator::iterator(const array_t* a) noexcept
        : source_(a)
        , value_(nullptr)
        , count_(heap_array(a)->count())
        , pos_(0) {
        set_value_();
    }

    uint32_t array_t::iterator::count() const noexcept {
        return count_;
    }

    const value_t* array_t::iterator::value() const noexcept {
        return value_;
    }

    array_t::iterator::operator const value_t*() const noexcept {
        return value_;
    }

    const value_t* array_t::iterator::operator->() const noexcept {
        return value_;
    }

    array_t::iterator::operator bool() const noexcept {
        return pos_ < count_;
    }

    array_t::iterator& array_t::iterator::operator++() {
        ++pos_;
        set_value_();
        return *this;
    }

    array_t::iterator& array_t::iterator::operator+=(uint32_t n) {
        pos_ += n;
        set_value_();
        return *this;
    }

    void array_t::iterator::set_value_() {
        if (pos_ < count_) {
            value_ = heap_array(source_)->get(pos_);
        } else {
            value_ = nullptr;
        }
    }


    retained_t<array_t> array_t::new_array(uint32_t initial_count) {
        return (new internal::heap_array_t(initial_count))->array();
    }

    retained_t<array_t> array_t::new_array(const array_t* a, copy_flags flags) {
        auto ha = retained(new internal::heap_array_t(a));
        if (flags) {
            ha->copy_children(flags);
        }
        return ha->array();
    }

    const array_t* array_t::empty_array() {
        static const array_t empty_array_;
        return &empty_array_;
    }

    array_t::array_t()
        : value_t(tag_array, 0, 0) {}

    uint32_t array_t::count() const noexcept {
        return heap_array(this)->count();
    }

    bool array_t::empty() const noexcept {
        return heap_array(this)->empty();
    }

    const value_t* array_t::get(uint32_t index) const noexcept {
        return heap_array(this)->get(index);
    }

    array_iterator_t array_t::begin() const noexcept {
        return iterator(this);
    }

    retained_t<array_t> array_t::copy(copy_flags f) const {
        return new_array(this, f);
    }

    retained_t<internal::heap_collection_t> array_t::mutable_copy() const {
        return new heap_array_t(this);
    }

    void array_t::copy_children(copy_flags flags) const {
        heap_array(this)->copy_children(flags);
    }

    bool array_t::is_changed() const {
        return heap_array(this)->is_changed();
    }

    void array_t::resize(uint32_t new_size) {
        heap_array(this)->resize(new_size);
    }

    void array_t::insert(uint32_t where, uint32_t n) {
        heap_array(this)->insert(where, n);
    }

    void array_t::remove(uint32_t where, uint32_t n) {
        heap_array(this)->remove(where, n);
    }

    internal::value_slot_t& array_t::slot(uint32_t index) {
        return heap_array(this)->slot(index);
    }

    internal::value_slot_t& array_t::appending() {
        return heap_array(this)->appending();
    }

} // namespace document::impl
