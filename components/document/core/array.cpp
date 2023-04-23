#include "array.hpp"
#include <components/document/core/internal.hpp>
#include <components/document/internal/heap.hpp>
#include <components/document/mutable/mutable_dict.hpp>
#include <components/document/support/varint.hpp>

namespace document::impl {

    namespace internal {

        class heap_array_t : public heap_collection_t {
        public:
            explicit heap_array_t(uint32_t initial_count)
                : heap_collection_t(tag_array)
                , items_(initial_count)
            {}

            explicit heap_array_t(const array_t *array);

            uint32_t count() const {
                return uint32_t(items_.size());
            }

            bool empty() const {
                return items_.empty();
            }

            const array_t *source() const {
                return source_;
            }

            const value_t* get(uint32_t index) {
                if (index >= count()) {
                    return nullptr;
                }
                auto &item = items_[index];
                if (item) {
                    return item.as_value();
                }
                assert(source_);
                return source_->get(index);
            }

            void resize(uint32_t new_size) {
                if (new_size == count()) {
                    return;
                }
                items_.resize(new_size, value_slot_t(nullptr));
                set_changed(true);
            }

            void populate(unsigned from_index) {
                if (!source_)
                    return;
                auto dst = items_.begin() + from_index;
                array_t::iterator src(source_);
                for (src += from_index; src && dst != items_.end(); ++src, ++dst) {
                    if (!*dst)
                        dst->set(src.value());
                }
            }

            void insert(uint32_t where, uint32_t n) {
                _throw_if(where > count(), error_code::out_of_range, "insert position is past end of array");
                if (n == 0) {
                    return;
                }
                populate(where);
                items_.insert(items_.begin() + where,  n, value_slot_t(nullptr));
                set_changed(true);
            }

            void remove(uint32_t where, uint32_t n) {
                _throw_if(where + n > count(), error_code::out_of_range, "remove range is past end of array");
                if (n == 0) {
                    return;
                }
                populate(where + n);
                auto at = items_.begin() + where;
                items_.erase(at, at + n);
                set_changed(true);
            }

            void copy_children(copy_flags flags) {
                if (flags & copy_immutables) {
                    if (!source_)
                        return;
                    uint32_t index = 0;
                    for (auto &mval : items_) {
                        if (!mval)
                            mval.set(source_->get(index));
                        ++index;
                    }
                    source_ = nullptr;
                }
                for (auto &entry : items_) {
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

        private:
            std::vector<value_slot_t> items_;
            retained_const_t<array_t> source_;
        };

        heap_array_t* heap_array(const array_t *array) {
            return dynamic_cast<heap_array_t*>(internal::heap_collection_t::as_heap_value(array));
        }

        array_t *to_array(const heap_array_t *ha) {
            return static_cast<array_t*>(const_cast<value_t*>(ha->as_value()));
        }

        heap_array_t::heap_array_t(const array_t *array)
            : heap_collection_t(tag_array)
            , items_(array ? array->count() : 0) {
            if (array) {
                auto ha = heap_array(array);
                items_ = ha->items_;
                source_ = ha->source_;
            }
        }

    } // namespace internal

    using namespace internal;

    array_t::iterator::iterator(const value_t* a) noexcept {
        if (_usually_false(a == nullptr)) {
            first_ = nullptr;
            width_ = size_narrow;
            count_ = 0;
        } else if (_usually_true(!a->is_mutable())) {
            first_ = reinterpret_cast<const value_t*>(&a->byte_[2]);
            width_ = a->is_wide_array() ? size_wide : size_narrow;
            count_ = a->count_value();
            if (_usually_false(count_ == long_array_count)) {
                uint32_t extra_count;
                size_t count_size = get_uvar_int32(std::string_view(reinterpret_cast<const char*>(first_), 10), &extra_count);
                if (_usually_true(count_size > 0))
                    count_ += extra_count;
                else
                    count_ = 0;
                first_ = offsetby(first_, static_cast<std::ptrdiff_t>(count_size + (count_size & 1)));
            }
        } else {
            auto mcoll = heap_value_t::as_heap_value(a);
            heap_array_t* mut_array;
            if (a->tag() == tag_array) {
                mut_array = dynamic_cast<heap_array_t*>(mcoll);
                count_ = mut_array->count();
            } else {
                mut_array = heap_array(dynamic_cast<heap_dict_t*>(mcoll)->array_key_value());
                count_ = mut_array->count() / 2;
            }
            first_ = count_ ? reinterpret_cast<const value_t*>(mut_array->get(0)) : nullptr;
            width_ = sizeof(internal::value_slot_t);
        }
        value_ = first_value();
    }

    const value_t* array_t::iterator::deref(const value_t* v) const noexcept {
        if (_usually_false(is_mutable()))
            return reinterpret_cast<const internal::value_slot_t*>(v)->as_value();
        return v->deref(width_ == size_wide);
    }

    const value_t* array_t::iterator::operator[](unsigned index) const noexcept {
        if (_usually_false(index >= count_))
            return nullptr;
        if (width_ == size_narrow)
            return offsetby(first_, size_narrow * index)->deref<false>();
        else if (_usually_true(width_ == size_wide))
            return offsetby(first_, size_wide * index)->deref<true>();
        else
            return (reinterpret_cast<const internal::value_slot_t*>(first_) + index)->as_value();
    }

    const value_t* array_t::iterator::second() const noexcept {
        return offsetby(first_, width_);
    }

    const value_t* array_t::iterator::first_value() const noexcept {
        if (_usually_false(count_ == 0))
            return nullptr;
        return first_;//deref(first_);
    }

    size_t array_t::iterator::index_of(const value_t* v) const noexcept {
        return (reinterpret_cast<size_t>(v) - reinterpret_cast<size_t>(first_)) / width_;
    }

    void array_t::iterator::offset(uint32_t n) {
        _throw_if(n > count_, error_code::out_of_range, "iterating past end of array");
        count_ -= n;
        if (_usually_true(count_ > 0))
            first_ = offsetby(first_, width_ * n);
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

    const value_t* array_t::iterator::read() noexcept {
        auto v = value_;
        ++(*this);
        return v;
    }

    array_t::iterator::operator bool() const noexcept { return count_ > 0; }

    bool array_t::iterator::is_mutable() const noexcept {
        return width_ > 4;
    }

    array_iterator_t& array_iterator_t::operator++() {
        offset(1);
        value_ = first_value();
        return *this;
    }

    array_iterator_t& array_iterator_t::operator+=(uint32_t n) {
        offset(n);
        value_ = first_value();
        return *this;
    }



    array_t::array_t()
        : value_t(tag_array, 0, 0)
    {}

    uint32_t array_t::count() const noexcept {
        if (_usually_false(is_mutable())) {
            return heap_array(this)->count();
        }
        return iterator(this).count_;
    }

    bool array_t::empty() const noexcept {
        if (_usually_false(is_mutable())) {
            return heap_array(this)->empty();
        }
        return count_is_zero();
    }

    const value_t* array_t::get(uint32_t index) const noexcept {
        if (_usually_false(is_mutable())) {
            return heap_array(this)->get(index);
        }
        return iterator(this)[index];
    }

    array_iterator_t array_t::begin() const noexcept {
        return iterator(this);
    }

    retained_t<array_t> array_t::new_array(uint32_t initial_count) {
        return to_array(new internal::heap_array_t(initial_count));
    }

    retained_t<array_t> array_t::new_array(const array_t *a, copy_flags flags) {
        auto ha = retained(new internal::heap_array_t(a));
        if (flags) {
            ha->copy_children(flags);
        }
        return to_array(ha);
    }

    const array_t* array_t::empty_array() {
        static const array_t empty_array_;
        return &empty_array_;
    }

    retained_t<array_t> array_t::copy(copy_flags f) {
        return new_array(this, f);
    }

    retained_t<internal::heap_collection_t> array_t::mutable_copy() const {
        return new heap_array_t(this);
    }

    void array_t::copy_children(copy_flags flags) {
        heap_array(this)->copy_children(flags);
    }

    const array_t *array_t::source() const {
        return heap_array(this)->source();
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
