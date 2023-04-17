#include "mutable_array.hpp"
#include <components/document/mutable/mutable_dict.h>
#include <components/document/support/varint.hpp>
#include <components/document/support/better_assert.hpp>

namespace document::impl::internal {

    heap_array_t::heap_array_t()
        : heap_collection_t(tag_array)
    {}

    heap_array_t::heap_array_t(uint32_t initial_count)
        : heap_collection_t(tag_array)
        , items_(initial_count)
    {}

    heap_array_t::heap_array_t(const array_t *array)
        : heap_collection_t(tag_array)
        , items_(array ? array->count() : 0)
    {
        if (array) {
            auto ha = array->heap_array();
            items_ = ha->items_;
            source_ = ha->source_;
        }
    }

    array_t *heap_array_t::as_mutable_array() const {
        return static_cast<array_t*>(const_cast<value_t*>(as_value()));
    }

    uint32_t heap_array_t::count() const {
        return uint32_t(items_.size());
    }

    bool heap_array_t::empty() const {
        return items_.empty();
    }

    const array_t *heap_array_t::source() const {
        return source_;
    }

    void heap_array_t::populate(unsigned from_index) {
        if (!source_)
            return;
        auto dst = items_.begin() + from_index;
        array_t::iterator src(source_);
        for (src += from_index; src && dst != items_.end(); ++src, ++dst) {
            if (!*dst)
                dst->set(src.value());
        }
    }

    const value_t* heap_array_t::get(uint32_t index) {
        if (index >= count())
            return nullptr;
        auto &item = items_[index];
        if (item)
            return item.as_value();
        assert(source_);
        return source_->get(index);
    }

    void heap_array_t::resize(uint32_t new_size) {
        if (new_size == count())
            return;
        items_.resize(new_size, value_slot_t(nullptr));
        set_changed(true);
    }

    void heap_array_t::insert(uint32_t where, uint32_t n) {
        _throw_if(where > count(), error_code::out_of_range, "insert position is past end of array");
        if (n == 0)
            return;
        populate(where);
        items_.insert(items_.begin() + where,  n, value_slot_t(nullptr));
        set_changed(true);
    }

    void heap_array_t::remove(uint32_t where, uint32_t n) {
        _throw_if(where + n > count(), error_code::out_of_range, "remove range is past end of array");
        if (n == 0)
            return;
        populate(where + n);
        auto at = items_.begin() + where;
        items_.erase(at, at + n);
        set_changed(true);
    }

    heap_collection_t* heap_array_t::get_mutable(uint32_t index, tags if_type) {
        if (index >= count())
            return nullptr;
        retained_t<heap_collection_t> result = nullptr;
        auto &mval = items_[index];
        if (mval) {
            result = mval.make_mutable(if_type);
        } else if (source_) {
            result = heap_collection_t::mutable_copy(source_->get(index), if_type);
            if (result)
                items_[index].set(result->as_value());
        }
        if (result)
            set_changed(true);
        return result;
    }

    value_slot_t& heap_array_t::setting(uint32_t index) {
#if DEBUG
        assert_precondition(index < items_.size());
#endif
        set_changed(true);
        return items_[index];
    }

    value_slot_t& heap_array_t::appending() {
        set_changed(true);
        items_.emplace_back();
        return items_.back();
    }

    value_slot_t &heap_array_t::inserting(uint32_t index) {
        insert(index, 1);
        return setting(index);
    }

    const value_slot_t* heap_array_t::first() {
        populate(0);
        return &items_.front();
    }

    void heap_array_t::disconnect_from_source() {
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

    void heap_array_t::copy_children(copy_flags flags) {
        if (flags & copy_immutables)
            disconnect_from_source();
        for (auto &entry : items_)
            entry.copy_value(flags);
    }

}
