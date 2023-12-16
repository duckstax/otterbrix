#include "dict.hpp"

#include <unordered_map>

#include <components/document/internal/heap.hpp>
#include <components/document/support/better_assert.hpp>

namespace document::impl {

    namespace internal {

        struct key_hash {
            size_t operator()(const dict_t::key_t& key) const { return std::hash<std::string_view>{}(key.string()); }
        };

        struct key_eq {
            bool operator()(const dict_t::key_t& key1, const dict_t::key_t& key2) const {
                return key1.string() == key2.string();
            }
        };

        class heap_dict_t : public heap_collection_t {
            retained_t<array_t> a_;
            std::unordered_map<dict_t::key_t, uint32_t, key_hash, key_eq> map_;
            uint32_t count_{0};

        public:
            explicit heap_dict_t(const dict_t* dict = nullptr);

            dict_t* dict() const { return reinterpret_cast<dict_t*>(const_cast<value_t*>(as_value())); }

            uint32_t count() const { return count_; }

            bool empty() const { return count_ == 0; }

            void copy_children(copy_flags flags) { a_->copy_children(flags); }

            const value_t* get_key(uint32_t index) const noexcept { return a_->get(index); }

            const value_t* get(uint32_t index) const noexcept { return a_->get(index + 1); }

            const value_t* get(const dict_t::key_t& key) const noexcept {
                auto it = map_.find(key);
                if (it != map_.end()) {
                    return get(it->second);
                }
                return nullptr;
            }

            const value_t* get(std::string_view key) const noexcept { return get(encode_key_(key)); }

            value_slot_t& slot(std::string_view key_str) {
                set_changed(true);
                auto key = encode_key_(key_str);
                auto it = map_.find(key);
                if (it == map_.end()) {
                    ++count_;
                    map_.emplace(key, a_->count());
                    a_->append(key_str);
                    a_->append(nullptr);
                    return a_->slot(a_->count() - 1);
                }
                return a_->slot(it->second + 1);
            }

            void remove(std::string_view str_key) {
                auto key = encode_key_(str_key);
                auto it = map_.find(key);
                if (it != map_.end()) {
                    --count_;
                    a_->remove(it->second, 2);
                    map_.erase(key);
                    set_changed(true);
                }
            }

            void remove_all() {
                if (count_ == 0) {
                    return;
                }
                a_->resize(0);
                map_.clear();
                count_ = 0;
                set_changed(true);
            }

        private:
            dict_t::key_t encode_key_(std::string_view key) const noexcept { return dict_t::key_t{key}; }
        };

        heap_dict_t* heap_dict(const dict_t* dict) {
            return reinterpret_cast<heap_dict_t*>(internal::heap_collection_t::as_heap_value(dict));
        }

        heap_dict_t::heap_dict_t(const dict_t* dict)
            : heap_collection_t(tag_dict) {
            if (dict) {
                count_ = dict->count();
                auto hd = heap_dict(dict);
                a_ = array_t::new_array(hd->a_);
                map_ = hd->map_;
            } else {
                a_ = array_t::new_array();
            }
        }

    } // namespace internal

    using namespace internal;

    dict_t::key_t::key_t(std::string_view raw_str)
        : raw_str_({raw_str.data(), raw_str.size()}) {}

    dict_t::key_t::key_t(const std::string& raw_str)
        : raw_str_(raw_str) {}

    dict_t::key_t::~key_t() = default;

    std::string_view dict_t::key_t::string() const noexcept { return {raw_str_.data(), raw_str_.size()}; }

    int dict_t::key_t::compare(const key_t& k) const noexcept { return raw_str_.compare(k.raw_str_); }

    dict_t::iterator::iterator(const dict_t* d) noexcept
        : source_(d)
        , count_(d->count())
        , pos_(0) {
        set_value_();
    }

    uint32_t dict_t::iterator::count() const noexcept { return count_; }

    std::string_view dict_t::iterator::key_string() const noexcept {
        if (key_) {
            return key()->as_string();
        }
        return {};
    }

    const value_t* dict_t::iterator::key() const noexcept { return key_; }

    const value_t* dict_t::iterator::value() const noexcept { return value_; }

    dict_t::iterator::operator bool() const noexcept { return pos_ < count_; }

    dict_t::iterator& dict_t::iterator::operator++() {
        ++pos_;
        set_value_();
        return *this;
    }

    dict_t::iterator& dict_t::iterator::operator+=(uint32_t n) {
        pos_ += n;
        set_value_();
        return *this;
    }

    void dict_t::iterator::set_value_() {
        if (pos_ < count_) {
            auto hd = heap_dict(source_);
            key_ = hd->get_key(2 * pos_);
            value_ = hd->get(2 * pos_);
        } else {
            key_ = nullptr;
            value_ = nullptr;
        }
    }

    dict_t::dict_t()
        : value_t(internal::tag_dict, 0, 0) {}

    retained_t<dict_t> dict_t::new_dict(const dict_t* d, copy_flags flags) {
        auto hd = retained(new internal::heap_dict_t(d));
        if (flags) {
            hd->copy_children(flags);
        }
        return hd->dict();
    }

    const dict_t* dict_t::empty_dict() {
        static const auto empty_dict_ = new_dict();
        return empty_dict_.get();
    }

    uint32_t dict_t::count() const noexcept { return heap_dict(this)->count(); }

    bool dict_t::empty() const noexcept { return heap_dict(this)->empty(); }

    const value_t* dict_t::get(key_t& key) const noexcept { return heap_dict(this)->get(key); }

    const value_t* dict_t::get(std::string_view key) const noexcept { return heap_dict(this)->get(key); }

    bool dict_t::is_equals(const dict_t* dv) const noexcept {
        dict_t::iterator i(this);
        dict_t::iterator j(dv);
        if (i.count() != j.count()) {
            return false;
        }
        if (shared_keys() == dv->shared_keys()) {
            for (; i; ++i, ++j) {
                if (i.key_string() != j.key_string() || !i.value()->is_equal(j.value())) {
                    return false;
                }
            }
        } else {
            unsigned n = 0;
            for (; i; ++i, ++n) {
                auto dvalue = dv->get(i.key_string());
                if (!dvalue || !i.value()->is_equal(dvalue)) {
                    return false;
                }
            }
            if (dv->count() != n) {
                return false;
            }
        }
        return true;
    }

    retained_t<dict_t> dict_t::copy(copy_flags f) const { return new_dict(this, f); }

    retained_t<internal::heap_collection_t> dict_t::mutable_copy() const { return new heap_dict_t(this); }

    void dict_t::copy_children(copy_flags flags) const { heap_dict(this)->copy_children(flags); }

    bool dict_t::is_changed() const { return heap_dict(this)->is_changed(); }

    void dict_t::remove(std::string_view key) { heap_dict(this)->remove(key); }

    void dict_t::remove_all() { heap_dict(this)->remove_all(); }

    dict_t::iterator dict_t::begin() const noexcept { return dict_t::iterator(this); }

    internal::value_slot_t& dict_t::slot(std::string_view key) { return heap_dict(this)->slot(key); }

} // namespace document::impl
