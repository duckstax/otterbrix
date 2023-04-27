#pragma once

#include <memory>
#include <string>

#include <components/document/core/array.hpp>

namespace document::impl {

    class dict_t : public value_t {
    public:
        class key_t {
        public:
            explicit key_t(std::string_view);
            explicit key_t(const std::string&);
            ~key_t();
            std::string_view string() const noexcept;
            int compare(const key_t& k) const noexcept;

        private:
            std::string const raw_str_;
        };

        class iterator {
        public:
            explicit iterator(const dict_t* d) noexcept;

            uint32_t count() const noexcept PURE;

            std::string_view key_string() const noexcept;
            const value_t* key() const noexcept PURE;
            const value_t* value() const noexcept PURE;

            explicit operator bool() const noexcept PURE;
            iterator& operator++();
            iterator& operator+=(uint32_t n);

        private:
            const dict_t* source_;
            const value_t* key_;
            const value_t* value_;
            uint32_t count_;
            uint32_t pos_;

            void set_value_();
        };

        static retained_t<dict_t> new_dict(const dict_t* d = nullptr, copy_flags flags = default_copy);
        static const dict_t* empty_dict();

        uint32_t count() const noexcept PURE;
        bool empty() const noexcept PURE;

        const value_t* get(key_t& key) const noexcept;
        const value_t* get(std::string_view key) const noexcept PURE;

        bool is_equals(const dict_t* NONNULL) const noexcept PURE;

        retained_t<dict_t> copy(copy_flags f = default_copy) const;
        retained_t<internal::heap_collection_t> mutable_copy() const;
        void copy_children(copy_flags flags = default_copy) const;

        bool is_changed() const;

        template<typename T>
        void set(std::string_view key, T value) {
            slot(key).set(value);
        }

        void remove(std::string_view key);
        void remove_all();

        iterator begin() const noexcept;

    private:
        dict_t();

        internal::value_slot_t& slot(std::string_view key);
    };

    using dict_iterator_t = dict_t::iterator;

} // namespace document::impl
