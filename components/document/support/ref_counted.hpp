#pragma once

#include <algorithm>
#include <atomic>
#include <components/document/support/platform_compat.hpp>
#include <utility>

namespace document {

    class ref_counted_t {
    public:
        ref_counted_t() = default;

        int ref_count() const PURE;

    protected:
        ref_counted_t(const ref_counted_t&);
        virtual ~ref_counted_t();

    private:
        template<typename REFCOUNTED>
        friend REFCOUNTED* retain(REFCOUNTED* r) noexcept;
        friend void release(const ref_counted_t* r) noexcept;
        friend void assign_ref(ref_counted_t*& dst, ref_counted_t* src) noexcept;

#if DEBUG
        void retain_() const noexcept { _careful_retain(); }
        void release_() const noexcept { _careful_release(); }
#else
        ALWAYS_INLINE void retain_() const noexcept { ++_ref_count; }
        void release_() const noexcept;
#endif

        static constexpr int32_t careful_initial_ref_count = -6666666;
        void _careful_retain() const noexcept;
        void _careful_release() const noexcept;

        mutable std::atomic<int32_t> _ref_count
#if DEBUG
            {careful_initial_ref_count};
#else
            {0};
#endif
    };

    template<typename REFCOUNTED>
    ALWAYS_INLINE REFCOUNTED* retain(REFCOUNTED* r) noexcept {
        if (r)
            r->retain_();
        return r;
    }

    NOINLINE void release(const ref_counted_t* r) noexcept;
    NOINLINE void assign_ref(ref_counted_t*& dst, ref_counted_t* src) noexcept;

    template<typename T>
    static inline void assign_ref(T*& dst, ref_counted_t* src) noexcept {
        assign_ref(reinterpret_cast<ref_counted_t*&>(dst), src);
    }

    template<typename T>
    class retained_t {
    public:
        retained_t() noexcept
            : _ref(nullptr) {}
        retained_t(T* t) noexcept
            : _ref(retain(t)) {}

        retained_t(const retained_t& r) noexcept
            : _ref(retain(r._ref)) {}
        retained_t(retained_t&& r) noexcept
            : _ref(std::move(r).detach()) {}

        template<typename U>
        retained_t(const retained_t<U>& r) noexcept
            : _ref(retain(r._ref)) {}

        template<typename U>
        retained_t(retained_t<U>&& r) noexcept
            : _ref(std::move(r).detach()) {}

        ~retained_t() { release(_ref); }

        operator T*() const& noexcept PURE STEPOVER { return _ref; }
        T* operator->() const noexcept PURE STEPOVER { return _ref; }
        T* get() const noexcept PURE STEPOVER { return _ref; }

        explicit operator bool() const PURE { return (_ref != nullptr); }

        retained_t& operator=(T* t) noexcept {
            assign_ref(_ref, t);
            return *this;
        }

        retained_t& operator=(const retained_t& r) noexcept { return *this = r._ref; }

        template<typename U>
        retained_t& operator=(const retained_t<U>& r) noexcept {
            return *this = r._ref;
        }

        retained_t& operator=(retained_t&& r) noexcept {
            std::swap(_ref, r._ref);
            return *this;
        }

        template<typename U>
        retained_t& operator=(retained_t<U>&& r) noexcept {
            auto old_ref = _ref;
            if (old_ref != r._ref) {
                _ref = std::move(r).detach();
                release(old_ref);
            }
            return *this;
        }

        T* detach() && noexcept {
            auto r = _ref;
            _ref = nullptr;
            return r;
        }
        operator T*() const&& = delete;

    private:
        template<class U>
        friend class retained_t;
        template<class U>
        friend class retained_const_t;
        template<class U>
        friend retained_t<U> adopt(U*) noexcept;

        retained_t(T* t, bool) noexcept
            : _ref(t) {}

        T* _ref;
    };

    template<typename T>
    class retained_const_t {
    public:
        retained_const_t() noexcept
            : _ref(nullptr) {}
        retained_const_t(const T* t) noexcept
            : _ref(retain(t)) {}
        retained_const_t(const retained_const_t& r) noexcept
            : _ref(retain(r._ref)) {}
        retained_const_t(retained_const_t&& r) noexcept
            : _ref(std::move(r).detach()) {}
        retained_const_t(const retained_t<T>& r) noexcept
            : _ref(retain(r._ref)) {}
        retained_const_t(retained_t<T>&& r) noexcept
            : _ref(std::move(r).detach()) {}
        ALWAYS_INLINE ~retained_const_t() { release(_ref); }

        operator const T*() const& noexcept PURE STEPOVER { return _ref; }
        const T* operator->() const noexcept PURE STEPOVER { return _ref; }
        const T* get() const noexcept PURE STEPOVER { return _ref; }

        retained_const_t& operator=(const T* t) noexcept {
            auto old_ref = _ref;
            _ref = retain(t);
            release(old_ref);
            return *this;
        }

        retained_const_t& operator=(const retained_const_t& r) noexcept { return *this = r._ref; }

        retained_const_t& operator=(retained_const_t&& r) noexcept {
            std::swap(_ref, r._ref);
            return *this;
        }

        template<typename U>
        retained_const_t& operator=(const retained_t<U>& r) noexcept {
            return *this = r._ref;
        }

        template<typename U>
        retained_const_t& operator=(retained_t<U>&& r) noexcept {
            std::swap(_ref, r._ref);
            return *this;
        }

        const T* detach() && noexcept {
            auto r = _ref;
            _ref = nullptr;
            return r;
        }

        operator const T*() const&& = delete;

    private:
        const T* _ref;
    };

    template<typename REFCOUNTED>
    inline retained_t<REFCOUNTED> retained(REFCOUNTED* r) noexcept {
        return retained_t<REFCOUNTED>(r);
    }

    template<typename REFCOUNTED>
    inline retained_const_t<REFCOUNTED> retained(const REFCOUNTED* r) noexcept {
        return retained_const_t<REFCOUNTED>(r);
    }

    template<typename REFCOUNTED>
    inline retained_t<REFCOUNTED> adopt(REFCOUNTED* r) noexcept {
        return retained_t<REFCOUNTED>(r, false);
    }

    template<class T, class... _Args>
    static inline retained_t<T> make_retained(_Args&&... __args) {
        return retained_t<T>(new T(std::forward<_Args>(__args)...));
    }

    template<typename REFCOUNTED>
    ALWAYS_INLINE REFCOUNTED* retain(retained_t<REFCOUNTED>&& retained) noexcept {
        return std::move(retained).detach();
    }

    template<typename REFCOUNTED>
    ALWAYS_INLINE const REFCOUNTED* retain(retained_const_t<REFCOUNTED>&& retained) noexcept {
        return std::move(retained).detach();
    }

} // namespace document
