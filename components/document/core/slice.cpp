#include "slice.hpp"

namespace document {

    static constexpr size_t heap_alignment_mask = 0x07;

    PURE static inline bool is_heap_aligned(const void* p) {
        return (reinterpret_cast<size_t>(const_cast<void*>(p)) & heap_alignment_mask) == 0;
    }

    struct shared_buffer_t {
        std::atomic<uint32_t> _ref_count{1};
        uint8_t _buf[4] = {0, 0, 0, 0};

        static inline void* operator new(size_t basicSize, size_t bufferSize) noexcept {
            return malloc(basicSize - sizeof(shared_buffer_t::_buf) + bufferSize);
        }

        static inline void operator delete(void* self) {
            assert_precondition(is_heap_aligned(self));
            free(self);
        }

        inline void retain() noexcept {
            assert_precondition(is_heap_aligned(this));
            ++_ref_count;
        }

        inline void release() noexcept {
            assert_precondition(is_heap_aligned(this));
            if (--_ref_count == 0)
                delete this;
        }
    };


    PURE static shared_buffer_t* to_shared_buffer(const void* buf) noexcept {
        return reinterpret_cast<shared_buffer_t*>(reinterpret_cast<uint8_t*>(const_cast<void*>(buf)) - offsetof(shared_buffer_t, _buf));
    }

    template <class T>
    void* to_void(T* t) {
        return reinterpret_cast<void*>(t);
    }

    template <class T>
    const void* to_const_void(T* t) {
        return const_cast<const void*>(reinterpret_cast<void*>(t));
    }

    template <class T>
    uint8_t* to_uint8_t(const T* t) {
        return reinterpret_cast<uint8_t*>(const_cast<T*>(t));
    }

    template <class T>
    const uint8_t* to_const_uint8_t(const T* t) {
        return reinterpret_cast<const uint8_t*>(t);
    }

    template <class T>
    char* to_char(const T* t) {
        return reinterpret_cast<char*>(const_cast<T*>(t));
    }

    template <class T>
    const char* to_const_char(const T* t) {
        return reinterpret_cast<const char*>(t);
    }

    alloc_slice_t create_slice_result(size_t size) noexcept {
        auto sb = new (size) shared_buffer_t;
        if (!sb)
            return {};
        return alloc_slice_t(&sb->_buf, size);
    }

    alloc_slice_t copy_slice(pure_slice_t s) noexcept {
        if (!s.buf)
            return {};
        auto sb = new (s.size) shared_buffer_t;
        if (!sb)
            return {};
        memcpy(&sb->_buf, s.buf, s.size);
        return alloc_slice_t(&sb->_buf, s.size);
    }

    inline void retain_buf(const void* buf) noexcept {
        if (buf)
            to_shared_buffer(buf)->retain();
    }

    inline void release_buf(const void* buf) noexcept {
        if (buf)
            to_shared_buffer(buf)->release();
    }

    inline constexpr size_t _strlen(const char *str) noexcept {
        if (!str)
            return 0;
        auto c = str;
        while (*c) ++c;
        return size_t(c - str);
    }


    constexpr pure_slice_t::pure_slice_t(std::string_view str) noexcept
        : pure_slice_t(str.data(), str.length()) {}

    constexpr pure_slice_t::pure_slice_t(std::nullptr_t) noexcept
        : pure_slice_t() {}

    constexpr pure_slice_t::pure_slice_t(const char* str) noexcept
        : buf(str)
        , size(_strlen(str)) {}

    pure_slice_t::pure_slice_t(const std::string& str) noexcept
        : buf(&str[0])
        , size(str.size()) {}

    constexpr pure_slice_t::pure_slice_t(const void* b, size_t s) noexcept
        : buf(b), size(s) {
        check_valid_slice();
    }

    bool pure_slice_t::empty() const noexcept {
        return size == 0;
    }

    pure_slice_t::operator bool() const noexcept {
        return buf != nullptr;
    }

    const uint8_t* pure_slice_t::begin() const noexcept {
        return to_const_uint8_t(buf);
    }

    const uint8_t* pure_slice_t::end() const noexcept {
        return begin() + size;
    }

    bool pure_slice_t::is_valid_address(const void *addr) const noexcept {
        return size_t(diff_pointer(addr, buf)) <= size;
    }

    bool pure_slice_t::is_contains_address(const void *addr) const noexcept {
        return size_t(diff_pointer(addr, buf)) < size;
    }

    bool pure_slice_t::is_contains_address_range(pure_slice_t s) const noexcept {
        return s.buf >= buf && s.end() <= end();
    }

    const void* pure_slice_t::offset(size_t o) const noexcept {
        return to_const_void(to_uint8_t(buf) + check(o));
    }

    const uint8_t& pure_slice_t::operator[](size_t off) const noexcept {
        assert_precondition(off < size);
        return to_const_uint8_t(buf)[off];
    }

    slice_t pure_slice_t::operator()(size_t off, size_t sz) const noexcept  {
        assert_precondition(off + sz <= size);
        return slice_t(offset(off), sz);
    }

    slice_t pure_slice_t::find(pure_slice_t target) const noexcept {
        char* src = to_char(buf);
        char* search = to_char(target.buf);
        char* found = std::search(src, src + size, search, search + target.size);
        if(found == src + size) {
            return null_slice;
        }
        return {found, target.size};
    }

    int pure_slice_t::compare(const pure_slice_t &s) const noexcept {
        if (size == s.size) {
            if (size == 0)
                return 0;
            return memcmp(buf, s.buf, size);
        } else if (size < s.size) {
            if (size == 0)
                return -1;
            int result = memcmp(buf, s.buf, size);
            return result ? result : -1;
        } else {
            if (s.size == 0)
                return 1;
            int result = memcmp(buf, s.buf, s.size);
            return result ? result : 1;
        }
    }

    bool pure_slice_t::operator==(const pure_slice_t& s) const noexcept {
        return this->size == s.size && (this->size == 0 || memcmp(this->buf, s.buf, this->size) == 0);
    }

    bool pure_slice_t::operator!=(const pure_slice_t& s) const noexcept {
        return !(*this == s);
    }

    bool pure_slice_t::operator<(const pure_slice_t& s) const noexcept {
        return compare(s) < 0;
    }

    bool pure_slice_t::operator>(const pure_slice_t& s) const noexcept {
        return compare(s) > 0;
    }

    bool pure_slice_t::operator<=(const pure_slice_t& s) const noexcept {
        return compare(s) <= 0;
    }

    bool pure_slice_t::operator>=(const pure_slice_t& s) const noexcept {
        return compare(s) >= 0;
    }

    uint32_t pure_slice_t::hash() const noexcept {
        std::hash<std::string> hash;
        return uint32_t(hash(static_cast<std::string>(*this)));
    }

    void pure_slice_t::copy_to(void* dst) const noexcept {
        if (size > 0)
            ::memcpy(dst, buf, size);
    }

    pure_slice_t::operator std::string() const {
        return std::string(to_const_char(buf), size);
    }

    pure_slice_t::operator std::string_view() const noexcept {
        return std::string_view(to_const_char(buf), size);
    }

    std::string pure_slice_t::as_string() const {
        return static_cast<std::string>(*this);
    }

    void pure_slice_t::set_buf(const void *b NONNULL) noexcept {
        const_cast<const void*&>(buf) = b;
        check_valid_slice();
    }

    void pure_slice_t::set_size(size_t s) noexcept {
        const_cast<size_t&>(size) = s;
        check_valid_slice();
    }

    void pure_slice_t::set(const void *b, size_t s) noexcept {
        const_cast<const void*&>(buf) = b;
        const_cast<size_t&>(size) = s;
        check_valid_slice();
    }

    pure_slice_t& pure_slice_t::operator=(const pure_slice_t& s) noexcept {
        set(s.buf, s.size);
        return *this;
    }

    [[noreturn]] void pure_slice_t::fail_bad_alloc() {
#ifdef __cpp_exceptions
        throw std::bad_alloc();
#else
        ::fputs("*** FATAL ERROR: heap allocation failed (fleece/slice_t.cc) ***\n", stderr);
        std::terminate();
#endif
    }

    constexpr void pure_slice_t::check_valid_slice() const {
        assert_precondition(buf != nullptr || size == 0);
        assert_precondition(size < (1ull << (8*sizeof(void*)-1)));
    }

    size_t pure_slice_t::check(size_t offset) const {
        assert_precondition(offset <= size);
        return offset;
    }


    constexpr slice_t::slice_t(std::nullptr_t) noexcept
        : pure_slice_t() {}

    constexpr slice_t::slice_t(null_slice_t) noexcept
        : pure_slice_t()
    {}

    constexpr slice_t::slice_t(const void* b, size_t s) noexcept
        : pure_slice_t(b, s) {}

    slice_t::slice_t(const void* start NONNULL, const void* end NONNULL) noexcept
        : slice_t(start, size_t(diff_pointer(end, start))) {
        assert_precondition(end >= start);
    }

    constexpr slice_t::slice_t(const alloc_slice_t &s) noexcept
        : pure_slice_t(s) {}

    slice_t::slice_t(const std::string& str) noexcept
        : pure_slice_t(str) {}

    constexpr slice_t::slice_t(const char* str) noexcept
        : pure_slice_t(str) {}

    slice_t& slice_t::operator=(const alloc_slice_t& s) noexcept {
        return *this = slice_t(s);
    }

    slice_t& slice_t::operator=(std::nullptr_t) noexcept {
        set(nullptr, 0);
        return *this;
    }

    slice_t& slice_t::operator=(null_slice_t) noexcept {
        set(nullptr, 0);
        return *this;
    }

    constexpr slice_t::slice_t(std::string_view str) noexcept
        : pure_slice_t(str) {}

    void slice_t::release() {
        char *c = const_cast<char *>(static_cast<const char *>(buf));
        delete[] c;
    }


    constexpr alloc_slice_t::alloc_slice_t(std::nullptr_t) noexcept
    {}

    constexpr alloc_slice_t::alloc_slice_t(null_slice_t) noexcept
    {}

    alloc_slice_t::alloc_slice_t(size_t sz)
        : alloc_slice_t(create_slice_result(sz)) {
        if (_usually_false(!buf))
            fail_bad_alloc();
    }

    alloc_slice_t::alloc_slice_t(const void* b, size_t s)
        : pure_slice_t(b, s) {}

    alloc_slice_t::alloc_slice_t(const void* start NONNULL, const void* end NONNULL)
        : alloc_slice_t(slice_t(start, end)) {}

    alloc_slice_t::alloc_slice_t(const char* str)
        : alloc_slice_t(slice_t(str)) {}

    alloc_slice_t::alloc_slice_t(const std::string& str)
        : alloc_slice_t(slice_t(str)) {}

    alloc_slice_t::alloc_slice_t(pure_slice_t s)
        : alloc_slice_t(copy_slice(s)) {
        if (_usually_false(!buf) && s.buf)
            fail_bad_alloc();
    }

    alloc_slice_t::alloc_slice_t(const alloc_slice_t& s) noexcept
        : pure_slice_t(s) {
        retain();
    }

    alloc_slice_t::alloc_slice_t(alloc_slice_t&& s) noexcept
        : pure_slice_t(s) {
        s.set(nullptr, 0);
    }

    alloc_slice_t::alloc_slice_t(std::string_view str)
        : alloc_slice_t(slice_t(str)) {}

    alloc_slice_t::~alloc_slice_t() {
        release_buf(buf);
    }

    alloc_slice_t& alloc_slice_t::operator=(const alloc_slice_t& s) noexcept {
        if (_usually_true(s.buf != buf)) {
            release();
            assign_from(s);
            retain();
        }
        return *this;
    }

    alloc_slice_t &alloc_slice_t::operator=(alloc_slice_t &&s) noexcept {
        std::swap(reinterpret_cast<slice_t&>(*this), reinterpret_cast<slice_t&>(s));
        return *this;
    }

    alloc_slice_t& alloc_slice_t::operator=(pure_slice_t s) {
        return *this = alloc_slice_t(s);
    }

    alloc_slice_t& alloc_slice_t::operator=(std::nullptr_t) noexcept {
        reset();
        return *this;
    }

    alloc_slice_t& alloc_slice_t::operator=(const char* str NONNULL) {
        *this = slice_t(str);
        return *this;
    }

    alloc_slice_t& alloc_slice_t::operator=(const std::string& str) {
        *this = slice_t(str);
        return *this;
    }

    void alloc_slice_t::reset() noexcept {
        release();
        assign_from(null_slice);
    }

    void alloc_slice_t::reset(size_t sz) {
        *this = alloc_slice_t(sz);
    }

    void alloc_slice_t::resize(size_t new_size) {
        if (new_size == size) {
            return;
        } else if (buf == nullptr) {
            reset(new_size);
        } else {
            alloc_slice_t new_slice(new_size);
            ::memcpy(const_cast<void*>(new_slice.buf), buf, std::min(size, new_size));
            *this = std::move(new_slice);
        }
    }

    void alloc_slice_t::append(pure_slice_t source) {
        if (_usually_false(source.size == 0))
            return;
        auto dst_off = size;
        auto src_off = size_t(diff_pointer(source.buf, buf));

        resize(dst_off + source.size);

        const void *src;
        if (_usually_false(src_off < dst_off)) {
            src = offset(src_off);
        } else {
            src = source.buf;
        }

        ::memcpy(const_cast<void*>(offset(dst_off)), src, source.size);
    }

    alloc_slice_t& alloc_slice_t::operator=(std::string_view str) {
        *this = slice_t(str);
        return *this;
    }

    alloc_slice_t& alloc_slice_t::retain() noexcept {
        retain_buf(buf);
        return *this;
    }
    void alloc_slice_t::release() noexcept {
        release_buf(buf);
    }

    void alloc_slice_t::retain(slice_t s) noexcept {
        reinterpret_cast<alloc_slice_t*>(&s)->retain();
    }

    void alloc_slice_t::release(slice_t s) noexcept {
        reinterpret_cast<alloc_slice_t*>(&s)->release();
    }

    void alloc_slice_t::assign_from(pure_slice_t s) {
        set(s.buf, s.size);
    }

}
