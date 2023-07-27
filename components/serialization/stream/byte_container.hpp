#include <cstdint>

#include <tuple>
#include <utility>
#include <vector>

template<typename BinaryType>
class byte_container : public BinaryType {
public:
    using container_type = BinaryType;
    using subtype_type = std::uint64_t;

    byte_container() noexcept(noexcept(container_type()))
        : container_type() {}

    byte_container(const container_type& b) noexcept(noexcept(container_type(b)))
        : container_type(b) {}

    byte_container(container_type&& b) noexcept(noexcept(container_type(std::move(b))))
        : container_type(std::move(b)) {}

    byte_container(const container_type& b, subtype_type subtype_) noexcept(noexcept(container_type(b)))
        : container_type(b)
        , m_subtype(subtype_)
        , m_has_subtype(true) {}

    byte_container(container_type&& b, subtype_type subtype_) noexcept(noexcept(container_type(std::move(b))))
        : container_type(std::move(b))
        , m_subtype(subtype_)
        , m_has_subtype(true) {}

    bool operator==(const byte_container& rhs) const {
        return std::tie(static_cast<const BinaryType&>(*this), m_subtype, m_has_subtype) ==
               std::tie(static_cast<const BinaryType&>(rhs), rhs.m_subtype, rhs.m_has_subtype);
    }

    bool operator!=(const byte_container& rhs) const {
        return !(rhs == *this);
    }

    void set_subtype(subtype_type subtype_) noexcept {
        m_subtype = subtype_;
        m_has_subtype = true;
    }

    constexpr subtype_type subtype() const noexcept {
        return m_has_subtype ? m_subtype : static_cast<subtype_type>(-1);
    }
    constexpr bool has_subtype() const noexcept {
        return m_has_subtype;
    }

    void clear_subtype() noexcept {
        m_subtype = 0;
        m_has_subtype = false;
    }

private:
    subtype_type m_subtype = 0;
    bool m_has_subtype = false;
};

using byte_container_t = byte_container<std::vector<std::uint8_t>>;