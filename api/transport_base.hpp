#pragma once

#include <memory>
#include <boost/optional.hpp>
#include <actor-zeta/intrusive_ptr.hpp>

namespace rocketjoe { namespace api {

enum class transport_type : unsigned char {
    http = 0x00,
    ws   = 0x01,
};


using transport_id = std::size_t ;

struct transport_base : public actor_zeta::ref_counted  {

    transport_base(transport_type type,transport_id);
    virtual ~transport_base() = default;
    auto type() -> transport_type;
    auto id() -> transport_id;

protected:
    transport_type  type_;
    transport_id    id_;

};

using transport = actor_zeta::intrusive_ptr<transport_base>;


template <typename T,typename ...Args>
inline auto make_transport(Args... args) -> transport {
    return actor_zeta::intrusive_ptr<T>(new T (std::forward<Args>(args)...));
}


}}