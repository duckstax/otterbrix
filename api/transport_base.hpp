#pragma once

#include <memory>

namespace rocketjoe { namespace api {

enum class transport_type : unsigned char {
    http = 0x00,
    ws   = 0x01,
};


using transport_id = std::size_t ;

struct transport_base {

    transport_base(transport_type type,transport_id);
    virtual ~transport_base() = default;
    auto type() -> transport_type;
    auto id() -> transport_id;

protected:
    transport_type  type_;
    transport_id    id_;

};


struct transport final {
    transport() = default;
    transport(std::shared_ptr<transport_base> ptr):transport_(std::move(ptr)){}
    ~transport() = default;
    std::shared_ptr<transport_base> transport_;
};


template <typename T,typename ...Args>
inline transport make_transport(Args... args){
    transport tmp(std::move(std::make_shared<T>(std::forward<Args>(args)...)));
    return tmp;
}


}}