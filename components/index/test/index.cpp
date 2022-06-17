#include <catch2/catch.hpp>

#include <actor-zeta/detail/pmr/memory_resource.hpp>
#include <actor-zeta/detail/pmr/default_resource.hpp>

#include "components/index/index.hpp"

class dummy final : public index_t {
public:
    explicit dummy(actor_zeta::detail::pmr::memory_resource* resource): index_t(resource){}
    auto insert_impl() -> void {}
    auto find_impl() -> void {}
};

TEST_CASE("base index") {

    actor_zeta::detail::pmr::memory_resource* resource = actor_zeta::detail::pmr::get_default_resource();
    auto index_engine = make_index_engine(resource);
    make_index<dummy>(index_engine,"test");

}