#pragma once
#include <map>
#include <memory>
#include <scoped_allocator>
#include <utility>

#include <components/document/document.hpp>
#include <components/document/document_view.hpp>
#include <components/log/log.hpp>
#include <components/parser/conditional_expression.hpp>
#include <components/session/session.hpp>

#include <actor-zeta/detail/pmr/memory_resource.hpp>
#include <actor-zeta/detail/pmr/polymorphic_allocator.hpp>

namespace components::index {

    using document_ptr = components::document::document_ptr;

    class query_t {
    public:
        std::string operator[](std::size_t index){
            return data_[index];
        }

        std::vector<std::string> data_;
    private:

    };

    class result_set_t {
    public:
        void append(document_ptr doc) {
            data_.emplace_back(doc);
        }
    private:
        std::vector<document_ptr> data_;
    };

    class index_t {
    public:
        using key_t = std::string;///::document::impl::value_t;
        using value_t = components::document::document_ptr;
        virtual ~index_t();
        auto insert(key_t key, value_t);
        auto find(query_t query) -> result_set_t;

    protected:
        explicit index_t(actor_zeta::detail::pmr::memory_resource* resource);
        virtual auto insert_impl(key_t key, value_t) -> void = 0;
        virtual auto find_impl(query_t) -> result_set_t = 0;
        actor_zeta::detail::pmr::memory_resource* resource_;
    };

    using index_raw_ptr = index_t*;
    using index_ptr = std::unique_ptr<index_t>;
    using keys_base_t = std::vector<std::string>;
    using id_index = uint32_t;

    struct index_engine_t final {
    public:
        using value_t = index_ptr;

        explicit index_engine_t(actor_zeta::detail::pmr::memory_resource* resource);
        auto find(const keys_base_t&) -> index_raw_ptr;
        auto emplace(keys_base_t , value_t ) -> uint32_t ;
        [[nodiscard]] auto size() const -> std::size_t;
        actor_zeta::detail::pmr::memory_resource* resource() noexcept;

    private:
        template<class Target>
        using base_alocator = std::scoped_allocator_adaptor<actor_zeta::detail::pmr::polymorphic_allocator<Target>>;
        using comparator_t = std::less<keys_base_t>;

        struct wrapper final {
            wrapper(keys_base_t keys_base,value_t index)
                : keys_base_(std::move(keys_base)),index_(std::move(index)){}
            value_t index_;
            const keys_base_t keys_base_;
        };

        using base_storgae = std::list<wrapper,base_alocator<wrapper>>;
        using allocator_t = base_alocator<std::pair<const keys_base_t, base_storgae::iterator>>;
        using allocator_1_t = base_alocator<std::pair<const id_index , base_storgae::iterator>>;

        using keys_to_doc_t = std::map<keys_base_t, base_storgae::iterator , comparator_t, allocator_t>;
        using index_to_doc_t = std::map<id_index, base_storgae::iterator, std::less<>, allocator_1_t>;

        actor_zeta::detail::pmr::memory_resource* resource_;
        keys_to_doc_t mapper_;
        index_to_doc_t index_to_mapper_;
        base_storgae storgae_;

    };

    class deleter final {
    public:
        explicit deleter(actor_zeta::detail::pmr::memory_resource* ptr);

        template<class T>
        void operator()(T* target) {
            target->~T();
            ptr_->deallocate(target, sizeof(T));
        }

    private:
        actor_zeta::detail::pmr::memory_resource* ptr_;
    };

    using index_engine_ptr = std::unique_ptr<index_engine_t, deleter>;

    auto make_index_engine(actor_zeta::detail::pmr::memory_resource* resource) -> index_engine_ptr;
    auto search_index(const index_engine_ptr& ptr, keys_base_t keys) -> index_t*;

    template<class Target, class... Args>
    auto make_index(index_engine_ptr& ptr, keys_base_t&& keys, Args&&... args) -> uint32_t {
        return ptr->emplace(std::move(keys), std::make_unique<Target>(ptr->resource(), std::forward<Args>(args)...));
    }

    void insert(const index_engine_ptr& index, const keys_base_t& params, std::vector<document_ptr>);
    void insert(const index_engine_ptr& index, uint32_t id, std::vector<document_ptr>);
    auto find(const index_engine_ptr& index, keys_base_t params, query_t query);

} // namespace components::index