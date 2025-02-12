#pragma once

#include <boost/intrusive/list_hook.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>
#include <components/document/document.hpp>
#include <memory_resource>

namespace services::collection::operators {

    class operator_data_t : public boost::intrusive_ref_counter<operator_data_t> {
        using document_ptr = components::document::document_ptr;

    public:
        using ptr = boost::intrusive_ptr<operator_data_t>;

        explicit operator_data_t(std::pmr::memory_resource* resource);

        ptr copy() const;

        std::size_t size() const;
        std::pmr::vector<document_ptr>& documents();
        std::pmr::memory_resource* resource();
        void append(document_ptr document);

    private:
        std::pmr::memory_resource* resource_;
        std::pmr::vector<document_ptr> documents_;
    };

    using operator_data_ptr = operator_data_t::ptr;

    inline operator_data_ptr make_operator_data(std::pmr::memory_resource* resource) {
        return {new operator_data_t(resource)};
    }

} // namespace services::collection::operators
