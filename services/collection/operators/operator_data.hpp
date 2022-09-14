#pragma once

#include <boost/intrusive/list_hook.hpp>
#include <memory_resource>
#include <components/document/document.hpp>

namespace services::collection::operators {

    class operator_data_t : public boost::intrusive::list_base_hook<> {
        using document_ptr = components::document::document_ptr;

    public:
        explicit operator_data_t(std::pmr::memory_resource* resource);

        std::size_t size() const;
        std::pmr::vector<document_ptr>& documents();
        void append(document_ptr document);

    private:
        std::pmr::vector<document_ptr> documents_;
    };

    using operator_data_ptr = std::unique_ptr<operator_data_t>;

    inline operator_data_ptr make_operator_data(std::pmr::memory_resource* resource) {
        return std::make_unique<operator_data_t>(resource);
    }

}
