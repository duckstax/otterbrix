#pragma once

#include <boost/intrusive/list_hook.hpp>
#include <memory_resource>
#include <components/document/document_id.hpp>

namespace services::collection::operators {

    class operator_write_data_t : public boost::intrusive::list_base_hook<> {
        using document_id_t = components::document::document_id_t;

    public:
        using ptr = std::unique_ptr<operator_write_data_t>;

        explicit operator_write_data_t(std::pmr::memory_resource* resource);

        ptr copy() const;

        std::size_t size() const;
        std::pmr::vector<document_id_t>& documents();
        void append(document_id_t document);

    private:
        std::pmr::memory_resource* resource_;
        std::pmr::vector<document_id_t> documents_;
    };

    using operator_write_data_ptr = operator_write_data_t::ptr;

    inline operator_write_data_ptr make_operator_write_data(std::pmr::memory_resource* resource) {
        return std::make_unique<operator_write_data_t>(resource);
    }

}
