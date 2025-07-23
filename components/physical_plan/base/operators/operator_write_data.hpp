#pragma once

#include <boost/intrusive/list_hook.hpp>
#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>
#include <components/document/document_id.hpp>
#include <memory_resource>
#include <variant>

namespace components::base::operators {

    class operator_write_data_t;

    class operator_write_data_t
        : public boost::intrusive_ref_counter<operator_write_data_t>
        , public boost::intrusive::list_base_hook<> {
        using ids_t = std::variant<std::pmr::vector<document::document_id_t>, std::pmr::vector<size_t>>;

    public:
        using ptr = boost::intrusive_ptr<operator_write_data_t>;

        template<typename T>
        explicit operator_write_data_t(std::pmr::memory_resource* resource, T)
            : resource_(resource)
            , ids_(std::pmr::vector<T>(resource)) {}

        ptr copy() const;

        std::size_t size() const;
        ids_t& ids();
        void append(document::document_id_t id);
        void append(size_t id);

    private:
        std::pmr::memory_resource* resource_;
        ids_t ids_;
    };

    using operator_write_data_ptr = operator_write_data_t::ptr;

    template<typename T>
    operator_write_data_ptr make_operator_write_data(std::pmr::memory_resource* resource) {
        // force template deduction
        return {new operator_write_data_t(resource, T())};
    }

} // namespace components::base::operators
