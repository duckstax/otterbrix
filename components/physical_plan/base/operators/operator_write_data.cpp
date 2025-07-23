#include "operator_write_data.hpp"

namespace components::base::operators {

    operator_write_data_t::ptr operator_write_data_t::copy() const {
        if (std::holds_alternative<std::pmr::vector<size_t>>(ids_)) {
            auto copy_data = make_operator_write_data<size_t>(resource_);
            auto& ids = std::get<std::pmr::vector<size_t>>(ids_);
            auto& copy_ids = std::get<std::pmr::vector<size_t>>(copy_data->ids_);
            copy_ids.reserve(ids.size());
            for (const auto& id : ids) {
                copy_ids.push_back(id);
            }
            return copy_data;
        } else {
            auto copy_data = make_operator_write_data<document::document_id_t>(resource_);
            auto& ids = std::get<std::pmr::vector<document::document_id_t>>(ids_);
            auto& copy_ids = std::get<std::pmr::vector<document::document_id_t>>(copy_data->ids_);
            copy_ids.reserve(ids.size());
            for (const auto& id : ids) {
                copy_ids.push_back(id);
            }
            return copy_data;
        }
    }

    std::size_t operator_write_data_t::size() const {
        if (std::holds_alternative<std::pmr::vector<size_t>>(ids_)) {
            return std::get<std::pmr::vector<size_t>>(ids_).size();
        } else {
            return std::get<std::pmr::vector<document::document_id_t>>(ids_).size();
        }
    }

    operator_write_data_t::ids_t& operator_write_data_t::ids() { return ids_; }

    // TODO return move
    void operator_write_data_t::append(document::document_id_t id) {
        std::get<std::pmr::vector<document::document_id_t>>(ids_).emplace_back(std::move(id));
    }

    void operator_write_data_t::append(size_t id) { std::get<std::pmr::vector<size_t>>(ids_).emplace_back(id); }

} // namespace components::base::operators
