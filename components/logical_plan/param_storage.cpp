#include "param_storage.hpp"

#include <components/serialization/serializer.hpp>

#include <core/pmr.hpp>

namespace components::logical_plan {

    const expr_value_t& get_parameter(const storage_parameters* storage, core::parameter_id_t id) {
        auto it = storage->parameters.find(id);
        if (it != storage->parameters.end()) {
            return it->second;
        }
        // TODO compile error
        return expr_value_t{};
    }

    auto parameter_node_t::parameters() const -> const storage_parameters& { return values_; }

    storage_parameters parameter_node_t::take_parameters() { return std::move(values_); }

    auto parameter_node_t::set_parameters(const storage_parameters& parameters) -> void { values_ = parameters; }

    auto parameter_node_t::next_id() -> core::parameter_id_t {
        auto tmp = counter_;
        counter_ += 1;
        return core::parameter_id_t(tmp);
    }

    auto parameter_node_t::parameter(core::parameter_id_t id) const -> const expr_value_t& {
        return get_parameter(&values_, id);
    }

    void parameter_node_t::serialize(serializer::base_serializer_t* serializer) const {
        serializer->start_array(values_.parameters.size() + 1);
        serializer->append("type", std::string("parameter_node_t"));
        for (const auto& [key, value] : values_.parameters) {
            serializer->start_array(2);
            serializer->append("key", key);
            serializer->append("value", value);
            serializer->end_array();
        }
        serializer->end_array();
    }

    parameter_node_ptr make_parameter_node(std::pmr::memory_resource* resource) {
        return {new parameter_node_t(resource)};
    }
} // namespace components::logical_plan
