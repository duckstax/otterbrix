#include "node.hpp"

#include <algorithm>
#include <unordered_map>
#include <unordered_set>
#include <utility>

#include <boost/container_hash/hash.hpp>

using namespace std::string_literals; // NOLINT

namespace components::logical_plan {

    node_t::node_t(node_type node_type)
        : type_(node_type) {}

    node_t::~node_t() {
        assert(outputs_.empty());

        if (inputs_[0]) {
            inputs_[0]->remove_output_pointer(*this);
        }

        if (inputs_[1]) {
            inputs_[1]->remove_output_pointer(*this);
        }
    }

    size_t node_t::hash() const {
        size_t hash{0};

        visit_lqp(shared_from_this(), [&hash](const auto& node) {
            if (node) {
                for (const auto& expression : node->node_expressions) {
                    boost::hash_combine(hash, expression->hash());
                }
                boost::hash_combine(hash, node->type);
                boost::hash_combine(hash, node->_on_shallow_hash());
                return visitation::visit_inputs;
            }

            return visitation::do_not_visit_inputs;
        });

        return hash;
    }

    size_t node_t::on_shallow_hash_impl() const {
        return 0;
    }

    boost::intrusive_ptr<node_t> node_t::left_input() const {
        return inputs_[0];
    }

    boost::intrusive_ptr<node_t> node_t::right_input() const {
        return inputs_[1];
    }

    boost::intrusive_ptr<node_t> node_t::input(input_side side) const {
        const auto input_index = static_cast<int>(side);
        return inputs_[input_index];
    }

    void node_t::set_left_input(const boost::intrusive_ptr<node_t>& left) {
        set_input(input_side::left, left);
    }

    void node_t::set_right_input(const boost::intrusive_ptr<node_t>& right) {
        assert(right == nullptr || type_ == node_type::join_t || type_ == node_type::union_t ||
               type_ == node_type::update_t || type_ == node_type::intersect_t);
        set_input(input_side::right, right);
    }

    void node_t::set_input(input_side side, const boost::intrusive_ptr<node_t>& input) {
        assert(side == input_side::left || input == nullptr || type_ == node_type::join_t ||
               type_ == node_type::union_t || type_ == node_type::update_t || type_ == node_type::intersect_t);

        auto& current_input = inputs_[static_cast<int>(side)];

        if (current_input == input) {
            return;
        }

        // Untie from previous input
        if (current_input) {
            current_input->remove_output_pointer(*this);
        }

        current_input = input;
        if (current_input) {
            current_input->add_output_pointer(shared_from_this());
        }
    }

    size_t node_t::input_count() const {
        return inputs_.size() - std::count(inputs_.cbegin(), inputs_.cend(), nullptr);
    }

    input_side node_t::get_input_side(const boost::intrusive_ptr<node_t>& output) const {
        if (output->inputs_[0].get() == this) {
            return input_side::left;
        }

        if (output->inputs_[1].get() == this) {
            return input_side::right;
        }

        throw std::logic_error("");
    }

    std::vector<input_side> node_t::get_input_sides() const {
        std::vector<input_side> input_sides;
        input_sides.reserve(outputs_.size());

        for (const auto& output_weak_ptr : outputs_) {
            const auto output = output_weak_ptr.lock();
            assert(output);
            input_sides.emplace_back(get_input_side(output));
        }

        return input_sides;
    }

    std::vector<boost::intrusive_ptr<node_t>> node_t::outputs() const {
        std::vector<boost::intrusive_ptr<node_t>> outputs;
        outputs.reserve(outputs_.size());

        for (const auto& output_weak_ptr : outputs_) {
            const auto output = output_weak_ptr.lock();
            assert(output);
            outputs.emplace_back(output);
        }

        return outputs;
    }


    void node_t::remove_output(const boost::intrusive_ptr<node_t>& output) { // NOLINT
        const auto input_side = get_input_side(output);
        output->set_input(input_side, nullptr);
    }

    void node_t::clear_outputs() {
        while (!outputs_.empty()) {
            auto output = outputs_.front().lock();
            assert(output);
            remove_output(output);
        }
    }

    std::vector<output_relation> node_t::output_relations() const {
        std::vector<output_relation> output_relations(output_count());

        const auto outputs = this->outputs();
        const auto input_sides = get_input_sides();

        const auto output_relation_count = output_relations.size();
        for (auto output_idx = size_t{0}; output_idx < output_relation_count; ++output_idx) {
            output_relations[output_idx] = output_relation{outputs[output_idx], input_sides[output_idx]};
        }

        return output_relations;
    }

    size_t node_t::output_count() const {
        return outputs_.size();
    }

    boost::intrusive_ptr<node_t> node_t::deep_copy(node_mapping input_node_mapping) const {
        return deep_copy_impl(input_node_mapping);
    }

    bool node_t::shallow_equals(const node_t& rhs, const node_mapping& node_mapping) const {
        if (type_ != rhs.type_) {
            return false;
        }
        return on_shallow_equals_impl(rhs, node_mapping);
    }

    bool node_t::operator==(const node_t& rhs) const {
        if (this == &rhs) {
            return true;
        }
        return !lqp_find_subplan_mismatch(shared_from_this(), rhs.shared_from_this());
    }

    bool node_t::operator!=(const node_t& rhs) const {
        return !operator==(rhs);
    }

    boost::intrusive_ptr<node_t> node_t::deep_copy_impl(node_mapping& node_mapping) const {
        auto copied_left_input = boost::intrusive_ptr<node_t>{};
        auto copied_right_input = boost::intrusive_ptr<node_t>{};

        if (left_input()) {
            copied_left_input = left_input()->deep_copy_impl(node_mapping);
        }

        if (right_input()) {
            copied_right_input = right_input()->deep_copy_impl(node_mapping);
        }

        auto copy = shallow_copy_impl(node_mapping);
        copy->set_left_input(copied_left_input);
        copy->set_right_input(copied_right_input);

        return copy;
    }

    boost::intrusive_ptr<node_t> node_t::shallow_copy_impl(node_mapping& node_mapping) const {
        const auto node_mapping_iter = node_mapping.find(shared_from_this());

        // Handle diamond shapes in the LQP; don't copy nodes twice
        if (node_mapping_iter != node_mapping.end()) {
            return node_mapping_iter->second;
        }

        auto shallow_copy = on_shallow_copy_impl(node_mapping);
        shallow_copy->comment = comment;
        node_mapping.emplace(shared_from_this(), shallow_copy);

        return shallow_copy;
    }

    void node_t::remove_output_pointer(const node_t& output) {
        const auto iter = std::find_if(outputs_.begin(), outputs_.end(), [&](const auto& other) {
            return &output == other.lock().get() || other.expired();
        });
        assert(iter != outputs_.end());

        outputs_.erase(iter);
    }

    void node_t::add_output_pointer(const boost::intrusive_ptr<node_t>& output) {
        outputs_.emplace_back(output);
    }

} // namespace components::logical_plan
