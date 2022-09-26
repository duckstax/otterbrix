#pragma once

#include <array>
#include <unordered_map>
#include <vector>

#include <boost/smart_ptr/intrusive_ptr.hpp>
#include <boost/smart_ptr/intrusive_ref_counter.hpp>

#include "forward.hpp"

#include <core/make_intrusive_ptr.hpp>

namespace components::logical_plan {

    class node_t;

    struct output_relation final {
        boost::intrusive_ptr<node_t> output;
        input_side input_side = input_side::left;
    };

    using node_mapping = std::unordered_map<boost::intrusive_ptr<const node_t>, boost::intrusive_ptr<node_t>>;

    class node_t : public boost::intrusive_ref_counter<node_t> {
    public:
        node_t(const node_type node_type);
        virtual ~node_t();

        boost::intrusive_ptr<node_t> left_input() const;
        boost::intrusive_ptr<node_t> right_input() const;
        boost::intrusive_ptr<node_t> input(input_side side) const;
        void set_left_input(const boost::intrusive_ptr<node_t>& left);
        void set_right_input(const boost::intrusive_ptr<node_t>& right);
        void set_input(input_side side, const boost::intrusive_ptr<node_t>& input);

        size_t input_count() const;
        input_side get_input_side(const boost::intrusive_ptr<node_t>& output) const;
        std::vector<input_side> get_input_sides() const;
        std::vector<boost::intrusive_ptr<node_t>> outputs() const;
        void remove_output(const boost::intrusive_ptr<node_t>& output);
        void clear_outputs();
        std::vector<output_relation> output_relations() const;
        size_t output_count() const;
        boost::intrusive_ptr<node_t> deep_copy(node_mapping input_node_mapping = {}) const;
        bool shallow_equals(const node_t& rhs, const node_mapping& node_mapping) const;

        bool operator==(const node_t& rhs) const;
        bool operator!=(const node_t& rhs) const;

        size_t hash() const;

    protected:
        virtual size_t on_shallow_hash_impl() const;
        virtual boost::intrusive_ptr<node_t> on_shallow_copy_impl(node_mapping& node_mapping) const = 0;
        virtual bool on_shallow_equals_impl(const node_t& rhs, const node_mapping& node_mapping) const = 0;

    private:
        const node_type type_;

        boost::intrusive_ptr<node_t> deep_copy_impl(node_mapping& node_mapping) const;
        boost::intrusive_ptr<node_t> shallow_copy_impl(node_mapping& node_mapping) const;

        void add_output_pointer(const boost::intrusive_ptr<node_t>& output);
        void remove_output_pointer(const node_t& output);

        std::vector<boost::intrusive_ptr<node_t>> outputs_;
        std::array<boost::intrusive_ptr<node_t>, 2> inputs_;
    };

    using node_ptr = boost::intrusive_ptr<node_t>;

    struct node_hash final {
        size_t operator()(const boost::intrusive_ptr<node_t>& node) const {
            return node->hash();
        }
    };

    struct node_equal final {
        size_t operator()(const boost::intrusive_ptr<node_t>& lhs, const boost::intrusive_ptr<node_t>& rhs) const {
            return lhs == rhs || *lhs == *rhs;
        }
    };

    template<typename Value>
    using unordered_map = std::unordered_map<boost::intrusive_ptr<node_t>, Value, node_hash, node_equal>;

} // namespace components::logical_plan
