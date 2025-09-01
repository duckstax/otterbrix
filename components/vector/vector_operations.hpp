#pragma once

#include "vector.hpp"

namespace components::vector::vector_ops {

    namespace {

        template<typename T, typename COMP>
        int64_t select_const(vector_t& left,
                             vector_t& right,
                             int64_t count,
                             indexing_vector_t* true_sel,
                             indexing_vector_t* false_sel) {
            auto ldata = left.data<T>();
            auto rdata = right.data<T>();

            COMP comp{};
            if (left.is_null() || right.is_null() || !comp(*ldata, *rdata)) {
                if (false_sel) {
                    for (int64_t i = 0; i < count; i++) {
                        false_sel->set_index(i, incremental_indexing_vector(left.resource())->get_index(i));
                    }
                }
                return 0;
            } else {
                if (true_sel) {
                    for (int64_t i = 0; i < count; i++) {
                        true_sel->set_index(i, incremental_indexing_vector(left.resource())->get_index(i));
                    }
                }
                return count;
            }
        }

        template<typename T,
                 typename COMP,
                 bool LEFT_CONSTANT,
                 bool RIGHT_CONSTANT,
                 bool HAS_TRUE_SEL,
                 bool HAS_FALSE_SEL>
        int64_t selection_flat_loop(const T* ldata,
                                    const T* rdata,
                                    int64_t count,
                                    validity_mask_t& validity_mask,
                                    indexing_vector_t* true_sel,
                                    indexing_vector_t* false_sel) {
            int64_t true_count = 0, false_count = 0;
            int64_t base_idx = 0;
            auto entry_count = validity_data_t::entry_count(count);
            for (int64_t entry_idx = 0; entry_idx < entry_count; entry_idx++) {
                auto validity_entry = validity_mask.get_validity_entry(entry_idx);
                int64_t next = std::min<int64_t>(base_idx + validity_mask_t::BITS_PER_VALUE, count);
                if (validity_entry == validity_data_t::MAX_ENTRY) {
                    for (; base_idx < next; base_idx++) {
                        int64_t result_idx = incremental_indexing_vector(validity_mask.resource())->get_index(base_idx);
                        int64_t lidx = LEFT_CONSTANT ? 0 : base_idx;
                        int64_t ridx = RIGHT_CONSTANT ? 0 : base_idx;
                        COMP comp{};
                        bool comparison_result = comp(ldata[lidx], rdata[ridx]);
                        if (HAS_TRUE_SEL) {
                            true_sel->set_index(true_count, result_idx);
                            true_count += comparison_result;
                        }
                        if (HAS_FALSE_SEL) {
                            false_sel->set_index(false_count, result_idx);
                            false_count += !comparison_result;
                        }
                    }
                } else if (validity_entry == 0) {
                    if (HAS_FALSE_SEL) {
                        for (; base_idx < next; base_idx++) {
                            int64_t result_idx =
                                incremental_indexing_vector(validity_mask.resource())->get_index(base_idx);
                            false_sel->set_index(false_count, result_idx);
                            false_count++;
                        }
                    }
                    base_idx = next;
                } else {
                    int64_t start = base_idx;
                    for (; base_idx < next; base_idx++) {
                        int64_t result_idx = incremental_indexing_vector(validity_mask.resource())->get_index(base_idx);
                        int64_t lidx = LEFT_CONSTANT ? 0 : base_idx;
                        int64_t ridx = RIGHT_CONSTANT ? 0 : base_idx;
                        COMP comp{};
                        bool comparison_result = (validity_entry & uint64_t(1) << uint64_t(base_idx - start)) &&
                                                 comp(ldata[lidx], rdata[ridx]);
                        if (HAS_TRUE_SEL) {
                            true_sel->set_index(true_count, result_idx);
                            true_count += comparison_result;
                        }
                        if (HAS_FALSE_SEL) {
                            false_sel->set_index(false_count, result_idx);
                            false_count += !comparison_result;
                        }
                    }
                }
            }
            if (HAS_TRUE_SEL) {
                return true_count;
            } else {
                return count - false_count;
            }
        }

        template<typename T, typename COMP, bool LEFT_CONSTANT, bool RIGHT_CONSTANT>
        int64_t selection_flat_loop_switch(const T* ldata,
                                           const T* rdata,
                                           int64_t count,
                                           validity_mask_t& mask,
                                           indexing_vector_t* true_sel,
                                           indexing_vector_t* false_sel) {
            if (true_sel && false_sel) {
                return selection_flat_loop<T, COMP, LEFT_CONSTANT, RIGHT_CONSTANT, true, true>(ldata,
                                                                                               rdata,
                                                                                               count,
                                                                                               mask,
                                                                                               true_sel,
                                                                                               false_sel);
            } else if (true_sel) {
                return selection_flat_loop<T, COMP, LEFT_CONSTANT, RIGHT_CONSTANT, true, false>(ldata,
                                                                                                rdata,
                                                                                                count,
                                                                                                mask,
                                                                                                true_sel,
                                                                                                false_sel);
            } else {
                assert(false_sel);
                return selection_flat_loop<T, COMP, LEFT_CONSTANT, RIGHT_CONSTANT, false, true>(ldata,
                                                                                                rdata,
                                                                                                count,
                                                                                                mask,
                                                                                                true_sel,
                                                                                                false_sel);
            }
        }

        template<typename T, typename COMP, bool LEFT_CONSTANT, bool RIGHT_CONSTANT>
        int64_t select_flat(vector_t& left,
                            vector_t& right,
                            int64_t count,
                            indexing_vector_t* true_sel,
                            indexing_vector_t* false_sel) {
            auto ldata = left.data<T>();
            auto rdata = right.data<T>();

            if (LEFT_CONSTANT && left.is_null() || RIGHT_CONSTANT && right.is_null()) {
                if (false_sel) {
                    for (int64_t i = 0; i < count; i++) {
                        false_sel->set_index(i, incremental_indexing_vector(left.resource())->get_index(i));
                    }
                }
                return 0;
            }

            if constexpr (LEFT_CONSTANT) {
                return selection_flat_loop_switch<T, COMP, LEFT_CONSTANT, RIGHT_CONSTANT>(ldata,
                                                                                          rdata,
                                                                                          count,
                                                                                          right.validity(),
                                                                                          true_sel,
                                                                                          false_sel);
            } else if constexpr (RIGHT_CONSTANT) {
                return selection_flat_loop_switch<T, COMP, LEFT_CONSTANT, RIGHT_CONSTANT>(ldata,
                                                                                          rdata,
                                                                                          count,
                                                                                          left.validity(),
                                                                                          true_sel,
                                                                                          false_sel);
            } else {
                auto combined_mask = left.validity();
                combined_mask.combine(right.validity(), count);
                return selection_flat_loop_switch<T, COMP, LEFT_CONSTANT, RIGHT_CONSTANT>(ldata,
                                                                                          rdata,
                                                                                          count,
                                                                                          combined_mask,
                                                                                          true_sel,
                                                                                          false_sel);
            }
        }

        template<typename T, typename COMP, bool NO_NULL, bool HAS_TRUE_SEL, bool HAS_FALSE_SEL>
        int64_t select_generic_loop(const T* ldata,
                                    const T* rdata,
                                    const indexing_vector_t* lsel,
                                    const indexing_vector_t* rsel,
                                    int64_t count,
                                    validity_mask_t& lvalidity,
                                    validity_mask_t& rvalidity,
                                    indexing_vector_t* true_sel,
                                    indexing_vector_t* false_sel) {
            int64_t true_count = 0, false_count = 0;
            for (int64_t i = 0; i < count; i++) {
                auto result_idx = incremental_indexing_vector(lvalidity.resource())->get_index(i);
                auto lindex = lsel->get_index(i);
                auto rindex = rsel->get_index(i);
                COMP comp{};
                if ((NO_NULL || (lvalidity.row_is_valid(lindex) && rvalidity.row_is_valid(rindex))) &&
                    comp(ldata[lindex], rdata[rindex])) {
                    if (HAS_TRUE_SEL) {
                        true_sel->set_index(true_count++, result_idx);
                    }
                } else {
                    if (HAS_FALSE_SEL) {
                        false_sel->set_index(false_count++, result_idx);
                    }
                }
            }
            if constexpr (HAS_TRUE_SEL) {
                return true_count;
            } else {
                return count - false_count;
            }
        }

        template<typename T, typename COMP, bool NO_NULL>
        int64_t select_generic_loop_indexing_switch(const T* ldata,
                                                    const T* rdata,
                                                    const indexing_vector_t* lsel,
                                                    const indexing_vector_t* rsel,
                                                    int64_t count,
                                                    validity_mask_t& lvalidity,
                                                    validity_mask_t& rvalidity,
                                                    indexing_vector_t* true_sel,
                                                    indexing_vector_t* false_sel) {
            if (true_sel && false_sel) {
                return select_generic_loop<T, COMP, NO_NULL, true, true>(ldata,
                                                                         rdata,
                                                                         lsel,
                                                                         rsel,
                                                                         count,
                                                                         lvalidity,
                                                                         rvalidity,
                                                                         true_sel,
                                                                         false_sel);
            } else if (true_sel) {
                return select_generic_loop<T, COMP, NO_NULL, true, false>(ldata,
                                                                          rdata,
                                                                          lsel,
                                                                          rsel,
                                                                          count,
                                                                          lvalidity,
                                                                          rvalidity,
                                                                          true_sel,
                                                                          false_sel);
            } else {
                assert(false_sel);
                return select_generic_loop<T, COMP, NO_NULL, false, true>(ldata,
                                                                          rdata,
                                                                          lsel,
                                                                          rsel,
                                                                          count,
                                                                          lvalidity,
                                                                          rvalidity,
                                                                          true_sel,
                                                                          false_sel);
            }
        }

        template<typename T, typename COMP>
        int64_t select_generic_loop_switch(const T* ldata,
                                           const T* rdata,
                                           const indexing_vector_t* lsel,
                                           const indexing_vector_t* rsel,
                                           int64_t count,
                                           validity_mask_t& lvalidity,
                                           validity_mask_t& rvalidity,
                                           indexing_vector_t* true_sel,
                                           indexing_vector_t* false_sel) {
            if (!lvalidity.all_valid() || !rvalidity.all_valid()) {
                return select_generic_loop_indexing_switch<T, COMP, false>(ldata,
                                                                           rdata,
                                                                           lsel,
                                                                           rsel,
                                                                           count,
                                                                           lvalidity,
                                                                           rvalidity,
                                                                           true_sel,
                                                                           false_sel);
            } else {
                return select_generic_loop_indexing_switch<T, COMP, true>(ldata,
                                                                          rdata,
                                                                          lsel,
                                                                          rsel,
                                                                          count,
                                                                          lvalidity,
                                                                          rvalidity,
                                                                          true_sel,
                                                                          false_sel);
            }
        }

        template<typename T, typename COMP>
        int64_t select_generic(vector_t& left,
                               vector_t& right,
                               int64_t count,
                               indexing_vector_t* true_sel,
                               indexing_vector_t* false_sel) {
            unified_vector_format ldata(left.resource(), left.size());
            unified_vector_format rdata(right.resource(), right.size());

            left.to_unified_format(count, ldata);
            right.to_unified_format(count, rdata);

            return select_generic_loop_switch<T, COMP>(ldata.get_data<T>(),
                                                       rdata.get_data<T>(),
                                                       ldata.referenced_indexing,
                                                       rdata.referenced_indexing,
                                                       count,
                                                       ldata.validity,
                                                       rdata.validity,
                                                       true_sel,
                                                       false_sel);
        }

        template<typename T, typename COMP>
        int64_t select(vector_t& left,
                       vector_t& right,
                       int64_t count,
                       indexing_vector_t* true_sel,
                       indexing_vector_t* false_sel) {
            if (left.get_vector_type() == vector_type::CONSTANT && right.get_vector_type() == vector_type::CONSTANT) {
                return select_const<T, COMP>(left, right, count, true_sel, false_sel);
            } else if (left.get_vector_type() == vector_type::CONSTANT &&
                       right.get_vector_type() == vector_type::FLAT) {
                return select_flat<T, COMP, true, false>(left, right, count, true_sel, false_sel);
            } else if (left.get_vector_type() == vector_type::FLAT &&
                       right.get_vector_type() == vector_type::CONSTANT) {
                return select_flat<T, COMP, false, true>(left, right, count, true_sel, false_sel);
            } else if (left.get_vector_type() == vector_type::FLAT && right.get_vector_type() == vector_type::FLAT) {
                return select_flat<T, COMP, false, false>(left, right, count, true_sel, false_sel);
            } else {
                return select_generic<T, COMP>(left, right, count, true_sel, false_sel);
            }
        }
    } // namespace

    void generate_sequence(vector_t& result, uint64_t count, int64_t start, int64_t increment);
    void generate_sequence(vector_t& result,
                           uint64_t count,
                           const indexing_vector_t& indexing,
                           int64_t start,
                           int64_t increment);

    void copy(const vector_t& source,
              vector_t& target,
              uint64_t source_count,
              uint64_t source_offset,
              uint64_t target_offset);
    void copy(const vector_t& source,
              vector_t& target,
              const indexing_vector_t& indexing,
              uint64_t source_count,
              uint64_t source_offset,
              uint64_t target_offset);
    void copy(const vector_t& source,
              vector_t& target,
              const indexing_vector_t& indexing,
              uint64_t source_count,
              uint64_t source_offset,
              uint64_t target_offset,
              uint64_t copy_count);

    void hash(vector_t& input, vector_t& result, uint64_t count);
    void hash(vector_t& input, vector_t& result, const indexing_vector_t& indexing, uint64_t count);

    void combine_hash(vector_t& hashes, vector_t& input, uint64_t count);
    void combine_hash(vector_t& hashes, vector_t& input, const indexing_vector_t& rindexing, uint64_t count);
    void write_to_storage(vector_t& source, uint64_t count, std::byte* target);

    template<typename COMP>
    int64_t
    compare(vector_t& left, vector_t& right, int64_t count, indexing_vector_t* true_sel, indexing_vector_t* false_sel) {
        assert(left.type().to_physical_type() == right.type().to_physical_type());

        switch (left.type().to_physical_type()) {
            case types::physical_type::BOOL:
            case types::physical_type::INT8:
                return select<int8_t, COMP>(left, right, count, true_sel, false_sel);
            case types::physical_type::INT16:
                return select<int16_t, COMP>(left, right, count, true_sel, false_sel);
            case types::physical_type::INT32:
                return select<int32_t, COMP>(left, right, count, true_sel, false_sel);
            case types::physical_type::INT64:
                return select<int64_t, COMP>(left, right, count, true_sel, false_sel);
            case types::physical_type::UINT8:
                return select<uint8_t, COMP>(left, right, count, true_sel, false_sel);
            case types::physical_type::UINT16:
                return select<uint16_t, COMP>(left, right, count, true_sel, false_sel);
            case types::physical_type::UINT32:
                return select<uint32_t, COMP>(left, right, count, true_sel, false_sel);
            case types::physical_type::UINT64:
                return select<uint64_t, COMP>(left, right, count, true_sel, false_sel);
            // case types::physical_type::INT128:
            // 	   return select<int128_t, COMP>(left, right, count, true_sel, false_sel);
            // case types::physical_type::UINT128:
            // 	   return select<uint128_t, COMP>(left, right, count, true_sel, false_sel);
            case types::physical_type::FLOAT:
                return select<float, COMP>(left, right, count, true_sel, false_sel);
            case types::physical_type::DOUBLE:
                return select<double, COMP>(left, right, count, true_sel, false_sel);
            case types::physical_type::STRING:
                return select<std::string_view, COMP>(left, right, count, true_sel, false_sel);
            default:
                throw std::runtime_error("Invalid type for comparison");
        }
    }
} // namespace components::vector::vector_ops