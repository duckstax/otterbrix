#pragma once

#include <components/vector/arrow/appender/append_data.hpp>
#include <components/vector/arrow/appender/scalar_data.hpp>
#include <components/vector/arrow/arrow_string_view.hpp>
#include <components/vector/vector.hpp>

namespace components::vector::arrow::appender {

    template<class SRC = std::string_view, class BUFTYPE = int64_t>
    struct arrow_string_data_t {
        static void initialize(arrow_append_data_t& result, const types::complex_logical_type& type, size_t capacity) {
            result.main_buffer().reserve((capacity + 1) * sizeof(BUFTYPE));
            result.auxiliary_buffer().reserve(capacity);
        }

        template<bool LARGE_STRING>
        static void
        AppendTemplated(arrow_append_data_t& append_data, vector_t& input, size_t from, size_t to, size_t input_size) {
            size_t size = to - from;
            unified_vector_format format(input.resource(), input_size);
            input.to_unified_format(input_size, format);
            auto& main_buffer = append_data.main_buffer();
            auto& validity_buffer = append_data.validity_buffer();
            auto& aux_buffer = append_data.auxiliary_buffer();

            validity_buffer.resize_validity(append_data.row_count + size);
            auto validity_data = (uint8_t*) validity_buffer.data();

            main_buffer.resize(main_buffer.size() + sizeof(BUFTYPE) * (size + 1));
            auto data = format.get_data<SRC>();
            auto offset_data = main_buffer.data<BUFTYPE>();
            if (append_data.row_count == 0) {
                offset_data[0] = 0;
            }
            auto last_offset = offset_data[append_data.row_count];
            for (size_t i = from; i < to; i++) {
                auto source_idx = format.referenced_indexing->get_index(i);
                auto offset_idx = append_data.row_count + i + 1 - from;

                if (!format.validity.row_is_valid(source_idx)) {
                    uint8_t current_bit;
                    size_t current_byte;
                    bit_position(append_data.row_count + i - from, current_byte, current_bit);
                    append_data.set_null(validity_data, current_byte, current_bit);
                    offset_data[offset_idx] = last_offset;
                    continue;
                }

                auto string_length = data[source_idx].size();

                auto current_offset = static_cast<size_t>(last_offset) + string_length;
                offset_data[offset_idx] = static_cast<BUFTYPE>(current_offset);

                aux_buffer.resize(current_offset);
                std::memcpy(aux_buffer.data() + last_offset, data[source_idx].data(), data[source_idx].size());

                last_offset = static_cast<BUFTYPE>(current_offset);
            }
            append_data.row_count += size;
        }

        static void
        append(arrow_append_data_t& append_data, vector_t& input, size_t from, size_t to, size_t input_size) {
            AppendTemplated<false>(append_data, input, from, to, input_size);
        }

        static void
        finalize(arrow_append_data_t& append_data, const types::complex_logical_type& type, ArrowArray* result) {
            result->n_buffers = 3;
            result->buffers[1] = append_data.main_buffer().data();
            result->buffers[2] = append_data.auxiliary_buffer().data();
        }
    };

    struct ArrowVarcharToStringViewData {
        static void initialize(arrow_append_data_t& result, const types::complex_logical_type& type, size_t capacity) {
            result.main_buffer().reserve((capacity) * sizeof(arrow_string_view_t));
            result.auxiliary_buffer().reserve(capacity);
            result.buffer_size_buffer().reserve(sizeof(int64_t));
        }

        static void
        append(arrow_append_data_t& append_data, vector_t& input, size_t from, size_t to, size_t input_size) {
            size_t size = to - from;
            unified_vector_format format(input.resource(), input_size);
            input.to_unified_format(input_size, format);
            auto& main_buffer = append_data.main_buffer();
            auto& validity_buffer = append_data.validity_buffer();
            auto& aux_buffer = append_data.auxiliary_buffer();
            validity_buffer.resize_validity(append_data.row_count + size);
            auto validity_data = (uint8_t*) validity_buffer.data();

            main_buffer.resize(main_buffer.size() + sizeof(arrow_string_view_t) * (size));
            auto data = format.get_data<std::string_view>();
            for (size_t i = from; i < to; i++) {
                auto result_idx = append_data.row_count + i - from;
                auto arrow_data = main_buffer.data<arrow_string_view_t>();
                auto source_idx = format.referenced_indexing->get_index(i);
                if (!format.validity.row_is_valid(source_idx)) {
                    // Null value
                    uint8_t current_bit;
                    size_t current_byte;
                    bit_position(result_idx, current_byte, current_bit);
                    append_data.set_null(validity_data, current_byte, current_bit);
                    arrow_data[result_idx] = arrow_string_view_t(0, "");
                    continue;
                }
                // These two are now the same buffer
                size_t string_length = data[source_idx].size();
                auto string_data = data[source_idx].data();
                if (string_length <= arrow_string_view_t::MAX_INLINED_BYTES) {
                    arrow_data[result_idx] = arrow_string_view_t(static_cast<int32_t>(string_length), string_data);
                } else {
                    arrow_data[result_idx] = arrow_string_view_t(static_cast<int32_t>(string_length),
                                                                 string_data,
                                                                 0,
                                                                 static_cast<int32_t>(append_data.offset));
                    auto current_offset = append_data.offset + string_length;
                    aux_buffer.resize(current_offset);
                    std::memcpy(aux_buffer.data() + append_data.offset,
                                data[source_idx].data(),
                                data[source_idx].size());
                    append_data.offset = current_offset;
                }
            }
            append_data.row_count += size;
        }

        static void
        finalize(arrow_append_data_t& append_data, const types::complex_logical_type& type, ArrowArray* result) {
            result->n_buffers = 4;
            result->buffers[1] = append_data.main_buffer().data();
            result->buffers[2] = append_data.auxiliary_buffer().data();
            reinterpret_cast<int64_t*>(append_data.buffer_size_buffer().data())[0] =
                static_cast<int64_t>(append_data.offset);
            result->buffers[3] = append_data.buffer_size_buffer().data();
        }
    };

} // namespace components::vector::arrow::appender