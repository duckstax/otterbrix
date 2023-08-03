#pragma once

#include <components/document/document.hpp>
#include <components/document/core/array.hpp>
#include <components/document/core/dict.hpp>

using namespace components::document;

document_ptr gen_number_doc(int64_t num);
document_ptr gen_special_doc(char type);

template <typename T>
void intToBytes(T number, uint8_t* buffer)
{
    for (int i = 0; i < sizeof(number); i++)
        buffer[sizeof(number) - 1 - i] = (number >> (i * 8));
}

document::retained_t<document::impl::array_t> gen_array(int num);
document::retained_t<document::impl::dict_t> gen_dict(int num);
std::string gen_id(int num);
document_ptr gen_doc(int num);
