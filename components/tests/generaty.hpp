#pragma once

#include <components/document/document.hpp>
#include <components/document/mutable/mutable_array.h>
#include <components/document/mutable/mutable_dict.h>

using namespace components::document;

document_ptr gen_number_doc(int num);

document::retained_t<document::impl::array_t> gen_array(int num);
document::retained_t<document::impl::dict_t> gen_dict(int num);
std::string gen_id(int num);
document_ptr gen_doc(int num);
