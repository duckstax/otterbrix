#pragma once

#include "../document.hpp"

using namespace components::new_document;

void gen_array(int num, const document_ptr& array);
void gen_dict(int num, const document_ptr& dict);
std::string gen_id(int num);
document_ptr gen_doc(int num, document_t::allocator_type* allocator);
