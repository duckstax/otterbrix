#pragma once

#include "nodes/nodes.h"
#include "nodes/pg_type_definitions.h"
#include <list>
#include <memory>

struct PGListCell {
    void* data;
};

struct ListNode {
    NodeTag type = T_List;
};

struct PGList : ListNode {
    std::list<PGListCell> lst{};
};

using List = PGList;
using ListCell = PGListCell;

extern std::unique_ptr<List> NIL_;

struct Nil_t {
    operator List*() const { return NIL_.get(); }
};
inline Nil_t NIL;

static inline PGListCell* list_head(const PGList* l) { return const_cast<PGListCell*>(l ? &*l->lst.begin() : nullptr); }

static inline int list_length(const PGList* l) { return l->lst.size(); }

#define lfirst(lc) ((lc)->data)
#define linitial(l) lfirst(list_head(l))
static inline void* lsecond(const PGList* l) { return (++l->lst.begin())->data; }
static inline void* lthird(const PGList* l) { return (++(++l->lst.begin()))->data; }
static inline void* llast(const PGList* l) { return l->lst.back().data; }

List* lcons(void* datum, List* list);

#define list_make1(x1) lcons(x1, NIL)
#define list_make2(x1, x2) lcons(x1, list_make1(x2))
#define list_make3(x1, x2, x3) lcons(x1, list_make2(x2, x3))
#define list_make4(x1, x2, x3, x4) lcons(x1, list_make3(x2, x3, x4))

#define foreach(cell, l)                                                                                               \
    auto _it_ = l->lst.begin();                                                                                        \
    for ((cell) = list_head(l); _it_ != l->lst.end(); ++_it_, (cell) = &*_it_)

#define forboth(cell1, l1, cell2, l2)                                                                                  \
    auto _it_l1_ = l1->lst.begin();                                                                                    \
    auto _it_l2_ = l2->lst.begin();                                                                                    \
    for ((cell1) = list_head(l1), (cell2) = list_head(l2); _it_l1_ != l1->lst.end() && _it_l2_ != l2->lst.end();       \
         ++_it_l1_, ++_it_l2_, (cell1) = &*_it_l1_, (cell2) = &*_it_l2_)

PGList* lappend(PGList* list, void* datum);
PGList* list_concat(PGList* list1, PGList* list2);
PGList* list_truncate(PGList* list, int new_size);
Node* list_nth(const PGList* list, int n); // was void*, but all uses get Node*
bool list_member(const PGList* list, const void* datum);
PGList* lcons(void* datum, List* list);
PGList* list_copy_tail(const PGList* list, int nskip);