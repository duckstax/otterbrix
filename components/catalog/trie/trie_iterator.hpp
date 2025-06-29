#include "trie_node.hpp"

class versioned_trie;
class trie_iterator;

class trie_iterator {
public:
    using iterator_category = std::forward_iterator_tag;
    using value_type = std::pair<std::string, const std::set<column_type_t>&>;
    using difference_type = std::ptrdiff_t;
    using pointer = value_type*;
    using reference = value_type&;

    trie_iterator(trie_node* node, bool is_end); // Убираем параметры по умолчанию
    trie_iterator(const trie_iterator& other);
    trie_iterator& operator=(const trie_iterator& other);
    ~trie_iterator();

    value_type operator*() const;
    trie_iterator& operator++();
    trie_iterator operator++(int);

    bool operator==(const trie_iterator& other) const;
    bool operator!=(const trie_iterator& other) const;

    void release_current_version();
    void acquire_current_version();

private:
    trie_node* current_node_;
    version_entry* current_version_;
    std::vector<std::pair<trie_node*, size_t>> stack_;
    std::string current_key_;
    bool is_end_;

    void find_next_valid_entry();
    void build_key_path(trie_node* node);
    void advance_to_next_node();

    friend class versioned_trie;
};
