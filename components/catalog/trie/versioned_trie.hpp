#include "trie_iterator.hpp"
#include "trie_node.hpp"

class versioned_trie {
public:
    versioned_trie()
        : root_(std::make_unique<trie_node>())
        , current_version_(0) {}

    // Проверить существование ключа
    bool contains(const std::string& key) const;

    // Проверить валидность пути (для JSON pointer'ов)
    bool is_path_valid(const std::string& key) const;

    // Вставить или обновить значение
    void insert(const std::string& key, const std::set<column_type_t>& types);

    // Найти значение по ключу
    trie_iterator find(const std::string& key);

    // Удалить ключ
    bool erase(const std::string& key);

    // Итераторы
    trie_iterator begin();
    trie_iterator end();

    // Получить текущую версию
    uint64_t get_current_version() const { return current_version_; }

    // Проверить, пустое ли дерево
    bool is_empty() const;

    // Очистить мертвые версии во всем дереве
    void cleanup();

private:
    std::unique_ptr<trie_node> root_;
    uint64_t current_version_;

    // Найти узел по ключу
    trie_node* find_node(const std::string& key) const;

    // Найти общий префикс двух строк
    size_t common_prefix_length(const std::string& a, const std::string& b) const;

    // Разделить узел в позиции split_pos
    void split_node(trie_node* node, size_t split_pos);

    // Удалить пустые узлы вверх по дереву
    void cleanup_empty_nodes(trie_node* node);

    friend class trie_iterator;
};
