#pragma once

#include <algorithm>
#include <list>
#include <memory>
#include <sstream>
#include <type_traits>


namespace ASTree {

    /*  AST-дерево представляет собой структурное
    представление исходной программы, очищенное от элементов конкретного
    синтаксиса (в рассматриваемом примере в AST-дерево не попал «разделитель»,
    т.к. он не имеет отношения непосредственно к семантике данного фрагмента
    программы, а лишь к конкретному синтаксису языка). В качестве узлов в AST-
    дереве выступают операторы, к которым присоединяются их аргументы,
    которые в свою очередь также могут быть составными узлами. Часто узлы
    AST-дерева получаются из лексем, выделенных на этапе лексического анализа,
    однако могут встречаться и узлы, которым ни одна лексема не соответствует
*/

    static int GlobalIdForASTNode = 0;

    template<typename NodeType, typename = typename std::enable_if<std::is_enum<NodeType>::value>::type>
    class ASTNode : public std::enable_shared_from_this<ASTNode<NodeType>> {
    public:
        using WeakPtr = std::weak_ptr<ASTNode<NodeType>>;
        using SharedPtr = std::shared_ptr<ASTNode<NodeType>>;

    private:
        explicit ASTNode() noexcept
            : ASTNode(static_cast<NodeType>(0), std::string()) {}

        explicit ASTNode(NodeType type, std::string text) noexcept
            : type_(std::move(type))
            , text_(std::move(text))
            , parent_(SharedPtr()) {
            ///DEBUGAST(std::string("ASTNode(): '") << toString())
            GlobalIdForASTNode++;
        }

        // следующая структура необходима для того, чтобы сработал std::make_shared с приватным
        // конструктором ASTNode()
        template<typename NodeType_>
        struct MakeSharedEnabler : public ASTNode<NodeType_> {
            explicit MakeSharedEnabler(NodeType_ type, std::string text)
                : ASTNode(std::move(type), std::move(text)) {}
        };

    public:
        ASTNode(const ASTNode&) = delete;

        ASTNode(ASTNode&& rVal) noexcept = default;

        ~ASTNode() {
            ///DEBUGAST(std::string("~ASTNode(): '") << toString())
        }

        static SharedPtr GetNewInstance(NodeType type, std::string text, const SharedPtr& child1, const SharedPtr& child2) {
            const SharedPtr instance(std::make_shared<MakeSharedEnabler<NodeType>>(std::move(type), std::move(text)));
            if (child1) {
                instance->addChild(child1);
            }
            if (child2) {
                instance->addChild(child2);
            }
            return instance;
        }

        static SharedPtr GetNewInstance(NodeType type, const SharedPtr& child1, const SharedPtr& child2) {
            return GetNewInstance(std::move(type), std::to_string(type), child1, child2);
        }

        static SharedPtr GetNewInstance(NodeType type, std::string text, const SharedPtr& child1) {
            return GetNewInstance(std::move(type), std::move(text), child1, SharedPtr());
        }

        static SharedPtr GetNewInstance(NodeType type, const SharedPtr& child1) {
            return GetNewInstance(std::move(type), std::to_string(type), child1);
        }

        static SharedPtr GetNewInstance(NodeType type, std::string text) {
            return GetNewInstance(std::move(type), std::move(text), SharedPtr(), SharedPtr());
        }

        static SharedPtr GetNewInstance(NodeType type) {
            return GetNewInstance(std::move(type), std::to_string(type));
        }

        // Добавить дочерний узел к текущему узлу
        void addChild(const SharedPtr child) {
            /// DEBUGAST("adding child: " << child->toString() << " to root: " << toString())

            if (child->isParentValid()) {
                ///DEBUGAST("Parent valid in child: " << child->toString())
                child->getParent().lock()->removeChild(child);
            }

            ////DEBUGAST("Count of childs in " << toString() << " " << childs_.size())
            auto self = this->shared_from_this(); //shared_from_this() we cannot use here,
                                                  //because this object has not been initialized
                                                  //yet and there isn't any shared_ptr that point to it
            child->setParent(self);

            childs_.remove(child); //если уже есть child в списке, удалить его
            childs_.emplace_back(std::move(child));
        }

        // Удалить дочерний узел из текущего узла (child - узел, который должен быть удален)
        void removeChild(const SharedPtr& child) {
            childs_.remove(child);

            // если текущий узел является отцом у дочернего, нужно это убрать, сделав отцом пустой SharedPtr
            if (child->getParent().lock() == this->shared_from_this()) {
                child->setParent(SharedPtr());
            }
        }

        // Получить к-во дочерних узлов у текущего узла
        size_t getChildsCount() const { return childs_.size(); }

        // Проверить, установлен ли отцовский узел
        bool isParentValid() const {
            //return (! parent_.owner_before(WeakPtr{})) && (! WeakPtr{}.owner_before(parent_));
            return parent_.lock() != nullptr;
        }

        // Получить WeakPtr на отцовский узел
        WeakPtr getParent() { return parent_; }

        // Сделать отцом текущего узла "parent" узел
        void setParent(const WeakPtr& parent) { parent_ = parent; }

        // Сделаться отцовским узлом у узла val
        void setAsParent(const SharedPtr& val) {
            auto self = this->shared_from_this();
            val->addChild(self);
        }

        // Полусить дочерний узел по его индексу
        SharedPtr getChild(long index) const {
            return *(std::next(childs_.begin(), index));
        }

        // Получить индекс дочернего узла по самому узлу.
        // Возвращается пара, где левое значение -- нашелся ли такой узел среди дочерних, правое -- индекс, если нашелся
        std::pair<bool, long> getChildIndex(const SharedPtr& child) const {
            // Find given element in list
            const auto& it = std::find(childs_.begin(), childs_.end(), child);

            if (it == childs_.end()) {
                return std::pair(false, -1);
            }

            return std::pair(true, std::distance(childs_.begin(), it));
        }

        // Получить индекс этого узла у его родительского узла, если тот устаовлен.
        // Возвращается пара, где левое значение -- нашелся ли такой узел среди дочерних у родительского, правое -- индекс, если нашелся
        std::pair<bool, long> getChildIndexInParent() {
            if (!isParentValid()) {
                return std::pair(false, -1);
            }

            const auto self = this->shared_from_this();
            return parent_.lock()->getChildIndex(self);
        }

        // Получить все дочернии узлы текущего
        std::list<SharedPtr> getChilds() const { return childs_; }

        // Получить уникальное имя узла
        std::string getUniqueName() const { return uniqName_; }

        // Получить тип узла
        NodeType getType() const { return type_; }

        // Получить присвоеный узлу текст
        std::string getText() const { return text_; }

        // Вывести информацию о текущем узле
        std::string toString() const { return text_ + " (type: " + std::to_string(type_) + ")"; }

    private:
        // уникальное имя узла
        std::string uniqName_ = "node_" + std::to_string(GlobalIdForASTNode);

        // тип узла
        NodeType type_;

        // текст, связанный с узлом
        std::string text_;

        // родительский узел для данного узла дерева
        WeakPtr parent_;

        // потомки (ветви) данного узла дерева
        std::list<SharedPtr> childs_;
    };

    template<typename NodeType, typename = typename std::enable_if<std::is_enum<NodeType>::value>::type>
    class ASTNodeWalker {
    public:
        explicit ASTNodeWalker(std::shared_ptr<ASTNode<NodeType>> head) noexcept
            : head_(std::move(head)) {}

        // сгенерирует файл для утилиты Graphviz (https://www.graphviz.org/)
        // которая сможет отобразить дерево
        void buildDotFormat() {
           /// std::stringstream stream;
            ///stream << "graph {" << std::endl;
            //stream << "\tnode[fontsize=10,margin=0,width=\".4\", height=\".3\"];" << std::endl;
            //stream << "\tsubgraph cluster17{" << std::endl;

            ASTNodeWalker::GetStringSubTree(head_, stream);

            stream << "\t}\n}" << std::endl;
            dotFormat_ = stream.str();
        }

        std::string getDotFormat() const {
            return dotFormat_;
        }

    private:
        static void GetStringSubTree(const std::shared_ptr<ASTNode<NodeType>>& node, std::stringstream& stream) {
            if (!node->getParent().lock()) {
                stream << "\t\tn" << node->getUniqueName() << "[label=\"" << node->getText() << "\"];" << std::endl;
            } else {
                stream << "\t\tn" << node->getUniqueName() << "[label=\"" << node->getText() << "\"];" << std::endl;
                stream << "\t\tn" << node->getParent().lock()->getUniqueName() << "--n" << node->getUniqueName() << ";" << std::endl;
            }

            for (int i = 0; i < node->getChildsCount(); i++) {
                GetStringSubTree(node->getChild(i), stream);
            }
        }

    private:
        const std::shared_ptr<ASTNode<NodeType>> head_;
        std::string dotFormat_;
    };

} // namespace ASTree
