#include "primer/trie.h"
#include <iostream>
#include <stack>
#include <string_view>
#include "common/exception.h"

namespace bustub {

template <class T>
auto Trie::Get(std::string_view key) const -> const T * {
  std::shared_ptr<const TrieNode> node = root_;
  for (auto ch : key) {
    if (node == nullptr || !node->children_.count(ch)) {
      return nullptr;
    }
    node = node->children_.find(ch) != node->children_.end() ? node->children_.at(ch) : nullptr;
  }
  if (!node || !node->is_value_node_) {
    return nullptr;
  }
  auto *leaf = dynamic_cast<const TrieNodeWithValue<T> *>(node.get());
  return leaf ? leaf->value_.get() : nullptr;
  // You should walk through the trie to find the node corresponding to the key. If the node doesn't exist, return
  // nullptr. After you find the node, you should use `dynamic_cast` to cast it to `const TrieNodeWithValue<T> *`. If
  // dynamic_cast returns `nullptr`, it means the type of the value is mismatched, and you should return nullptr.
  // Otherwise, return the value.
}

template <class T>
auto Trie::Put(std::string_view key, T value) const -> Trie {
  // Note that `T` might be a non-copyable type. Always use `std::move` when creating `shared_ptr` on that value.
  // You should walk through the trie and create new nodes if necessary. If the node corresponding to the key already
  // exists, you should create a new `TrieNodeWithValue`.
  std::stack<std::shared_ptr<TrieNode>> node_stack;
  auto node = root_ != nullptr ? std::shared_ptr<TrieNode>(root_->Clone()) : std::make_shared<TrieNode>();
  for (auto ch : key) {
    node_stack.push(node);
    if (node == nullptr || !node->children_.count(ch)) {
      node = std::make_shared<TrieNode>();
    } else {
      node = node->children_.at(ch)->Clone();
    }
  }

  node = std::make_shared<TrieNodeWithValue<T>>(node->children_, std::make_shared<T>(std::move(value)));
  for (auto it = key.rbegin(); it != key.rend(); ++it) {
    node_stack.top()->children_[*it] = node;
    node = node_stack.top();
    node_stack.pop();
  }
  return Trie(node);
}

auto Trie::Remove(std::string_view key) const -> Trie {
  // You should walk through the trie and remove nodes if necessary. If the node doesn't contain a value any more,
  // you should convert it to `TrieNode`. If a node doesn't have children any more, you should remove it.
  std::vector<std::shared_ptr<const TrieNode>> node_stack;
  std::shared_ptr<const TrieNode> cur_node = root_;
  for (auto ch : key) {
    node_stack.push_back(cur_node);
    if (cur_node == nullptr || cur_node->children_.count(ch) == 0) {
      return Trie(root_);
    }
    cur_node = cur_node->children_.at(ch);
  }
  if (!cur_node->is_value_node_) {
    return Trie(root_);
  }
  std::shared_ptr<const TrieNode> end_node = std::make_shared<TrieNode>(cur_node->children_);
  std::shared_ptr<const TrieNode> child_node = end_node;
  cur_node = end_node;
  for (size_t i = node_stack.size() - 1; i < node_stack.size(); --i) {
    cur_node = std::shared_ptr<const TrieNode>(node_stack[i]->Clone());
    const_cast<TrieNode *>(cur_node.get())->children_[key[i]] = child_node;
    if (cur_node->children_.at(key[i])->children_.empty() && !cur_node->children_.at(key[i])->is_value_node_) {
      const_cast<TrieNode *>(cur_node.get())->children_.erase(key[i]);
    }

    child_node = cur_node;
  }
  if (cur_node->children_.empty()) {
    return Trie(nullptr);
  }
  return Trie(cur_node);
}

// Below are explicit instantiation of template functions.
//
// Generally people would write the implementation of template classes and functions in the header file. However, we
// separate the implementation into a .cpp file to make things clearer. In order to make the compiler know the
// implementation of the template functions, we need to explicitly instantiate them here, so that they can be picked up
// by the linker.

template auto Trie::Put(std::string_view key, uint32_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint32_t *;

template auto Trie::Put(std::string_view key, uint64_t value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const uint64_t *;

template auto Trie::Put(std::string_view key, std::string value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const std::string *;

// If your solution cannot compile for non-copy tests, you can remove the below lines to get partial score.

using Integer = std::unique_ptr<uint32_t>;

template auto Trie::Put(std::string_view key, Integer value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const Integer *;

template auto Trie::Put(std::string_view key, MoveBlocked value) const -> Trie;
template auto Trie::Get(std::string_view key) const -> const MoveBlocked *;

}  // namespace bustub
