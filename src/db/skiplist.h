#pragma once

#include <atomic>
#include "util/arena.h"
#include "util/random.h"

namespace db {

template <typename Key, class Comparator>
class SkipList {
private:
  struct Node;

public:
	// Using "cmp" as Comparator, "Arena" as Memory pool
  SkipList(Comparator cmp, Arena* arena);
  SkipList(const SkipList&) = delete;
  SkipList& operator=(const SkipList&) = delete;

	// The method inserts key into SkipList
	// DO NOT STORE THE SAME KEY
	void Insert(const Key& key);

	// If there is a single key in the list, this method will return true
	void Contains(const Key& key) const;

	// Iterator for SkipList
	class Iterator {
		public:
			explicit Iterator(const SkipList* list);

			// Returns true if Iterator is on a valid node
			bool Valid() const;

			// Returns key
			// NEED Valid() before
			const Key& key() const;

			// Advance to next
			// NEED Valid() before
			void Next();

			// Advance to prev
			// NEED Valid() before
			void Prev();

			// Advance to the first entry with a key >= target
			void Seek(const Key& target);

			// Move to the first entry
			void SeekToFirst();

			// Move to the last entry
			void SeekToLast();

		private:
			const SkipList* list_;
			Node* node_;
	};

private:
	enum { kMaxHeight = 12 };

	inline int GetMaxHeight() const {
		return max_height_.load(std::memory_order_relaxed);
	}

	Node* NewNode(const Key & key,int height);
	int RandomHeight();
	bool Equal(const Key& a,const Key& b) const { return compare_(a,b) == 0; }

	// Return true if key is greater than n 
	bool KeyIsAfterNode(const Key& key,Node * n) const;

	// Return a node which is greater than or equal key
	// Return nullptr if there is no such node 
	// 
	// If prev if not a nullptr, fills prev[level] with prev ptr
	// node at [level] for every level in [0..max_height_-1] (storage from 0 to max_height -1 level 's prev ptr)
	Node* FindGreaterOrEqual(const Key&key, Node**prev) const;

	// Return the latest node with a key < key
	// Return head_ if there is no such node
	Node* FindLessThan(const Key& key) const;

	// Return the last node in the list
	// If empty, return head_
	Node* FindLast() const;

	// Comparator could NOT CHANGE after construction
	Comparator const compare_;

	// Use Arena to allocate nodes
	Arena* const arena_;

	Node* const head_;

	// Read/Written only by Insert()
	std::atomic<int> max_height_;

	// Read/Written only by Insert()
	Random rnd_;
};

}  // namespace db