extern "C" {

struct BTree;

struct BTree* btree_create(void);
void btree_insert(struct BTree* tree, int key, int value);
int btree_find(struct BTree* tree, int key, int* value);
void btree_delete(struct BTree* tree);

}

#include <unordered_map>
using namespace std;

struct BTree {
    unordered_map<int, int> inner;
};

struct BTree* btree_create(void) {
    return new BTree;
}

void btree_delete(struct BTree* tree) {
    delete tree;
}

void btree_insert(struct BTree* tree, int key, int value) {
    tree->inner.insert(key, value);
}

int btree_find(struct BTree* tree, int key, int* value) {
    auto it = tree->inner.find(key);
    if (it == tree->inner.end()) {
        return -1;
    } else {
        *value = it->second;
        return 0;
    }
}