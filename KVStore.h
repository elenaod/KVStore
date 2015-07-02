#ifndef __KVSTORE_H_
#define __KVSTORE_H_

#include "data.h"
#include <cstdio>
#include <cstring>
#include <vector>
#include <fstream>

// add compression to the files where the data is stored
// boost file operations instead of those
// add CLI
// find on multiple namespaces
// add log
// apparently unsigned long long == unsigned long => change?
// or at least typedef it... "unsigned long long" is kinda long

struct StoreEntry {
  std::string fileName;
  std::string master;
  unsigned long long entries;

  StoreEntry(const char* name, const char* masterName,
             unsigned long long size);
};

class KVStore {
  public:
    KVStore(const char* _name, unsigned long _size);

    void add(const data& key, const data& value);
    data* find(const data& key);
    short remove(const data& key);

    void addNamespace(const char* nsName, int _size);
    KVStore* getNamespace(const char* nsName);

    short update(const data& key, const data& value);

    void printReadable();
    void rehash(unsigned long long newSize);

    ~KVStore();
  private:

    void writePair(const data& key, const data& value);
    // moves to the start of the record holding key (before next)
    void gotoKey (const data& key);

    // use streams, if possible, they'll go better with boost
    std::fstream dbOffsets, db;
    std::string name, master;
    unsigned long size;

    std::vector<StoreEntry> stores;
};

#endif
