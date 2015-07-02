#include "KVStore.h"
#include <boost/filesystem.hpp>

using namespace boost::filesystem;

#define __ENTRY_SIZE sizeof(unsigned long long)
#define __OFFSET_SUFFIX "off"

void createAndOpen(std::fstream& file, std::string name){
  if (!exists(name.c_str())){
    file.open(name.c_str(), std::fstream::out | 
                            std::fstream::binary);
    file.close();
  }
  file.open(name.c_str(), std::fstream::in | std::fstream::out |
                          std::fstream::binary);
}

StoreEntry::StoreEntry(const char* name, const char* masterName,
                       unsigned long long size) :
  fileName(name), master(masterName) {
  entries = size;
}

KVStore::KVStore(const char* _name, unsigned long _size) :
  name(_name) {
  size = _size;

  std::string ofName = name + __OFFSET_SUFFIX;
  createAndOpen(dbOffsets, ofName);
  if (file_size(ofName) == 0){
    unsigned long long x = 0;
    for (unsigned long long i = 0; i < size; ++i)
      dbOffsets.write((char*)&x, sizeof(unsigned long long));
  }

  createAndOpen(db, name);
}

/*
  returns 0 if the value was added
  otherwise returns an error code
  error if key already exists
*/
void KVStore::add(const data& key, const data& value) {
  unsigned long long end;
  db.seekp(0, std::fstream::end);
  end = db.tellp();
  unsigned long entryN = hash(key) % size;
  dbOffsets.seekg(__ENTRY_SIZE * entryN, std::fstream::beg);
  unsigned long long offset;
  dbOffsets.read((char*)&offset, sizeof(unsigned long long));
  if (offset == 0) {
    unsigned long long dbOffset = end + 1;
    dbOffsets.seekp(__ENTRY_SIZE * entryN, std::fstream::beg);
    dbOffsets.write((char*)&dbOffset, __ENTRY_SIZE);
    writePair(key, value);
    return;
  }
  else {
    unsigned long long next = 0;
    db.seekg(offset - 1, std::fstream::beg);
    do {
      offset = db.tellg();
      db.read((char*)&next, sizeof(unsigned long long));
      data thisKey;
      unsigned long long keySize = thisKey.read(db);
      if (thisKey == key) {
        return;
      }
      db.seekg(next, std::fstream::beg);
      if (db.tellg() > end) {
        exit(0);
      }
    } while (next != 0);
    db.seekp(offset, std::fstream::beg);
    unsigned long long nextOffset = end;
    db.write((char*)&end, sizeof(unsigned long long));
    db.seekp(0, std::fstream::end);
    writePair(key, value);
  }
}

  /*
    returns a pointer to *value* if *key* was found.
    if key was not found, returns 0
  */
data* KVStore::find(const data& key){
  unsigned long long entryN = hash(key) % size, offset = 0;

  dbOffsets.seekg(entryN * __ENTRY_SIZE, std::fstream::beg);
  dbOffsets.read((char*)&offset, __ENTRY_SIZE);

  if (offset == 0) {
    return 0;
  }

  --offset;
  unsigned long long next = 0;
  do {
    db.seekg(offset, std::fstream::beg);
    db.read((char*)&next, __ENTRY_SIZE);
    data thisKey;
    thisKey.read(db);
    if (thisKey == key){
      data *value = new data();
      value->read(db);
      return value;
    }
    offset = next;
  } while (next != 0);
  return 0;
}

short KVStore::remove(const data& key) {
  unsigned long entryN = hash(key) % size;
  // check at dbOffsets
  dbOffsets.seekg(entryN * __ENTRY_SIZE, std::fstream::beg);
  unsigned long long offset = 0;
  dbOffsets.read((char*)&offset, __ENTRY_SIZE);
  if (offset == 0) {
    return -1;
  }
  --offset;
  // getKey from db at offset
  unsigned long long next = 0;
  do {
    db.seekg(offset, std::fstream::beg);
    db.read((char*)&next, __ENTRY_SIZE);
    data thisKey;
    unsigned int keySize = 0;
    keySize = thisKey.read(db);
    if (thisKey == key){
      // mark as deleted
      db.seekp(offset + sizeof(next), std::fstream::beg);
      thisKey.type = DELETED;
      thisKey.write(db);
      return 0;
    }
    offset = next;
  }
  while (next != 0);
}

void KVStore::addNamespace(const char* nsName, int _size){
  KVStore *newNs = new KVStore(nsName, _size);
  StoreEntry se (nsName, name.c_str(), _size);
  stores.push_back(se);
  delete newNs;
}

KVStore* KVStore::getNamespace(const char* nsName){
  // do with iterators - too lazy to do now
  for(unsigned i = 0; i < stores.size(); ++i){
    if (stores[i].fileName == nsName){
      KVStore *ns =
        new KVStore(stores[i].fileName.c_str(),
                    stores[i].entries);
      return ns;
    }
  }
}

void KVStore::printReadable(){
  unsigned long long next = 0;
  db.seekg(0, std::fstream::beg);
  printf("Reference: sizeof(ull) = %lu, sizeof(size_t) = %lu\n",
          sizeof(unsigned long long), sizeof(size_t));
  while (db.read((char*)&next, sizeof(unsigned long long)) != 0){
    printf("%llu ", next);
    data d; d.read(db); d.print();
    d.read(db); d.print();
    printf("\n");
  }
  db.clear();
}

short KVStore::update(const data& key, const data& value){
  remove(key);
  add(key, value);
}

KVStore::~KVStore(){
  db.flush();
  dbOffsets.flush();
  db.close();
  dbOffsets.close();
}

void KVStore::rehash(unsigned long long newSize){
  size = newSize;
  std::fstream oldDb;
  printf("rehash()::opening old files\n");
  db.close(); dbOffsets.close();

  oldDb.open(name.c_str(), std::fstream::in | std::fstream::binary);

  createAndOpen(dbOffsets, "tempOff");
  if (file_size("tempOff") == 0){
    unsigned long long x = 0;
    for (unsigned long long i = 0; i < size; ++i)
      dbOffsets.write((char*)&x, sizeof(unsigned long long));
  }

  createAndOpen(db, "temp");
  // point db and dboffset to them
  // open db and dboffset in different pointers;
  unsigned long long next = 0;

  db.clear(); dbOffsets.clear();
  while (oldDb.read((char*)&next, __ENTRY_SIZE) != 0){
    data _key, _value;
    _key.read(oldDb);
    _value.read(oldDb);
    if (_key.type != DELETED) {
      add(_key, _value); // to temp && tempoff
    }
  }

  oldDb.clear();
  // rename temp and tempoff
  oldDb.close();
  remove(name.c_str()); remove((name + __OFFSET_SUFFIX).c_str());
  rename("temp", name.c_str());
  rename("tempOff", (name + __OFFSET_SUFFIX).c_str());
  db.close(); dbOffsets.close();
  if (exists(name.c_str()))
    db.open(name.c_str(), std::fstream::in | std::fstream::out |
                          std::fstream::binary);
  else {
    printf("Error: Failed to open file after re-hash\n");
    exit(0);
  }
  if (exists((name + __OFFSET_SUFFIX).c_str()))
    dbOffsets.open((name + __OFFSET_SUFFIX).c_str(),
         std::fstream::in | std::fstream::out | std::fstream::binary);
  else {
    printf("Error: Failed to open offsets after re-hash\n");
    exit(0);
  }
}

void KVStore::writePair(const data& key, const data& value) {
  unsigned long long next = 0;
  db.write((char*)&next, sizeof(unsigned long long));
  key.write(db); value.write(db);
}
