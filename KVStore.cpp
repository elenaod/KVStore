#include "KVStore.h"

#define __ENTRY_SIZE sizeof(unsigned long long)
#define __OFFSET_SUFFIX "off"

StoreEntry::StoreEntry(const char* name, const char* masterName,
                       unsigned long long size) :
                     fileName(name), master(masterName){
  entries = size;
}

KVStore::KVStore(const char* _name, unsigned long _size) :
  name(_name) {
  size = _size;

  std::string ofName = name + __OFFSET_SUFFIX;
  dbOffsets = fopen(ofName.c_str(), "rb+");
  if (dbOffsets == 0) {
    dbOffsets = fopen(ofName.c_str(), "ab+");
    unsigned long long x = 0;
    for (unsigned long long i = 0; i < size; ++i)
      fwrite(&x, __ENTRY_SIZE, 1, dbOffsets);
    fclose(dbOffsets);
    dbOffsets = fopen(ofName.c_str(), "rb+");
  }
  db = fopen(name.c_str(), "rb+");

  if (db == 0){
    db = fopen(name.c_str(), "ab+");
    fclose(db);
    db = fopen(name.c_str(), "rb+");
  }
}

  /*
    returns 0 if the value was added
    otherwise returns an error code
    error if key already exists
  */
void KVStore::add(const data& key, const data& value) {
  printf("===========add()===========================\n");
  unsigned long long end;
  fseek(db, 0, SEEK_END);
  end = ftell(db);
  unsigned long entryN = hash(key) % size;
  printf("hash = %lu ", entryN); key.print();
  fseek(dbOffsets, __ENTRY_SIZE * entryN, SEEK_SET);
  unsigned long long offset;
  fread(&offset, __ENTRY_SIZE, 1, dbOffsets);
  printf("add():: offset is %llu\n", offset);
  if (offset == 0) {
    unsigned long long dbOffset = end + 1;
    printf("add():: writing offset = %llu at pos %llu\n",
           dbOffset, offset);
    fseek(dbOffsets, __ENTRY_SIZE * entryN, SEEK_SET);
    fwrite(&dbOffset, __ENTRY_SIZE, 1, dbOffsets);
    writePair(key, value);
    return;
  }
  else {
    unsigned long long next = 0;
    fseek(db, offset - 1, SEEK_SET);
    do {
      offset = ftell(db);
      printf("add()::reading from %llu\n", offset - 1);
      fread(&next, sizeof(unsigned long long), 1, db);
      printf("add()::next entry is @ %llu\n", next);
      data thisKey;
      unsigned long long keySize = thisKey.read(db);
      if (thisKey == key) {
        printf("Error: add detected duplicate key, try update\n");
        return;
      }
      printf("add()::skipping to %llu\n", next);
      fseek(db, next, SEEK_SET);
    }
    while (next != 0);
    printf("add()::last read was @ %llu\n", offset);
    fseek(db, offset, SEEK_SET);
    printf("add()::currently at %lu, end is @ %llu\n",
           ftell(db), end);
    unsigned long long nextOffset = end;
    fwrite(&end, sizeof(unsigned long long), 1, db);
    printf("add()::next = %llu\n", nextOffset);
    fseek(db, 0, SEEK_END);
    writePair(key, value);
  }
  printf("===========add()===========================\n");
}

  /*
    returns a pointer to *value* if *key* was found.
    if key was not found, returns 0
  */
data* KVStore::find(const data& key){
    unsigned long entryN = hash(key) % size;
    // check at dbOffsets
    fseek(dbOffsets, entryN * __ENTRY_SIZE, SEEK_SET);
    printf("hash is %lu, looking @ %lu\n",entryN, ftell(dbOffsets));
    unsigned long long offset = 0;
    fread(&offset, __ENTRY_SIZE, 1, dbOffsets);
    printf("offset is: %llu\n", offset);
    if (offset == 0) {
      return 0;
    }
    --offset;
    // getKey from db at offset
    unsigned long long next = 0;
    do {
      fseek(db, offset, SEEK_SET);
      printf("reading from %lu\n", ftell(db));
      fread(&next, __ENTRY_SIZE, 1, db);
      printf("next is: %llu\n", next);
      data thisKey;
      unsigned int keySize = 0;
      keySize = thisKey.read(db);
      printf("read key with Size = %u\n", keySize);
      if (thisKey == key){
        printf("find::KEY FOUND!\n");
        printf("thisKey.type = %d\n", thisKey.type);
        data *value = new data();
        value->read(db);
        return value;
      }
      printf("KEY IS DIFFERENT, CONTINUING\n");
      printf("NEXT == %llu\n", next);
      offset = next;
    }
    while (next != 0);
    return 0;
  }

short KVStore::remove(const data& key) {
    unsigned long entryN = hash(key) % size;
    // check at dbOffsets
    fseek(dbOffsets, entryN * __ENTRY_SIZE, SEEK_SET);
    unsigned long long offset = 0;
    fread(&offset, __ENTRY_SIZE, 1, dbOffsets);
    printf("remove::offset is: %llu\n", offset);
    if (offset == 0) {
      return -1;
    }
    --offset;
    // getKey from db at offset
    unsigned long long next = 0;
    do {
      fseek(db, offset, SEEK_SET);
      printf("remove::reading from %lu\n", ftell(db));
      fread(&next, __ENTRY_SIZE, 1, db);
      printf("remove::next is: %llu\n", next);
      data thisKey;
      unsigned int keySize = 0;
      keySize = thisKey.read(db);
      printf("remove::read key with Size = %u\n", keySize);
      if (thisKey == key){
        printf("KEY FOUND!!!\n");
        // mark as deleted
        fseek(db, offset + sizeof(next), SEEK_SET);
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

void KVStore::printReadable() const {
  unsigned long long next = 0;
  fseek(db, 0, SEEK_SET);
  printf("Reference: sizeof(ull) = %lu, sizeof(size_t) = %lu\n",
          sizeof(unsigned long long), sizeof(size_t));
  while (fread(&next, sizeof(unsigned long long), 1, db) != 0){
    printf("%llu ", next);
    data d; d.read(db); d.print();
    d.read(db); d.print();
    printf("\n");
  }
}

short KVStore::update(const data& key, const data& value){
  remove(key);
  add(key, value);
}

KVStore::~KVStore(){
  fflush(db);
  fflush(dbOffsets);
  fclose(db);
  fclose(dbOffsets);
}

void KVStore::rehash(unsigned long long newSize){
  size = newSize;
  FILE *oldDb = db, *oldDbOff = dbOffsets;
  db = fopen("dbs/temp", "ab+");
  dbOffsets = fopen("dbs/tempOff", "ab+");
  // point db and dboffset to them
  // open db and dboffset in different pointers;
  unsigned long long next = 0;

  while (fread(&next, __ENTRY_SIZE, 1, oldDb) != 0){
  data _key, _value;
  _key.read(oldDb);
  if (_key.size != 0){
    _value.read(oldDb);
    add(_key, _value); // to temp && tempoff
  }
  }

  // rename temp and tempoff
  fclose(oldDb); fclose(oldDbOff);
  remove(name.c_str()); remove(name + __OFFSET_SUFFIX);
  rename("dbs/temp", name.c_str());
  rename("dbs/tempOff", (name + __OFFSET_SUFFIX).c_str());
}

void KVStore::writePair(const data& key, const data& value) {
  printf("writePair::Current position is %ld\n", ftell(db));
  unsigned long long next = 0;
  fwrite(&next, sizeof(unsigned long long), 1, db);
  key.write(db); value.write(db);
}
