#ifndef __DATA_H_
#define __DATA_H_

#include <cstdio>
#include <string>

enum dataType {
  INT = 1,
  STRING,
  DOUBLE,
  DELETED
};

struct data {
  void *value;
  size_t size;
  dataType type;

  data();
  data(const data& d);
  data(const int& num);
  data(const char* str);
  data(const std::string& str);
  data(const double& fp);
  data(const float& fp);

  char* asString() const;
  int asInt() const;
  double asDouble() const;

  bool operator==(const data& rhs);

  unsigned long long write(FILE *file,
                           unsigned long long offset = 0) const;
  unsigned long long read(FILE *file, unsigned long long offset = 0);
  void print() const;
  ~data();
};

unsigned long hash(const data& val);

#endif
