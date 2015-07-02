#include "data.h"

#include <string>
#include <cstring>

data::data() {
  value = 0; size = 0;
}

data::data(const data& d) {
  value = malloc(d.size);
  memcpy(value, d.value, d.size); size = d.size; type = d.type;
}

data::data(const int& num){
  size = sizeof(num); type = INT;
  value = malloc(size);
  memcpy(value, &num, size);
}

data::data(const char* str){
  size = sizeof(char) * strlen(str) + 1;
  type = STRING;
  value = malloc(size);
  strcpy((char*)value, str);
}

data::data(const std::string& str){
  size = sizeof(char) * str.length() + 1;
  type = STRING;
  value = malloc(size);
  strcpy((char*)value, str.c_str());
}

data::data(const double& fp){
  size = sizeof(double);
  type = DOUBLE;
  value = malloc(size);
  memcpy(value, &fp, size);
}

data::data(const float& fp){
  size = sizeof(float);
  type = DOUBLE;
  value = malloc(size);
  memcpy(value, &fp, size);
}

char* data::asString() const {
  return (char*) value;
}

int data::asInt() const{
  return *(int*) value;
}

double data::asDouble() const{
  return *(double*) value;
}
bool data::operator==(const data& rhs){
  return type == rhs.type &&
         size == rhs.size && memcmp(value, rhs.value, size) == 0;
}

void data::print() const{
  printf("(%lu, ", size);
  switch (type) {
    case INT: {printf("INT, %d)", *(int*)value); break;}
    case STRING: {printf("STRING, '%s')", (char*)value); break;}
    case DOUBLE: {printf("DOUBLE, %lf)", *(double*)value); break;}
    case DELETED: {printf("DELETED)"); break;}
    default: {printf("%d\n", type);}
  }
}

unsigned long long data::write(std::fstream& file) const{
  file.write((char*)&size, sizeof(size));
  file.write((char*)&type, sizeof(type));
  if (value == 0){
    char nullchar = 0;
    for(unsigned long long i = 0; i < size; ++i)
      file.write((char*)&nullchar, 1);
  }
  else {
    file.write((char*)value, size);
  }
  return sizeof(size) + sizeof(type) + size;
}

unsigned long long data::read(std::fstream& file){
  file.read((char*)&size, sizeof(size));
  file.read((char*)&type, sizeof(type));
  if (value != 0) free(value);
  value = malloc(size);
  file.read((char*)value, size);
  return sizeof(size) + sizeof(type) + size;
}

data::~data() {
  if (value != 0)
    free(value);
}

unsigned long hash(const data& val) {
  char *str;
  switch(val.type){
    case INT: {
      std::string s = std::to_string(val.asInt());
      str = (char*)malloc(sizeof(char) * strlen(s.c_str()) + 1);
      memcpy(str, s.c_str(), sizeof(char) * s.length());
      str[strlen(s.c_str())] = '\0';
      break;
    }
    case STRING: {str = val.asString();}
  }
  unsigned long hash = 5381;
  int c;

  while (c = *str++)
    hash = ((hash << 5) + hash) + c; /* hash * 33 + c */

  return hash;
}

