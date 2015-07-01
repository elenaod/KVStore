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
    default: {printf("Error: type not recognised\n"); exit(1);}
  }
}

unsigned long long data::write(FILE *file,
                               unsigned long long offset) const{
  fwrite(&size, sizeof(size), 1, file);
  fwrite(&type, sizeof(type), 1, file);
  if (value == 0){
    char nullchar = 0;
    for(unsigned long long i = 0; i < size; ++i)
      fwrite(&nullchar, 1, 1, file);
  }
  else {
    fwrite(value, size, 1, file);
  }
  return sizeof(size) + sizeof(type) + size;
}

unsigned long long data::read(FILE *file, unsigned long long offset){
  fread(&size, sizeof(size), 1, file);
  fread(&type, sizeof(type), 1, file);
  if (value != 0) free(value);
  value = malloc(size);
  fread(value, size, 1, file);
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

