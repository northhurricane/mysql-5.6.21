#ifndef _LENDIAN_H
#define _LENDIAN_H

/*
封装mysql的存储和
*/
#include <stdint.h>
#include <string.h>
#include <my_global.h>
#include <my_byteorder.h>

inline uint8_t endian_read_uint8(const void *source)
{
  return *((uint8_t*)source);
}

inline void endian_write_uint8(void *target, uint8_t value)
{
  *((uint8_t*)target) = value;
}

inline uint16_t endian_read_uint16(const void *source)
{
  uint16_t value;
  ushortget(value, source);
  return value;
}

inline void endian_write_uint16(void *target, uint16_t value)
{
  shortstore(target, value);
}

inline uint32_t endian_read_uint32(const void *source)
{
  uint32_t value;
  longget(value, source);
  return value;
}

inline void endian_write_uint32(void *target, uint32_t value)
{
  longstore(target, value);
}

inline uint64_t endian_read_uint64(const void *source)
{
  uint64_t value;
  longlongget(value, source);
  return value;
}

inline void endian_write_uint64(void *target, uint64_t value)
{
  longlongstore(target, value);
}

inline float endian_read_float(const void *source)
{
  float value;
  floatget(value, source);
  return value;
}

inline void endian_write_float(void *target, float value)
{
  floatstore(target, value);
}

inline double endian_read_double(const void *source)
{
  double value;
  doubleget(value, source);
  return value;
}

inline void endian_write_double(void *target, double value)
{
  doublestore(target, value);
}

#endif //_LENDIAN_H
