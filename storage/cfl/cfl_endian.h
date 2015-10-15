#ifndef _LENDIAN_H
#define _LENDIAN_H

#include <stdint.h>

inline uint8_t endian_read_uint8(void *source)
{
  return *((uint8_t*)source);
}

inline void endian_write_uint8(void *target, uint8_t value)
{
  *((uint8_t*)target) = value;
}

inline uint16_t endian_read_uint16(void *source)
{
  return *((uint16_t*)source);
}

inline void endian_write_uint16(void *target, uint16_t value)
{
  *((uint16_t*)target) = value;
}

inline uint32_t endian_read_uint32(void *source)
{
  return *((uint32_t*)source);
}

inline void endian_write_uint32(void *target, uint32_t value)
{
  *((uint32_t*)target) = value;
}

inline uint64_t endian_read_uint64(void *source)
{
  return *((uint64_t*)source);
}

inline void endian_write_uint64(void *target, uint64_t value)
{
  *((uint64_t*)target) = value;
}

#endif //_LENDIAN_H
