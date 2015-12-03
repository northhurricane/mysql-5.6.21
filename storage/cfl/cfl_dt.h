#ifndef _CFL_DT_H_
#define _CFL_DT_H_

#include <stdint.h>
#include <time.h>
#include "cfl_endian.h"

/*
以公元纪年的日期。秒最高精度
*/
struct cfl_dt_struct
{
  int16_t  year;
  uint8_t  month;
  uint8_t  day;
  uint8_t  hour;
  uint8_t  minute;
  uint8_t  second;
  uint32_t microsecond;
};
typedef struct cfl_dt_struct cfl_dt_t;

typedef int64_t cfl_dti_t;

#define CFL_DTI_STORAGE_SIZE (8)

#define CFL_DTI_MIN (-1)
#define CFL_DTI_MAX (0x7FFFFFFFFFFFFFFF)

/*
64位整型时间日期结构
从高到低
16b |4b   |5b |5b  |6b    |6b    |20b
year|month|day|hour|minute|second|microsecond 
*/

#define YEAR_BITS (16)
#define MONTH_BITS (4)
#define DAY_BITS (5)
#define HOUR_BITS (5)
#define MINUTE_BITS (6)
#define SECOND_BITS (6)
#define MICROSECOND_BITS (20)

#define YEAR_MASK (0xFFFF)
#define MONTH_MASK (0xF)
#define DAY_MASK (0x1F)
#define HOUR_MASK (0x1F)
#define MINUTE_MASK (0x3F)
#define SECOND_MASK (0x3F)
#define MICROSECOND_MASK (0xFFFFF)

inline void
cfl_dti2s(cfl_dti_t dti, void *target)
{
  endian_write_uint64(target, dti);
}

inline cfl_dti_t
cfl_s2dti(void *source)
{
  return endian_read_uint64(source);
}

inline cfl_dti_t cfl_t2i(const cfl_dt_t *v)
{
  cfl_dti_t temp = 0;

  temp |= v->year;
  temp = temp << MONTH_BITS;
  temp |= v->month;
  temp = temp << DAY_BITS;
  temp |= v->day;
  temp = temp << HOUR_BITS;
  temp |= v->hour;
  temp = temp << MINUTE_BITS;
  temp |= v->minute;
  temp = temp << SECOND_BITS;
  temp |= v->second;
  temp = temp << MICROSECOND_BITS;
  temp |= v->microsecond;

  return temp;
}

inline int cfl_i2t(cfl_dti_t v, cfl_dt_t *dt)
{
  cfl_dti_t temp = v;
  dt->microsecond = (int32_t)(temp & MICROSECOND_MASK);
  temp = temp >> MICROSECOND_BITS;
  dt->second = (int8_t)(temp & SECOND_MASK);
  temp = temp >> SECOND_BITS;
  dt->minute = (int8_t)(temp & MINUTE_MASK);
  temp = temp >> MINUTE_BITS;
  dt->hour = (int8_t)(temp & HOUR_MASK);
  temp = temp >> HOUR_BITS;
  dt->day = (int8_t)(temp & DAY_MASK);
  temp = temp >> DAY_BITS;
  dt->month = (int8_t)(temp & MONTH_MASK);
  temp = temp >> MONTH_BITS;
  dt->year = (int16_t)(temp & YEAR_MASK);

  return 0;
}

inline int cfl_dti_cmp(int64_t dti1, int64_t dti2)
{
  if (dti1 == dti2)
  {
    return 0;
  }
  if (dti1 > dti2)
  {
    return 1;
  }
  return -1;
}

inline int cfl_dt_cmp(const cfl_dt_t *dt1, const cfl_dt_t *dt2)
{
  int64_t dti1 = cfl_t2i(dt1);
  int64_t dti2 = cfl_t2i(dt2);

  return cfl_dti_cmp(dti1, dti2);
}

inline bool cfl_dt_valid(const cfl_dt_t *dt)
{
  if ((dt->year < 0) || (dt->month > 12) || (dt->day > 31)
      || (dt->hour > 24) || (dt->minute > 59) || (dt->second > 59)
      || (dt->microsecond > 999999))
    return false;

  return true;
}

#define TM_BASE_YEAR (1900)
#define TM_BASE_MONTH (1)
#define TM_BASE_DAY (1)
inline void cfl_tm2cdt(cfl_dt_t &cdt, struct tm *dt)
{
  cdt.year = TM_BASE_YEAR + dt->tm_year;
  cdt.month = TM_BASE_MONTH + dt->tm_mon;
  cdt.day = TM_BASE_DAY + dt->tm_mday;
  cdt.hour = dt->tm_hour;
  cdt.minute = dt->tm_min;
  cdt.second = dt->tm_sec;
}

inline void cfl_tv2cdt(cfl_dt_t &cdt, const struct timeval &tv)
{
  time_t time;
  struct tm *tm;
  time = tv.tv_sec;
  tm = localtime(&time);
  cfl_tm2cdt(cdt, tm);
  cdt.microsecond = tv.tv_usec;
}

#endif //_CFL_DT_H_
