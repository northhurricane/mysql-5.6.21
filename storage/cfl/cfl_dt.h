#ifndef _CFL_DT_H_
#define _CFL_DT_H_

#include <stdint.h>

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
#define SECONDE_BITS (6)
#define MICROSECOND_BITS (20)

#define YEAR_MASK (0xFFFF)
#define MONTH_MASK (0xF)
#define DAY_MASK (0x1F)
#define HUOR_MASK (0x1F)
#define MINUTE_MASK (0x3F)
#define SECOND_MASK (0x3F)
#define MICROSECOND_MASK (0xFFFFF)


inline int64_t cfl_t2i(const cfl_dt_t *v)
{
  int64_t temp = 0;

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

inline int cfl_i2t(int64_t v, cfl_dt_t *dt)
{
  int64_t temp = v;
  dt->microsecond = (int32_t)(temp & MICROSECOND_MASK);
  temp = temp >> MICROSECOND_BITS;
  dt->second = (int8_t)(temp & SECOND_MASK);
  temp = temp >> SECOND_BITS;
  dt->minute = (int8_t)(temp & MINUTE_MASK);
  temp = temp >> MINUTE_BITS;
  dt->hour = temp & HOUR_BITS;
  temp = temp >> HOUR_BITS;
  dt->day = (int8_t)(temp & DAY_MASK);
  temp = temp >> DAY_BITS;
  dt->month = (int8_t)(temp & MONTH_MASK);
  temp = temp >> MONTH_BITS;
  dt->year = (int16_t)(temp & YEAR_MASK);
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

inline boolean cfl_dt_valid(const cfl_dt_t *dt)
{
  if ((dt->year < 0) || (dt->month > 12) || (dt->day > 31)
      || (dt->hour > 24) || (dt->minute > 59) || (dt->second > 59)
      || (dt->microsecond > 999999))
    return false;

  return true;
}

#endif //_CFL_DT_H_
