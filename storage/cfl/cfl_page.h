#ifndef _CFL_PAGE_H_
#define _CFL_PAGE_H_

#include <stdint.h>
/*
存储单元的映射
*/

#define CFL_PAGE_SIZE (1024 * 1024)

#define CFL_PAGE_FIX_SPACE_SIZE (0)

#define CFL_PAGE_INDEX_UNIT_SIZE (2)

#define CFL_PAGE_MAGIC_HEAD (OxAC81FD7321CA92FB)
#define CFL_PAGE_MAGIC_TAIL (0xC725ADFE6420CB63)

/**/
struct cfl_mpage_struct
{
  uint32_t page_no;
  void *mpage;
};

typedef struct cfl_page_struct cfl_page_t;

#endif //_CFL_PAGE_H_
