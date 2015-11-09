#ifndef _CFL_PAGE_H_
#define _CFL_PAGE_H_

#include <stdint.h>
#include <list>
#include <string.h>
#include "cfl_endian.h"
#include "cfl_dt.h"

using namespace std;
/*
页存储单元
每页设计为1M大小
*/
/*
页面格式
page magic head
page header
rows data
rows index
page magic tail
*/
/*
页头格式
row count:页面内记录数量
*/

#define CFL_PAGE_SIZE (1024 * 1024)

#define CFL_PAGE_HEAD_SIZE (16)
#define CFL_PAGE_HEAD (0)
#define CFL_PAGE_ROW_COUNT (CFL_PAGE_HEAD)

#define CFL_PAGE_ROW_POS_SIZE (3)

#define CFL_PAGE_MAGIC_HEAD (OxAC81FD7321CA92FB)
#define CFL_PAGE_MAGIC_TAIL (0xC725ADFE6420CB63)

#define CFL_PAGE_MAGIC_HEAD_SIZE (8)
#define CFL_PAGE_MAGIC_TAIL_SIZE (8)

#define CFL_PAGE_FIX_SPACE_SIZE (CFL_PAGE_MAGIC_HEAD_SIZE + \
                                 CFL_PAGE_MAGIC_TAIL_SIZE + CFL_PAGE_HEAD_SIZE)

inline void
cfl_page_write_row_count(void *page, uint32_t row_count)
{
  uint32_t offset = CFL_PAGE_MAGIC_HEAD_SIZE + CFL_PAGE_ROW_COUNT;
  uint8_t *pos = (uint8_t*)page;
  pos += offset;
  endian_write_uint32(pos, row_count);
}

inline uint32_t
cfl_page_read_row_count(void *page)
{
  uint32_t offset = CFL_PAGE_MAGIC_HEAD_SIZE + CFL_PAGE_ROW_COUNT;
  uint8_t *pos = (uint8_t*)page;
  pos += offset;
  return endian_read_uint32(pos);
}

inline void
cfl_page_write_row_offset(uint8_t *addr, uint32_t v)
{
  addr[0] = (uint8_t)(v & 0xFF);
  addr[1] = (uint8_t)((v & 0xFF00) >> 8);
  addr[2] = (uint8_t)((v & 0xFF0000) >> 8);
}

inline uint32_t
cfl_page_read_row_offset(uint8_t *addr)
{
  uint32_t byte1, byte2, byte3 = 0;
  byte1 = addr[0];
  byte2 = addr[1] << 8;
  byte3 = addr[2] << 16;

  uint32_t temp = byte1 | byte2 | byte3;
  return temp;
}

/*
  读取第nth个row offset
  nth:0-base
*/
inline uint8_t*
cfl_page_nth_row_offset(void *page, uint32_t nth)
{
  uint8_t *page_tail = (uint8_t*)page + CFL_PAGE_SIZE;
  uint8_t *nth_off = page_tail - CFL_PAGE_MAGIC_TAIL_SIZE
                               - (nth + 1) * CFL_PAGE_ROW_POS_SIZE;
  return nth_off;
}

class CflStorage;
/*
  已排序的数据写入maker
*/
class CflPageMaker
{
public :
  static CflPageMaker *Create();
  static int Destroy(CflPageMaker *maker);

  void AddRow(void *row, uint16_t row_size);
  void set_page_index(cfl_dti_t dti) { page_index_ = dti; }
  cfl_dti_t page_index() { return page_index_; }
  void Flush(CflStorage *storage);

private :
  uint32_t rows_counter_;
  uint8_t *buffer_;
  uint32_t row_pos_; //行在页面内的偏移
  uint8_t *row_offset_; //写入行偏移
  uint8_t *pos_offset_; //位置偏移
  cfl_dti_t page_index_;

  void Reset()
  {
    rows_counter_ = 0;
    row_pos_ = CFL_PAGE_MAGIC_HEAD_SIZE + CFL_PAGE_HEAD_SIZE;
    row_offset_ = buffer_ + CFL_PAGE_MAGIC_HEAD_SIZE + CFL_PAGE_HEAD_SIZE;
    pos_offset_ = (buffer_ + CFL_PAGE_SIZE) - CFL_PAGE_MAGIC_TAIL_SIZE
                                            - CFL_PAGE_ROW_POS_SIZE;
  }

  CflPageMaker() {
    buffer_ = NULL;
    Reset();
  }

  ~CflPageMaker() {
    if (buffer_ != NULL)
      free(buffer_);
  }

};

class CflPagePool;

/*
  内存中的页对象
*/
class CflPage
{
private :
  friend class CflPagePool;
  void *page_; //页缓冲区
  uint8_t pool_id_;

public :
  uint8_t pool_id() { return pool_id_; }
  void *page() { return page_; }
};

#define CflPageMaxPool (255)
/*
  内存页的管理对象
*/
class CflPageManager
{
public :
  /*初始化和销毁*/
  static int Initialize(int pools_number, int pool_size);
  static int Deinitialize();

  /*从存储中获取页*/
  static CflPage* GetPage(CflStorage *storage, uint32_t nth_page);
  /*将数据页归还*/
  static int PutPage(CflPage *page);

private :
  //Manager下管理的pool
  static CflPagePool *pools_[CflPageMaxPool];
  static uint8_t pools_number_;
  static bool initialized_;
  static uint8_t curr_pool_;
};

class CflPagePool
{
private :
  friend class CflPageManager;

  static CflPagePool* Create(int size, uint8_t id);
  static int Destroy(CflPagePool *pool);

  CflPage *GetPage(CflStorage *storage, uint32_t nth_page);
  int PutPage(CflPage *page);

  uint8_t id_; //每个pool都有自己的id，这个id在程序上和CflPageManager的pools_中元素位置相对应

  uint8_t *pages_buffer_; //可以使用的页面缓冲区
  CflPage *pages_;  //指向CflPage类对象的数组指针
  int size_;        //pool中可缓冲的页面数目

  mysql_mutex_t mutex_;
  list<CflPage*> free_pages_;
  CflPage *Dequeue();
  int Enqueue(CflPage *page);
};

#endif //_CFL_PAGE_H_
