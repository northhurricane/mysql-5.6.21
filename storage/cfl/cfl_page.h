#ifndef _CFL_PAGE_H_
#define _CFL_PAGE_H_

#include <stdint.h>
#include <list>
#include <string.h>
#include "cfl_endian.h"
#include "cfl_dt.h"
#include <field.h>
#include "cfl_row.h"

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
/*
rows index
  用于存储页面内行的位置。与行的写入顺序相反，index的写入时从页尾向页头写，
  为了便于计算行的长度，最后一个index写入的是一个虚拟的行偏移，也就是最后一行的结尾
*/

#define CFL_PAGE_SIZE (64 * 1024)

#define CFL_PAGE_HEAD_SIZE (16)
#define CFL_PAGE_HEAD (0)
#define CFL_PAGE_ROW_COUNT (CFL_PAGE_HEAD)

#define CFL_PAGE_ROW_POS_SIZE (2)

#define CFL_PAGE_MAGIC_HEAD (0XAC81FD7321CA92FB)
#define CFL_PAGE_MAGIC_TAIL (0XC725ADFE6420CB63)

#define CFL_PAGE_MAGIC_HEAD_SIZE (8)
#define CFL_PAGE_MAGIC_TAIL_SIZE (8)

#define CFL_PAGE_FIX_SPACE_SIZE (CFL_PAGE_MAGIC_HEAD_SIZE + \
                                 CFL_PAGE_MAGIC_TAIL_SIZE + CFL_PAGE_HEAD_SIZE)

/*
  写入行数
*/
inline void
cfl_page_write_row_count(void *page, uint32_t row_count)
{
  uint32_t offset = CFL_PAGE_MAGIC_HEAD_SIZE + CFL_PAGE_ROW_COUNT;
  uint8_t *pos = (uint8_t*)page;
  pos += offset;
  endian_write_uint32(pos, row_count);
}

/*
  读取行数
*/
inline uint32_t
cfl_page_read_row_count(void *page)
{
  uint32_t offset = CFL_PAGE_MAGIC_HEAD_SIZE + CFL_PAGE_ROW_COUNT;
  uint8_t *pos = (uint8_t*)page;
  pos += offset;
  return endian_read_uint32(pos);
}

/*
  写入偏移
*/
inline void
cfl_page_write_row_offset(uint8_t *addr, uint32_t v)
{
  addr[0] = (uint8_t)(v & 0xFF);
  addr[1] = (uint8_t)((v & 0xFF00) >> 8);
  //addr[2] = (uint8_t)((v & 0xFF0000) >> 8);
}

/*
  读取偏移
*/
inline uint32_t
cfl_page_read_row_offset(uint8_t *addr)
{
  uint32_t byte1 = 0, byte2 = 0;//, byte3 = 0;
  byte1 = addr[0];
  byte2 = addr[1] << 8;
  //byte3 = addr[2] << 16;

  uint32_t temp = byte1 | byte2;// | byte3;
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

/*
  读取第nth个row的地址
  nth:0-base
*/
inline uint8_t*
cfl_page_nth_row(uint8_t *page_data, uint32_t nth)
{
  uint8_t  *nth_off;
  uint8_t  *row;
  uint32_t offset;

  nth_off = cfl_page_nth_row_offset(page_data, nth);
  offset = cfl_page_read_row_offset(nth_off);
  row = page_data + offset;

  return row;
}

/*
  读取第nth个row的地址
  nth:0-base
*/
inline cfl_dti_t
cfl_page_nth_row_key(uint8_t *page_data, uint32_t nth, Field **fields)
{
  uint8_t *row = cfl_page_nth_row(page_data, nth);
  return cfl_row_get_key_data(fields, row);
}

/*
  获取第nth个row的地址
  返回值
    返回行长度
    0表示不存在该行
  参数
    nth:0-based
*/
inline uint32_t
cfl_page_nth_row_length(uint8_t * page_data, uint32_t nth)
{
  uint8_t  *nth_off;
  uint32_t row_count;

  row_count = cfl_page_read_row_count(page_data);
  if (nth >= row_count)
    return 0;

  uint32_t offset;
  nth_off = cfl_page_nth_row_offset(page_data, nth);
  offset = cfl_page_read_row_offset(nth_off);

  uint32_t offset_next;
  nth_off = cfl_page_nth_row_offset(page_data, nth + 1);
  offset_next = cfl_page_read_row_offset(nth_off);

  return offset_next - offset;
}

/*
  检查新行插入是否导致当前页面。
*/
inline
bool cfl_will_insert_cause_page_overflow(uint32_t row_size
                                         , uint32_t exist_rows
                                         , uint32_t exist_rows_size)
{
  //计算当前行是否导致存储内容超过cfl的page容量
  //计算行数所占中间
  uint32_t total_rows_size = row_size + exist_rows_size;
  //页面内索引所占空间。由于page的设计原因，包括当前行的index entry，还要增加一个index entry的空间。参考cfl_page.cc
  uint32_t index_size = (exist_rows + 2) * CFL_PAGE_ROW_POS_SIZE;
  //计算占用的所有空间
  uint32_t total_size = CFL_PAGE_FIX_SPACE_SIZE + total_rows_size + index_size;

  //测试用
  /*if (total_size > 80)
    return true;*/

  if (total_size > CFL_PAGE_SIZE)
    return true;
  return false;

}

/*
  在页面内定位行。
  参数：
    row_no:0-based。返回值为true，指向定位到的数据；返回值为false，指向大于该记录的最小记录
    
  返回值：
    true,找到等值的key
    false,未找到等值的key
  
*/
bool
cfl_page_locate_row(void *page, Field **fields
                    , cfl_dti_t key, uint32_t *row_no);

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
  void Flush(CflStorage *storage, cfl_dti_t key);

private :
  uint32_t rows_counter_;
  uint8_t *buffer_;
  uint32_t row_pos_; //行在页面内的偏移
  uint8_t *row_offset_; //写入行偏移
  uint8_t *pos_offset_; //位置偏移

  //重置状态
  void Reset()
  {
    rows_counter_ = 0;
    row_pos_ = CFL_PAGE_MAGIC_HEAD_SIZE + CFL_PAGE_HEAD_SIZE;
    row_offset_ = buffer_ + CFL_PAGE_MAGIC_HEAD_SIZE + CFL_PAGE_HEAD_SIZE;
    pos_offset_ = (buffer_ + CFL_PAGE_SIZE) - CFL_PAGE_MAGIC_TAIL_SIZE
                                            - CFL_PAGE_ROW_POS_SIZE;
    //magic head/tail填写
    uint64_t magic = CFL_PAGE_MAGIC_HEAD;
    endian_write_uint64(buffer_, magic);
    magic = CFL_PAGE_MAGIC_TAIL;
    endian_write_uint64((buffer_ + CFL_PAGE_SIZE) - CFL_PAGE_MAGIC_TAIL_SIZE
                        , magic);
  }

  CflPageMaker() {
    buffer_ = NULL;
  }

  ~CflPageMaker() {
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

  /*
    从存储中获取页
    参数：
      nth_page:
  */
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
