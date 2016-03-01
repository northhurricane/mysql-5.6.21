#ifndef _CFL_INSERT_BUFFER_H_
#define _CFL_INSERT_BUFFER_H_

/*
针对每个cfl表，有一个插入缓冲区，用于进行快速插入
该对象多线程下操作不安全
*/

#include <stdint.h>
#include <vector>
#include <list>
#include "cfl.h"
#include "cfl_dt.h"

using namespace std;

class CflPageMaker;
class CflStorage;

/*
  用于在insert buffer中排序
*/
struct cfl_veckey_struct
{
  cfl_dti_t key;
  uint32_t row_pos;
  uint16_t row_size;
};
typedef struct cfl_veckey_struct cfl_veckey_t;

class CflInsertBuffer
{
public :
  static CflInsertBuffer* Create();
  static CflInsertBuffer* Create(uint32_t size);
  static void Destroy(CflInsertBuffer *insert_buffer);

public :
  /*
    parameter:
      key:转换为ctrip fast log的时间key
      row:转换为ctrip fast log格式的row
      row_size:行数据的长度
    return:
      缓存中的的row所使用的空间
   */
  uint32_t Insert(cfl_dti_t key, void *row, uint16_t row_size);
  /*
    将buffer中的数据刷入磁盘
    return :
      0:刷入磁盘成功
      其他:刷入失败
   */
  int Flush(CflPageMaker *maker, CflStorage *storage);
  /*
    清除buffer中的数据
   */
  void Clear()
  {
    Reset();
  }
  /*
    获取缓存中真正row的个数
  */
  uint32_t GetRowsCount() { return sorted_eles_.size() - 2; }
  /*
    获取插入的行所占用的空间
  */
  uint32_t GetRowsSize() { return offset_; }
  /*
    插入行是否会导致缓冲区溢出
  */
  bool OverFlow(uint32_t row_size)
  {
    if ((row_size + offset_) > buffer_size_)
      return true;
    return false;
  }
  /*
    获取缓冲区中的行
    参数
      nth     ：输入参数，1-based，所要获取的行
      row_size：输出参数，所获取行的大小
    return
      如果成功，返回行所在的地址；否则返回0（空指针）
  */
  const uint8_t* GetNthRow(uint32_t nth, uint32_t *row_size)
  {
    *row_size = 0;
    if (nth > GetRowsCount())
      return NULL;
    *row_size = sorted_eles_[nth].row_size;
    return buffer_ + sorted_eles_[nth].row_pos;
  }

  /*
    GetNthRow并不是完全安全的，返回的指针回暴露内部的存储。
    定义一个未实现的函数来特别说明一下
  */
  uint8_t* GetNthRowSafe(uint32_t nth, uint8_t *buffer
                         , uint32_t buffer_size, uint32_t *row_size)
  {
    return NULL;
  }

private :
  //返回插入位置
  uint32_t Locate(cfl_dti_t key);
  int Initialize();
  int Initialize(uint32_t size);
  bool Deinitialize();

  CflInsertBuffer() {}
  ~CflInsertBuffer() {}

  //初始化
  inline void InitFakeRecord()
  {
    DBUG_ASSERT(sorted_eles_.size() == 0);
    cfl_veckey_t min = {CFL_DTI_MIN, 0, 0};
    cfl_veckey_t max = {CFL_DTI_MAX, 0, 0};

    sorted_eles_.push_back(min);
    sorted_eles_.push_back(max);
  }

  //重置insert buffer的内部数据，处于插入的初始状态
  inline void Reset()
  {
    offset_ = 0;
    sorted_eles_.clear();
    InitFakeRecord();
  }
  
private :
  //缓冲区，数据将顺序的写入缓冲区
  uint8_t  *buffer_;
  uint32_t buffer_size_;
  uint32_t offset_; //当前写入位置

  //小于下面key值的记录将被无法插入
  cfl_dti_t max_logged_key_;

  //当前排序的数据索引
  vector<cfl_veckey_t> sorted_eles_;
};

#define CFL_INSERT_BUFFER_POOL_DEFAULT_SIZE (1)

class CflInsertBufferPool
{
public :
  static CflInsertBufferPool* Create(uint32_t pool_size, uint32_t buffer_size);
  static int Destroy(CflInsertBufferPool *pool);

  CflInsertBuffer* GetBuffer();
  void PutBuffer(CflInsertBuffer *page);

private :
  CflInsertBufferPool() {}

  void Lock()
  {
    mysql_mutex_lock(&pool_mutex_);
  }
  void Unlock()
  {
    mysql_mutex_unlock(&pool_mutex_);
  }

  int Initialize();
  int Deinitialize();
  list<CflInsertBuffer*> free_buffers_;
  mysql_mutex_t pool_mutex_;

  uint32_t buffer_size_;
};

#endif //_CFL_INSERT_BUFFER_H_
