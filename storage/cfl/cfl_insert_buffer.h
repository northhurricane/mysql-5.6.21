#ifndef _CFL_INSERT_BUFFER_H_
#define _CFL_INSERT_BUFFER_H_

/*
针对每个cfl表，有一个插入缓冲区，用于进行快速插入
该对象多线程下操作不安全
*/

#include <stdint.h>
#include <vector>
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
  uint32_t GetRowsSize() { return offset_; }

private :
  //返回插入位置
  uint32_t Locate(cfl_dti_t key);
  int Initialize();
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


#endif //_CFL_INSERT_BUFFER_H_
