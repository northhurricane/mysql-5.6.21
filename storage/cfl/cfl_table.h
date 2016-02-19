#ifndef _CFL_TABLE_H_
#define _CFL_TABLE_H_

#include <my_pthread.h>
#include "cfl_dt.h"
#include "cfl_page.h"
#include "cfl.h"
#include "cfl_cursor.h"
#include <string>

using namespace std;
/*
每个CflTable对象对应一个cfl引擎的表，该对象用来组织cfl表的存储
存储设计概要
1、存储分为两部分。索引和数据，数据保存完整的行信息，索引保存索引时间列的信息
2、根据1，将索引和数据分别存储在不同的文件中，并对应不同的对象
3、写入顺序。首先在data中写入page，然后在index中写入索引项，最后在索引头中更新索引项的计数。这样的写入顺序可以确保在意外宕机后，再次启动不会出现数据上的错误。但会造成数据丢失。可在事务功能添加后进行确保数据的持久性需求。
*/
class CflIndex;
class CflData;
class CflInsertBuffer;

/*
  cfl表的存储对象
*/
class CflStorage
{
public :
  /*创建进行存储的文件*/
  static int CreateStorage(const char *name);
  /*销毁进行存储的文件*/
  static int DestroyStorage(const char *name);
  /*创建对象，打开进行存储的文件与之关联*/
  static CflStorage *Open(const char *name);
  static int Close(CflStorage *storage);

  //
  int WritePage(void *page, uint32_t rows_count, cfl_dti_t dti);
  /*
    读取数据页
    参数：
      nth_page:0-based.
  */
  int ReadPage(void *buffer, uint32_t buffer_size, uint32_t nth_page);

  /*
    定位记录
    返回值：
      true :定位到满足条件的记录
      false:没有定位到满足到的记录
    参数：
      key   :搜索key
      keycmp:比较方式
      result:
        返回值为true有意义
        KEY_EQUAL，返回等于key的第一条记录
        KEY_GE   ，返回等于key的第一条记录
        KEY_G    ，返回大于key的第一条记录
        KEY_LE   ，返回等于key或者小于key的最后一条记录
        KEY_L    ，返回小于key的最后一条记录
  */
  bool LocateRow(cfl_dti_t key, enum cfl_key_cmp keycmp
                 , cfl_cursor_t &cursor, Field **fields);

private :
  CflIndex *index_;
  CflData *data_;

  CflStorage()
  {
    index_ = NULL; data_ = NULL;
  }
  ~CflStorage()
  {
  }
  int Initialize(const char *name);
  int Deinitialize();
};

class CflTable
{
public :
  static int CreateStorage(const char *name);
  static int DestroyStorage(const char *name);

  static CflTable* Create(const char *name);
  static int Destroy(CflTable *table);
  /*static CflTable* Open();
    static int Close();*/

public :
  void Insert(cfl_dti_t key, void *row, uint16_t row_size);
  int Truncate();

  CflStorage *GetStorage() { return storage_; }

private :
  int Initialize(const char *name);
  int Deinitialize();

  CflInsertBuffer *insert_buffer_;
  CflPageMaker *maker_;
  CflStorage *storage_;

  /*插入缓冲区保护，在进行行插入时保护插入缓冲区*/
  mysql_mutex_t insert_buffer_mutex_;

protected :
  static CflTable* CreateInstance(const char *name);
  static int DestroyInstance(CflTable *table);

  string table_name_;
  const string &GetTableName() { return table_name_; }

  uint32_t ref_count_;
  uint32_t GetRefCount() { return ref_count_; }
  uint32_t IncRefCount() { ref_count_++; return GetRefCount(); }
  uint32_t DecRefCount() { ref_count_--; return GetRefCount(); }
private :
  bool PageOverflow(uint16_t row_size);
  CflTable() {
    insert_buffer_ = NULL;
    maker_ = NULL;
    storage_ = NULL;
    ref_count_ = 0;
  }
  ~CflTable() {}

  friend class CflTableInstanceManager;
};


#endif //_CFL_TABLE_H_
