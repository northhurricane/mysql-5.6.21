#ifndef _CFL_TABLE_H_
#define _CFL_TABLE_H_

#include <my_pthread.h>
#include "cfl_dt.h"
#include "cfl_page.h"

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
  void WritePage(void *page, uint32_t rows_count);

private :
  CflIndex *index_;
  CflData *data_;
};

class CflTable
{
public :
  static CflTable* Create();
  static int Destroy();
  static CflTable* Open();
  static int Close();

public :
  void Insert(cfl_dti_t key, void *row, uint16_t row_size);

private :
  CflInsertBuffer *insert_buffer_;
  CflPageMaker *maker_;
  CflStorage *storage_;

  /*插入缓冲区保护，在进行行插入时保护插入缓冲区*/
  mysql_mutex_t insert_buffer_mutex_;

private :
  bool PageOverflow(uint16_t row_size);
  CflTable();
  ~CflTable();
};


#endif //_CFL_TABLE_H_
