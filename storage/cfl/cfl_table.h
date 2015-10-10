#ifndef _CFL_TABLE_H_
#define _CFL_TABLE_H_

/*
每个CflTable对象对应一个cfl引擎的表，该对象用来组织cfl表的存储
存储设计概要
1、存储分为两部分。索引和数据，数据保存完整的行信息，索引保存索引时间列的信息
2、根据1，将索引和数据分别存储在不同的文件中，并对应不同的对象
*/
class CflIndex;
class CflData;
class CflInsertBuffer;

class CflTable
{
public :
  static int Create();
  static int Destroy();
  static int Open();
  static int Close();

public :
  void Insert();

private :
  CflIndex *index_;
  CflData *data_;
  CflInsertBuffer *insert_buffer_;

  CflTable();
  ~CflTable();
};


#endif //_CFL_TABLE_H_
