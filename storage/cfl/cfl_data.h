#ifndef _CFL_DATA_H_
#define _CFL_DATA_H_

#include <stdint.h>
#include <stdio.h>
#include "cfl.h"

#define CFL_DATA_FILE_SUFFIX ".cfld"
/*
数据存储对象
数据部分只保存数据页，编号从0开始，0表示第一个数据页
*/
class CflData
{
public :
  static int CreateDataStorage(const char *name);
  static int DestroyDataStorage(const char *name);
  static CflData *Create(const char *name);
  static int Destroy(CflData *data);

  void WritePage(uint32_t page_no, void *page, uint32_t page_size);

private :
  //文件
  FILE *data_file_;
};

#endif //_CFL_DATA_H_
