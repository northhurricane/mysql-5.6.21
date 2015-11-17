#ifndef _CFL_DATA_H_
#define _CFL_DATA_H_

#include <stdint.h>
#include <stdio.h>
#include "cfl.h"
#include "cfl_file.h"

#define CFL_DATA_FILE_SUFFIX ".cfld"
/*
数据存储对象
数据部分只保存数据页，编号从0开始，0表示第一个数据页
有效的数据页不记录在data file中，有效的数据页通过Create函数通过参数传入
*/
class CflData
{
public :
  static int CreateDataStorage(const char *name);
  static int DestroyDataStorage(const char *name);
  static CflData *Create(const char *name, uint32_t page_no);
  static int Destroy(CflData *data);

  int WritePage(void *page, uint32_t page_size);
  /*
    读取指定页面
    参数
      nth_page:0-based。读取页面的
   */
  int ReadPage(void *buffer, uint32_t buffer_size, uint32_t nth_page);

private :
  cf_t cf_file_;
  uint32_t curr_page_no_;  //0-based。当前写入页的编号
  uint64_t file_size_; //当前文件大小
};

#endif //_CFL_DATA_H_
