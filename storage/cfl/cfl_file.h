#ifndef _CFL_FILE_H_
#define _CFL_FILE_H_

#include <stdint.h>

/*
  该实现仅考虑linux下的文件操作，其他平台下的文件操作需要另外处理
*/

struct cf_struct
{
  int fd;  //文件句柄
};

typedef struct cf_struct cf_t;

/*
创建文件
返回值:-1失败，0成功
*/
int
cf_create(const char *);

/*
销毁文件
返回值:-1失败，0成功
*/
int
cf_destroy(const char *);

/*
打开文件
返回值:-1失败，0成功
*/
int
cf_open(const char *, cf_t *cf);

/*
关闭文件
返回值:-1失败，0成功
*/
int
cf_close(cf_t *cf);

/*
获取文件大小
返回值:-1失败，成功返回文件大小
*/
int64_t
cf_size(cf_t *cf);

/*
数据写入文件
参数:
  cf:文件
  offset:0-based，写入的偏移位置
  data:数据
  bytes2write:写入数据的字节数
返回值:
  -1失败，成功返回写入的字节数
*/
int
cf_write(cf_t *cf, uint64_t offset, const void *data, int bytes2write);

/*
从文件读取数据
参数:
  cf:文件
  offset:0-based，读取偏移
  data:读取缓冲区
  bytes2read:读取字节数
返回值:
  -1失败，成功返回成功读取的字节数
*/
int
cf_read(cf_t *cf, uint64_t offset
        , void *buf, uint32_t buf_size, int bytes2read);

/*
扩展文件到指定的大小
返回值:-1失败，成功返回文件大小
*/
int64_t
cf_extend(cf_t *cf, uint64_t size);

#endif //_CFL_FILE_H_
