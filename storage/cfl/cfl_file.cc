#include "cfl_file.h"
#include <unistd.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <errno.h>
#include <string.h>

bool
cf_exist(const char *name)
{
  if( access( name, F_OK ) != -1 ) {
    return true;
  } else {
    return false;
  }
}

int
cf_create(const char * name)
{
  if (cf_exist(name))
    return -1;

  int fd;
  mode_t mode;

  mode = S_IRUSR | S_IWUSR;
  fd = open(name, O_RDWR|O_CREAT|O_WRONLY|O_TRUNC, mode);
  if (fd < 0)
    return -1;
  close(fd);

  return 0;
}

int
cf_destroy(const char *name)
{
  if (!cf_exist(name))
    return -1;
  int r = remove(name);
  if (r < 0)
    return -1;
  return 0;
}

int
cf_open(const char * name, cf_t *cf)
{
  if (!cf_exist(name))
    return -1;
  int fd = open(name, O_RDWR);
  if (fd < 0)
    return -1;
  cf->fd = fd;
  return 0;
}

int
cf_close(cf_t *cf)
{
  int r = close(cf->fd);
  if (r < 0)
    return -1;

  return 0;
}

int64_t
cf_size(cf_t *cf)
{
  off_t off = lseek(cf->fd, 0, SEEK_END);
  if (off < 0)
    return -1;
  return (int64_t)off;
}

int
cf_write(cf_t *cf, uint64_t offset, void *data, int bytes2write)
{
  if (bytes2write <= 0)
    return -1;

  int r;
  r = pwrite(cf->fd, data, bytes2write, (off_t)offset);
  return 0;
}

int
cf_read(cf_t *cf, uint64_t offset
        , void *buf, uint32_t buf_size, int bytes2read)
{
  if (bytes2read <= 0)
    return -1;
  int r;
  r = pread(cf->fd, buf, bytes2read, (off_t)offset);
  return 0;
}

int64_t
cf_extend(cf_t *cf, uint64_t size)
{
  int64_t file_size;
  if (size == 0)
    return 0;
  file_size = cf_size(cf);
  if (file_size < 0)
    return -1;
  if ((uint64_t)file_size >= size)
    return file_size;
  uint8_t buf[1] = {0};
  ssize_t nsize = pwrite(cf->fd, buf, sizeof(buf), size - 1);
  if (nsize < 0)
    return -1;

  return size;
}
