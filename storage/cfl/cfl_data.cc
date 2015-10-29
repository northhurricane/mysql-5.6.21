#include <string.h>
#include "cfl_data.h"

int
cfl_data_file_name(char *buffer, uint32_t buffer_size, const char *name)
{
  char *data_file_name = (char*)buffer;

  strcpy(data_file_name, name);
  strcat(data_file_name, CFL_DATA_FILE_SUFFIX);

  return 0;
}

int
CflData::CreateDataStorage(const char *name)
{
  char data_file_name[256];

  cfl_data_file_name(data_file_name, sizeof(data_file_name), name);

  int r = cf_create(data_file_name);
  if (r < 0)
  {
    return -1;
  }

  return 0;
}

int
CflData::DestroyDataStorage(const char *name)
{
  char data_file_name[256];

  cfl_data_file_name(data_file_name, sizeof(data_file_name), name);
  int r = remove(data_file_name);
  if (r < 0)
    return -1;

  return 0;
}

CflData*
CflData::Create(const char *name)
{
  char data_file_name[256];

  cfl_data_file_name(data_file_name, sizeof(data_file_name), name);

  cf_t cf_file;
  int r = cf_open(data_file_name, &cf_file);
  if (r < 0)
    return NULL;

  CflData *data = new CflData();
  data->cf_file_ = cf_file;

  return data;
}

int
CflData::Destroy(CflData *data)
{
  DBUG_ASSERT(data != NULL);

  cf_close(&data->cf_file_);
  delete data;

  return 0;
}

int
CflData::WritePage(void *page, uint32_t page_size)
{
  uint64_t offset = 0;

  int r = cf_write(&cf_file_, offset, page, page_size);
  if (r < 0)
    return -1;

  return 0;
}

