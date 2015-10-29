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

  strcpy(data_file_name, name);
  strcat(data_file_name, CFL_DATA_FILE_SUFFIX);
  
  FILE *f = fopen(data_file_name, "wb+");

  if (f == NULL)
  {
    return -1;
  }

  fclose(f);
  return 0;
}

int
CflData::DestroyDataStorage(const char *name)
{
  char data_file_name[256];

  cfl_data_file_name(data_file_name, sizeof(data_file_name), name);
  int r = remove(data_file_name);

  return 0;
}

CflData*
CflData::Create(const char *name)
{
  char data_file_name[256];

  cfl_data_file_name(data_file_name, sizeof(data_file_name), name);

  FILE *f = fopen(data_file_name, "wb+");

  if (f == NULL)
  {
    return NULL;
  }

  CflData *data = new CflData();
  data->data_file_ = f;

  return data;
}

int
CflData::Destroy(CflData *data)
{
  DBUG_ASSERT(data != NULL);

  fclose(data->data_file_);
  delete data;
}

int
CflData::WritePage(void *page, uint32_t page_size)
{
  
  return 0;
}

