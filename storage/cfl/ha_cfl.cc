/* Copyright (c) 2004, 2013, Oracle and/or its affiliates. All rights reserved.

  This program is free software; you can redistribute it and/or modify
  it under the terms of the GNU General Public License as published by
  the Free Software Foundation; version 2 of the License.

  This program is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License for more details.

  You should have received a copy of the GNU General Public License
  along with this program; if not, write to the Free Software
  Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

/**
  @file ha_cfl.cc

  @brief
  The ha_cfl engine is a stubbed storage engine for example purposes only;
  it does nothing at this point. Its purpose is to provide a source
  code illustration of how to begin writing new storage engines; see also
  /storage/cfl/ha_cfl.h.

  @details
  ha_cfl will let you create/open/delete tables, but
  nothing further (for example, indexes are not supported nor can data
  be stored in the table). Use this example as a template for
  implementing the same functionality in your own storage engine. You
  can enable the example storage engine in your build by doing the
  following during your build process:<br> ./configure
  --with-example-storage-engine

  Once this is done, MySQL will let you create tables with:<br>
  CREATE TABLE <table name> (...) ENGINE=EXAMPLE;

  Please read the object definition in ha_cfl.h before reading the rest
  of this file.

  @note
  When you create an EXAMPLE table, the MySQL Server creates a table .frm
  (format) file in the database directory, using the table name as the file
  name as is customary with MySQL. No other files are created. To get an idea
  of what occurs, here is an example select that would do a scan of an entire
  table:

  @code
  ha_cfl::store_lock
  ha_cfl::external_lock
  ha_cfl::info
  ha_cfl::rnd_init
  ha_cfl::extra
  ENUM HA_EXTRA_CACHE        Cache record in HA_rrnd()
  ha_cfl::rnd_next
  ha_cfl::rnd_next
  ha_cfl::rnd_next
  ha_cfl::rnd_next
  ha_cfl::rnd_next
  ha_cfl::rnd_next
  ha_cfl::rnd_next
  ha_cfl::rnd_next
  ha_cfl::rnd_next
  ha_cfl::extra
  ENUM HA_EXTRA_NO_CACHE     End caching of records (def)
  ha_cfl::external_lock
  ha_cfl::extra
  ENUM HA_EXTRA_RESET        Reset database to after open
  @endcode

  Happy coding!<br>
*/

#include "sql_priv.h"
#include "sql_class.h"           // MYSQL_HANDLERTON_INTERFACE_VERSION
#include "ha_cfl.h"
#include "probes_mysql.h"
#include "sql_plugin.h"
#include "cfl.h"
#include "cfl_table.h"
#include "cfl_row.h"

static handler *cfl_create_handler(handlerton *hton,
                                   TABLE_SHARE *table, 
                                   MEM_ROOT *mem_root);

#define CFL_INDEX_TIMESTAMP_NAME "key_timestamp"
#define CFL_INDEX_TIMESTAMP_SCALE 6

static int
cfl_check_create_info(TABLE *table_arg, HA_CREATE_INFO *create_info)
{
  //进行列检查
  Field *rfield;
  bool has_key_column = false;
  for (Field **field= table_arg->s->field; *field; field++)
  {
    rfield = *field;

    //必须存在一个用于
    enum_field_types type = rfield->type();
    if (MYSQL_TYPE_TIMESTAMP == type)
    {
      uint scale = rfield->decimals();
      if (strcmp(rfield->field_name, CFL_INDEX_TIMESTAMP_NAME) == 0)
      {
        if (scale == CFL_INDEX_TIMESTAMP_SCALE)
        {
          has_key_column = true;
        }
      }
    }
    //现阶段不允许存在null的列
    if (rfield->real_maybe_null())
    {
      my_error(ER_CHECK_NOT_IMPLEMENTED, MYF(0), "nullable columns");
      return (HA_ERR_UNSUPPORTED);
    }
  }

  if (!has_key_column)
  {
    my_error(ER_CHECK_NOT_IMPLEMENTED, MYF(0)
             , " that none timestamp(6) field is defined in create table");
    return (HA_ERR_UNSUPPORTED);
  }

  //如果带有index的信息，进行index信息的查询
  if (table_arg->s->keys > 1)
  {
    my_error(ER_CHECK_NOT_IMPLEMENTED, MYF(0)
             , " more than one index defined.");
    return (HA_ERR_UNSUPPORTED);
  }

  if (table_arg->s->keys == 1)
  {
    KEY *index = table_arg->s->key_info;
    if (index->actual_key_parts != 1)
    {
      my_error(ER_CHECK_NOT_IMPLEMENTED, MYF(0)
               , " more than 1 key in index");
      return (HA_ERR_UNSUPPORTED);
    }

    if (index->actual_key_parts != index->usable_key_parts)
    {
      my_error(ER_CHECK_NOT_IMPLEMENTED, MYF(0)
               , " wrong key defination.");
      return (HA_ERR_UNSUPPORTED);
    }

    if (strcmp(index->key_part[0].field->field_name
               , CFL_INDEX_TIMESTAMP_NAME) != 0)
    {
      my_error(ER_CHECK_NOT_IMPLEMENTED, MYF(0)
               , " key not on key_timestamp field");
      return (HA_ERR_UNSUPPORTED);
    }
  }

  return 0;
}

handlerton *cfl_ton;

/* Interface to mysqld, to check system tables supported by SE */
static const char* cfl_system_database();
static bool cfl_is_supported_system_table(const char *db,
                                      const char *table_name,
                                      bool is_sql_layer_system_table);
#ifdef HAVE_PSI_INTERFACE
static PSI_mutex_key ex_key_mutex_cfl_share_mutex;

static PSI_mutex_info all_cfl_mutexes[]=
{
  { &ex_key_mutex_cfl_share_mutex, "Cfl_share::mutex", 0}
};

static void init_cfl_psi_keys()
{
  const char* category= "cfl";
  int count;

  count= array_elements(all_cfl_mutexes);
  mysql_mutex_register(category, all_cfl_mutexes, count);
}
#endif

Cfl_share::Cfl_share()
{
  thr_lock_init(&lock);
  mysql_mutex_init(ex_key_mutex_cfl_share_mutex,
                   &mutex, MY_MUTEX_INIT_FAST);
}

static int
cfl_init_handlerton(handlerton *cfl_ton)
{
  cfl_ton->state=                     SHOW_OPTION_YES;
  cfl_ton->create=                    cfl_create_handler;
  cfl_ton->flags=                     HTON_CAN_RECREATE;
  cfl_ton->system_database=   cfl_system_database;
  cfl_ton->is_supported_system_table= cfl_is_supported_system_table;

  return 0;
}

static int cfl_init_func(void *p)
{
  DBUG_ENTER("cfl_init_func");

#ifdef HAVE_PSI_INTERFACE
  init_cfl_psi_keys();
#endif

  cfl_ton= (handlerton *)p;

  cfl_init_handlerton(cfl_ton);
  CflPageManager::Initialize(1, 10);

  DBUG_RETURN(0);
}

static int cfl_deinit_func(void *p)
{
  CflPageManager::Deinitialize();

  return 0;
}


/**
  @brief
  Example of simple lock controls. The "share" it creates is a
  structure we will pass to each example handler. Do you have to have
  one of these? Well, you have pieces that are used for locking, and
  they are needed to function.
*/

Cfl_share *ha_cfl::get_share()
{
  Cfl_share *tmp_share;

  DBUG_ENTER("ha_cfl::get_share()");

  lock_shared_ha_data();
  if (!(tmp_share= static_cast<Cfl_share*>(get_ha_share_ptr())))
  {
    tmp_share= new Cfl_share;
    if (!tmp_share)
      goto err;

    set_ha_share_ptr(static_cast<Handler_share*>(tmp_share));
  }
err:
  unlock_shared_ha_data();
  DBUG_RETURN(tmp_share);
}


static handler* cfl_create_handler(handlerton *hton,
                                   TABLE_SHARE *table, 
                                   MEM_ROOT *mem_root)
{
  return new (mem_root) ha_cfl(hton, table);
}

ha_cfl::ha_cfl(handlerton *hton, TABLE_SHARE *table_arg)
  :handler(hton, table_arg)
{
  cfl_table_ = NULL;
  DBUG_ASSERT(1);
}

ha_cfl::~ha_cfl()
{
  if (cfl_table_ != NULL)
    CflTable::Destroy(cfl_table_);
}


/**
  @brief
  If frm_error() is called then we will use this to determine
  the file extensions that exist for the storage engine. This is also
  used by the default rename_table and delete_table method in
  handler.cc.

  For engines that have two file name extentions (separate meta/index file
  and data file), the order of elements is relevant. First element of engine
  file name extentions array should be meta/index file extention. Second
  element - data file extention. This order is assumed by
  prepare_for_repair() when REPAIR TABLE ... USE_FRM is issued.

  @see
  rename_table method in handler.cc and
  delete_table method in handler.cc
*/

static const char *ha_cfl_exts[] = {
  NullS
};

const char **ha_cfl::bas_ext() const
{
  return ha_cfl_exts;
}

/*
  Following handler function provides access to
  system database specific to SE. This interface
  is optional, so every SE need not implement it.
*/
const char* ha_cfl_system_database= NULL;
const char* cfl_system_database()
{
  return ha_cfl_system_database;
}

/*
  List of all system tables specific to the SE.
  Array element would look like below,
     { "<database_name>", "<system table name>" },
  The last element MUST be,
     { (const char*)NULL, (const char*)NULL }

  This array is optional, so every SE need not implement it.
*/
static st_system_tablename ha_cfl_system_tables[]= {
  {(const char*)NULL, (const char*)NULL}
};

/**
  @brief Check if the given db.tablename is a system table for this SE.

  @param db                         Database name to check.
  @param table_name                 table name to check.
  @param is_sql_layer_system_table  if the supplied db.table_name is a SQL
                                    layer system table.

  @return
    @retval TRUE   Given db.table_name is supported system table.
    @retval FALSE  Given db.table_name is not a supported system table.
*/
static bool cfl_is_supported_system_table(const char *db,
                                              const char *table_name,
                                              bool is_sql_layer_system_table)
{
  st_system_tablename *systab;

  // Does this SE support "ALL" SQL layer system tables ?
  if (is_sql_layer_system_table)
    return false;

  // Check if this is SE layer system tables
  systab= ha_cfl_system_tables;
  while (systab && systab->db)
  {
    if (systab->db == db &&
        strcmp(systab->tablename, table_name) == 0)
      return true;
    systab++;
  }

  return false;
}


/**
  @brief
  Used for opening tables. The name will be the name of the file.

  @details
  A table is opened when it needs to be opened; e.g. when a request comes in
  for a SELECT on the table (tables are not open and closed for each request,
  they are cached).

  Called from handler.cc by handler::ha_open(). The server opens all tables by
  calling ha_open() which then calls the handler specific open().

  @see
  handler::ha_open() in handler.cc
*/

int ha_cfl::open(const char *name, int mode, uint test_if_locked)
{
  DBUG_ENTER("ha_cfl::open");

  if (!(share = get_share()))
    DBUG_RETURN(1);
  thr_lock_data_init(&share->lock,&lock,NULL);

  cfl_table_ = CflTable::Create(name);

  DBUG_RETURN(0);
}


/**
  @brief
  Closes a table.

  @details
  Called from sql_base.cc, sql_select.cc, and table.cc. In sql_select.cc it is
  only used to close up temporary tables or during the process where a
  temporary table is converted over to being a myisam table.

  For sql_base.cc look at close_data_tables().

  @see
  sql_base.cc, sql_select.cc and table.cc
*/

int ha_cfl::close(void)
{
  DBUG_ENTER("ha_cfl::close");
  DBUG_RETURN(0);
}


/**
  @brief
  write_row() inserts a row. No extra() hint is given currently if a bulk load
  is happening. buf() is a byte array of data. You can use the field
  information to extract the data from the native byte array type.

  @details
  Example of this would be:
  @code
  for (Field **field=table->field ; *field ; field++)
  {
    ...
  }
  @endcode

  See ha_tina.cc for an example of extracting all of the data as strings.
  ha_berekly.cc has an example of how to store it intact by "packing" it
  for ha_berkeley's own native storage type.

  See the note for update_row() on auto_increments. This case also applies to
  write_row().

  Called from item_sum.cc, item_sum.cc, sql_acl.cc, sql_insert.cc,
  sql_insert.cc, sql_select.cc, sql_table.cc, sql_udf.cc, and sql_update.cc.

  @see
  item_sum.cc, item_sum.cc, sql_acl.cc, sql_insert.cc,
  sql_insert.cc, sql_select.cc, sql_table.cc, sql_udf.cc and sql_update.cc
*/

//static cfl_dti_t test_key = 0; //for test insert buffer key

int ha_cfl::write_row(uchar *buf)
{
  DBUG_ENTER("ha_cfl::write_row");

  /*
    Example of a successful write_row. We don't store the data
    anywhere; they are thrown away. A real implementation will
    probably need to do something with 'buf'. We report a success
    here, to pretend that the insert was successful.
  */
  //获取记录的长度
  //  uint32_t rec_length = table->s->reclength;
  uint8_t cfl_row_buf[65536];
  uint32_t cfl_row_size = 0;
  cfl_dti_t key_dti = 0;
  my_bitmap_map *org_bitmap= dbug_tmp_use_all_columns(table, table->read_set);
  cfl_row_size = cfl_row_from_mysql(table->field, buf
                                    , cfl_row_buf, sizeof(cfl_row_buf)
                                    , &key_dti);
  dbug_tmp_restore_column_map(table->read_set, org_bitmap);
  //将cfl的row写入insert buffer
  if (cfl_row_size == 0)
  {
    DBUG_RETURN(1);
  }
  cfl_table_->Insert(key_dti, cfl_row_buf, cfl_row_size);

  DBUG_RETURN(0);
}


/**
  @brief
  Yes, update_row() does what you expect, it updates a row. old_data will have
  the previous row record in it, while new_data will have the newest data in it.
  Keep in mind that the server can do updates based on ordering if an ORDER BY
  clause was used. Consecutive ordering is not guaranteed.

  @details
  Currently new_data will not have an updated auto_increament record. You can
  do this for example by doing:

  @code

  if (table->next_number_field && record == table->record[0])
    update_auto_increment();

  @endcode

  Called from sql_select.cc, sql_acl.cc, sql_update.cc, and sql_insert.cc.

  @see
  sql_select.cc, sql_acl.cc, sql_update.cc and sql_insert.cc
*/
int ha_cfl::update_row(const uchar *old_data, uchar *new_data)
{

  DBUG_ENTER("ha_cfl::update_row");
  DBUG_RETURN(HA_ERR_WRONG_COMMAND);
}


/**
  @brief
  This will delete a row. buf will contain a copy of the row to be deleted.
  The server will call this right after the current row has been called (from
  either a previous rnd_nexT() or index call).

  @details
  If you keep a pointer to the last row or can access a primary key it will
  make doing the deletion quite a bit easier. Keep in mind that the server does
  not guarantee consecutive deletions. ORDER BY clauses can be used.

  Called in sql_acl.cc and sql_udf.cc to manage internal table
  information.  Called in sql_delete.cc, sql_insert.cc, and
  sql_select.cc. In sql_select it is used for removing duplicates
  while in insert it is used for REPLACE calls.

  @see
  sql_acl.cc, sql_udf.cc, sql_delete.cc, sql_insert.cc and sql_select.cc
*/

int ha_cfl::delete_row(const uchar *buf)
{
  DBUG_ENTER("ha_cfl::delete_row");
  DBUG_RETURN(HA_ERR_WRONG_COMMAND);
}


/**
  @brief
  Positions an index cursor to the index specified in the handle. Fetches the
  row if available. If the key value is null, begin at the first key of the
  index.
*/

/*int ha_cfl::index_read_map(
  uchar *buf, const uchar *key,
  key_part_map keypart_map __attribute__((unused)),
  enum ha_rkey_function find_flag
  __attribute__((unused)))
{
  int rc;
  DBUG_ENTER("ha_cfl::index_read");
  MYSQL_INDEX_READ_ROW_START(table_share->db.str, table_share->table_name.str);
  rc= HA_ERR_WRONG_COMMAND;
  MYSQL_INDEX_READ_ROW_DONE(rc);
  DBUG_RETURN(rc);
}
*/


/**
  @brief
  rnd_init() is called when the system wants the storage engine to do a table
  scan. See the example in the introduction at the top of this file to see when
  rnd_init() is called.

  @details
  Called from filesort.cc, records.cc, sql_handler.cc, sql_select.cc, sql_table.cc,
  and sql_update.cc.

  @see
  filesort.cc, records.cc, sql_handler.cc, sql_select.cc, sql_table.cc and sql_update.cc
*/
int ha_cfl::rnd_init(bool scan)
{
  DBUG_ENTER("ha_cfl::rnd_init");

  cfl_cursor_init(cursor_);
  
  DBUG_RETURN(0);
}

int ha_cfl::rnd_end()
{
  DBUG_ENTER("ha_cfl::rnd_end");
  DBUG_RETURN(0);
}


/**
  @brief
  This is called for each row of the table scan. When you run out of records
  you should return HA_ERR_END_OF_FILE. Fill buff up with the row information.
  The Field structure for the table is the key to getting data into buf
  in a manner that will allow the server to understand it.

  @details
  Called from filesort.cc, records.cc, sql_handler.cc, sql_select.cc, sql_table.cc,
  and sql_update.cc.

  @see
  filesort.cc, records.cc, sql_handler.cc, sql_select.cc, sql_table.cc and sql_update.cc
*/
int ha_cfl::rnd_next(uchar *buf)
{
  int rc = 0;
  DBUG_ENTER("ha_cfl::rnd_next");
  MYSQL_READ_ROW_START(table_share->db.str, table_share->table_name.str,
                       TRUE);


  bool over;
  int r;
  r = fetch_next(over);
  if (r < 0)
  {
    //出现错误
  }

  if (over)
  {
    //没有更多的数据
    rc= HA_ERR_END_OF_FILE;
    //更新cfl_rnd的
    MYSQL_READ_ROW_DONE(rc);
    DBUG_RETURN(rc);
  }

  //获取cfl的记录数据
  uint32_t row_length = 0;
  uint8_t *row = NULL;

  my_bitmap_map *org_bitmap;
  bool read_all;
  /* We must read all columns in case a table is opened for update */
  read_all= !bitmap_is_clear_all(table->write_set);
  /* Avoid asserts in ::store() for columns that are not going to be updated */
  org_bitmap= dbug_tmp_use_all_columns(table, table->write_set);

  memset(buf, 0, table->s->null_bytes);
  row = cfl_cursor_row_get(cursor_);
  row_length = cfl_cursor_row_length_get(cursor_);
  //将cfl的记录数据转换为mysql的记录
  cfl_row_to_mysql(table->field, buf, NULL, row, row_length);

  dbug_tmp_restore_column_map(table->write_set, org_bitmap);

  DBUG_RETURN(rc);
}


/**
  @brief
  position() is called after each call to rnd_next() if the data needs
  to be ordered. You can do something like the following to store
  the position:
  @code
  my_store_ptr(ref, ref_length, current_position);
  @endcode

  @details
  The server uses ref to store data. ref_length in the above case is
  the size needed to store current_position. ref is just a byte array
  that the server will maintain. If you are using offsets to mark rows, then
  current_position should be the offset. If it is a primary key like in
  BDB, then it needs to be a primary key.

  Called from filesort.cc, sql_select.cc, sql_delete.cc, and sql_update.cc.

  @see
  filesort.cc, sql_select.cc, sql_delete.cc and sql_update.cc
*/
void ha_cfl::position(const uchar *record)
{
  DBUG_ENTER("ha_cfl::position");
  DBUG_VOID_RETURN;
}


/**
  @brief
  This is like rnd_next, but you are given a position to use
  to determine the row. The position will be of the type that you stored in
  ref. You can use ha_get_ptr(pos,ref_length) to retrieve whatever key
  or position you saved when position() was called.

  @details
  Called from filesort.cc, records.cc, sql_insert.cc, sql_select.cc, and sql_update.cc.

  @see
  filesort.cc, records.cc, sql_insert.cc, sql_select.cc and sql_update.cc
*/
int ha_cfl::rnd_pos(uchar *buf, uchar *pos)
{
  int rc;
  DBUG_ENTER("ha_cfl::rnd_pos");
  MYSQL_READ_ROW_START(table_share->db.str, table_share->table_name.str,
                       TRUE);
  rc= HA_ERR_WRONG_COMMAND;
  MYSQL_READ_ROW_DONE(rc);
  DBUG_RETURN(rc);
}


/**
  @brief
  ::info() is used to return information to the optimizer. See my_base.h for
  the complete description.

  @details
  Currently this table handler doesn't implement most of the fields really needed.
  SHOW also makes use of this data.

  You will probably want to have the following in your code:
  @code
  if (records < 2)
    records = 2;
  @endcode
  The reason is that the server will optimize for cases of only a single
  record. If, in a table scan, you don't know the number of records, it
  will probably be better to set records to two so you can return as many
  records as you need. Along with records, a few more variables you may wish
  to set are:
    records
    deleted
    data_file_length
    index_file_length
    delete_length
    check_time
  Take a look at the public variables in handler.h for more information.

  Called in filesort.cc, ha_heap.cc, item_sum.cc, opt_sum.cc, sql_delete.cc,
  sql_delete.cc, sql_derived.cc, sql_select.cc, sql_select.cc, sql_select.cc,
  sql_select.cc, sql_select.cc, sql_show.cc, sql_show.cc, sql_show.cc, sql_show.cc,
  sql_table.cc, sql_union.cc, and sql_update.cc.

  @see
  filesort.cc, ha_heap.cc, item_sum.cc, opt_sum.cc, sql_delete.cc, sql_delete.cc,
  sql_derived.cc, sql_select.cc, sql_select.cc, sql_select.cc, sql_select.cc,
  sql_select.cc, sql_show.cc, sql_show.cc, sql_show.cc, sql_show.cc, sql_table.cc,
  sql_union.cc and sql_update.cc
*/
int ha_cfl::info(uint flag)
{
  DBUG_ENTER("ha_cfl::info");
  statistic();
  DBUG_RETURN(0);
}


/**
  @brief
  extra() is called whenever the server wishes to send a hint to
  the storage engine. The myisam engine implements the most hints.
  ha_innodb.cc has the most exhaustive list of these hints.

    @see
  ha_innodb.cc
*/
int ha_cfl::extra(enum ha_extra_function operation)
{
  DBUG_ENTER("ha_cfl::extra");
  DBUG_RETURN(0);
}


/**
  @brief
  Used to delete all rows in a table, including cases of truncate and cases where
  the optimizer realizes that all rows will be removed as a result of an SQL statement.

  @details
  Called from item_sum.cc by Item_func_group_concat::clear(),
  Item_sum_count_distinct::clear(), and Item_func_group_concat::clear().
  Called from sql_delete.cc by mysql_delete().
  Called from sql_select.cc by JOIN::reinit().
  Called from sql_union.cc by st_select_lex_unit::exec().

  @see
  Item_func_group_concat::clear(), Item_sum_count_distinct::clear() and
  Item_func_group_concat::clear() in item_sum.cc;
  mysql_delete() in sql_delete.cc;
  JOIN::reinit() in sql_select.cc and
  st_select_lex_unit::exec() in sql_union.cc.
*/
int ha_cfl::delete_all_rows()
{
  DBUG_ENTER("ha_cfl::delete_all_rows");
  DBUG_RETURN(HA_ERR_WRONG_COMMAND);
}


/**
  @brief
  Used for handler specific truncate table.  The table is locked in
  exclusive mode and handler is responsible for reseting the auto-
  increment counter.

  @details
  Called from Truncate_statement::handler_truncate.
  Not used if the handlerton supports HTON_CAN_RECREATE, unless this
  engine can be used as a partition. In this case, it is invoked when
  a particular partition is to be truncated.

  @see
  Truncate_statement in sql_truncate.cc
  Remarks in handler::truncate.
*/
int ha_cfl::truncate()
{
  DBUG_ENTER("ha_cfl::truncate");
  DBUG_RETURN(HA_ERR_WRONG_COMMAND);
}


/**
  @brief
  This create a lock on the table. If you are implementing a storage engine
  that can handle transacations look at ha_berkely.cc to see how you will
  want to go about doing this. Otherwise you should consider calling flock()
  here. Hint: Read the section "locking functions for mysql" in lock.cc to understand
  this.

  @details
  Called from lock.cc by lock_external() and unlock_external(). Also called
  from sql_table.cc by copy_data_between_tables().

  @see
  lock.cc by lock_external() and unlock_external() in lock.cc;
  the section "locking functions for mysql" in lock.cc;
  copy_data_between_tables() in sql_table.cc.
*/
int ha_cfl::external_lock(THD *thd, int lock_type)
{
  DBUG_ENTER("ha_cfl::external_lock");
  DBUG_RETURN(0);
}


/**
  @brief
  The idea with handler::store_lock() is: The statement decides which locks
  should be needed for the table. For updates/deletes/inserts we get WRITE
  locks, for SELECT... we get read locks.

  @details
  Before adding the lock into the table lock handler (see thr_lock.c),
  mysqld calls store lock with the requested locks. Store lock can now
  modify a write lock to a read lock (or some other lock), ignore the
  lock (if we don't want to use MySQL table locks at all), or add locks
  for many tables (like we do when we are using a MERGE handler).

  Berkeley DB, for example, changes all WRITE locks to TL_WRITE_ALLOW_WRITE
  (which signals that we are doing WRITES, but are still allowing other
  readers and writers).

  When releasing locks, store_lock() is also called. In this case one
  usually doesn't have to do anything.

  In some exceptional cases MySQL may send a request for a TL_IGNORE;
  This means that we are requesting the same lock as last time and this
  should also be ignored. (This may happen when someone does a flush
  table when we have opened a part of the tables, in which case mysqld
  closes and reopens the tables and tries to get the same locks at last
  time). In the future we will probably try to remove this.

  Called from lock.cc by get_lock_data().

  @note
  In this method one should NEVER rely on table->in_use, it may, in fact,
  refer to a different thread! (this happens if get_lock_data() is called
  from mysql_lock_abort_for_thread() function)

  @see
  get_lock_data() in lock.cc
*/
THR_LOCK_DATA **ha_cfl::store_lock(THD *thd,
                                       THR_LOCK_DATA **to,
                                       enum thr_lock_type lock_type)
{
  if (lock_type != TL_IGNORE && lock.type == TL_UNLOCK)
    lock.type=lock_type;
  *to++= &lock;
  return to;
}


/**
  @brief
  Used to delete a table. By the time delete_table() has been called all
  opened references to this table will have been closed (and your globally
  shared references released). The variable name will just be the name of
  the table. You will need to remove any files you have created at this point.

  @details
  If you do not implement this, the default delete_table() is called from
  handler.cc and it will delete all files with the file extensions returned
  by bas_ext().

  Called from handler.cc by delete_table and ha_create_table(). Only used
  during create if the table_flag HA_DROP_BEFORE_CREATE was specified for
  the storage engine.

  @see
  delete_table and ha_create_table() in handler.cc
*/
int ha_cfl::delete_table(const char *name)
{
  DBUG_ENTER("ha_cfl::delete_table");

  CflTable::DestroyStorage(name);

  DBUG_RETURN(0);
}


/**
  @brief
  Renames a table from one name to another via an alter table call.

  @details
  If you do not implement this, the default rename_table() is called from
  handler.cc and it will delete all files with the file extensions returned
  by bas_ext().

  Called from sql_table.cc by mysql_rename_table().

  @see
  mysql_rename_table() in sql_table.cc
*/
int ha_cfl::rename_table(const char * from, const char * to)
{
  DBUG_ENTER("ha_cfl::rename_table ");
  DBUG_RETURN(HA_ERR_WRONG_COMMAND);
}


/**
  @brief
  Given a starting key and an ending key, estimate the number of rows that
  will exist between the two keys.

  @details
  end_key may be empty, in which case determine if start_key matches any rows.

  Called from opt_range.cc by check_quick_keys().

  @see
  check_quick_keys() in opt_range.cc
*/
ha_rows ha_cfl::records_in_range(uint inx, key_range *min_key,
                                     key_range *max_key)
{
  DBUG_ENTER("ha_cfl::records_in_range");
  DBUG_RETURN(1);
}


/**
  @brief
  create() is called to create a database. The variable name will have the name
  of the table.

  @details
  When create() is called you do not need to worry about
  opening the table. Also, the .frm file will have already been
  created so adjusting create_info is not necessary. You can overwrite
  the .frm file at this point if you wish to change the table
  definition, but there are no methods currently provided for doing
  so.

  Called from handle.cc by ha_create_table().

  @see
  ha_create_table() in handle.cc
*/

int ha_cfl::create(const char *name, TABLE *table_arg,
                       HA_CREATE_INFO *create_info)
{
  DBUG_ENTER("ha_cfl::create");

  int r = cfl_check_create_info(table_arg, create_info);
  if (r != 0)
  {
    DBUG_RETURN(r);
  }

  r = CflTable::CreateStorage(name);
  if (r < 0)
  {
  }

  DBUG_RETURN(0);
}


struct st_mysql_storage_engine cfl_storage_engine=
{ MYSQL_HANDLERTON_INTERFACE_VERSION };

static ulong srv_enum_var= 0;
static ulong srv_ulong_var= 0;
static double srv_double_var= 0;

const char *enum_var_names[]=
{
  "e1", "e2", NullS
};

TYPELIB enum_var_typelib=
{
  array_elements(enum_var_names) - 1, "enum_var_typelib",
  enum_var_names, NULL
};

static MYSQL_SYSVAR_ENUM(
  enum_var,                       // name
  srv_enum_var,                   // varname
  PLUGIN_VAR_RQCMDARG,            // opt
  "Sample ENUM system variable.", // comment
  NULL,                           // check
  NULL,                           // update
  0,                              // def
  &enum_var_typelib);             // typelib

static MYSQL_SYSVAR_ULONG(
  ulong_var,
  srv_ulong_var,
  PLUGIN_VAR_RQCMDARG,
  "0..1000",
  NULL,
  NULL,
  8,
  0,
  1000,
  0);

static MYSQL_SYSVAR_DOUBLE(
  double_var,
  srv_double_var,
  PLUGIN_VAR_RQCMDARG,
  "0.500000..1000.500000",
  NULL,
  NULL,
  8.5,
  0.5,
  1000.5,
  0);                             // reserved always 0

static MYSQL_THDVAR_DOUBLE(
  double_thdvar,
  PLUGIN_VAR_RQCMDARG,
  "0.500000..1000.500000",
  NULL,
  NULL,
  8.5,
  0.5,
  1000.5,
  0);

static struct st_mysql_sys_var* cfl_variables[]= {
  MYSQL_SYSVAR(enum_var),
  MYSQL_SYSVAR(ulong_var),
  MYSQL_SYSVAR(double_var),
  MYSQL_SYSVAR(double_thdvar),
  NULL
};

// this is an example of SHOW_FUNC and of my_snprintf() service
static int show_func_example(MYSQL_THD thd, struct st_mysql_show_var *var,
                             char *buf)
{
  var->type= SHOW_CHAR;
  var->value= buf; // it's of SHOW_VAR_FUNC_BUFF_SIZE bytes
  my_snprintf(buf, SHOW_VAR_FUNC_BUFF_SIZE,
              "enum_var is %lu, ulong_var is %lu, "
              "double_var is %f, %.6b", // %b is a MySQL extension
              srv_enum_var, srv_ulong_var, srv_double_var, "really");
  return 0;
}

static struct st_mysql_show_var cfl_status[]=
{
  {"example_func_example",  (char *)show_func_example, SHOW_FUNC},
  {0,0,SHOW_UNDEF}
};

mysql_declare_plugin(cfl)
{
  MYSQL_STORAGE_ENGINE_PLUGIN,
  &cfl_storage_engine,
  "CFL",
  "Jiangyx, Ctrip",
  "ctrip fast log storage engine",
  PLUGIN_LICENSE_GPL,
  cfl_init_func,                            /* Plugin Init */
  cfl_deinit_func,                          /* Plugin Deinit */
  0x0001,                                   /* 0.1 */
  cfl_status,                               /* status variables */
  cfl_variables,                            /* system variables */
  NULL,                                     /* config options */
  0,                                        /* flags */
}
mysql_declare_plugin_end;


/*cfl private handler function*/
int
ha_cfl::fetch_next(bool &over)
{
  //用于测试，可删除
  /*if (cfl_cursor_position_get(cursor_) == 1)
  {
    over = true;
    return 0;
    }*/

  /*
    首先定位下一条记录，然后获取记录相关
  */
  //定位下一条记录
  int r;
  r = next(over);
  if (r < 0)
  {
  }

  if (over)
  {
    return 0;
  }

  //获取记录相关信息
  r = fetch();
  if (r < 0)
  {
  }

  return 0;
}

int
ha_cfl::next(bool &over)
{
  uint32_t page_no;
  uint32_t row_no;
  CflPage *page = NULL;
  /*
    算法说明
      page_no和row_no进行下一行的定位标识
      进入next的时候存在3种情况
        1、从未获取过记录，此时cursor的postion为CFL_CURSOR_BEFOR_START
        2、所有记录已经获取，此时cursor的postion为CFL_CURSOR_AFTER_END
        3、获取其中记录。此时cursor的postion为获取的记录数
    实现说明
      ClfPage的申请和释放要注意，不要造成未释放的页面存在
  */

  over = false;
  if (cfl_cursor_position_get(cursor_) == CFL_CURSOR_BEFOR_START)
  {
    /*情况1处理*/
    page_no = 0;
    row_no = 0;
    page = CflPageManager::GetPage(cfl_table_->GetStorage(), page_no);
    //尝试获取第一页，如果不存在，则设为CFL_CURSOR_AFTER_END
    //todo : 也有可能页面不足。需要改进pagepool的调度算法
    if (page == NULL)
    {
      over = true;
      cfl_cursor_position_set(cursor_, CFL_CURSOR_AFTER_END);
      return 0;
    }
    cfl_cursor_page_set(cursor_, page);
    cfl_cursor_position_set(cursor_, 1);
  }
  else if (cfl_cursor_position_get(cursor_) == CFL_CURSOR_AFTER_END)
  {
    /*情况2处理*/
    over = true;
    return 0;
  }
  else
  {
    /*情况3处理*/
    //获取下一条记录的位置信息
    page_no = cfl_cursor_page_no_get(cursor_);
    page = cfl_cursor_page_get(cursor_);
    row_no = cfl_cursor_row_no_get(cursor_);
    row_no++;
    uint64_t curpos = cfl_cursor_position_get(cursor_);
    curpos++;
    cfl_cursor_position_set(cursor_, curpos);
  }

  /*
    有如下情况
    1、在当前页找到数据.成功返回
    2、记录可能在下一页。此时又存在两种情况
      2-1、下一页存在。如果记录页存在，其中必有记录，next一定会获得一条记录
      2-2、下一页不存在。说明已经完成所有记录的next，设置cursor状态为CFL_CURSOR_AFTER_END
  */
  DBUG_ASSERT(page != NULL);
  void *buffer = page->page();
  uint32_t row_count = cfl_page_read_row_count(buffer);
  if (row_no < row_count)
  {
    //获取行
    cfl_cursor_row_no_set(cursor_, row_no);    
  }
  else
  {
    //行不在当前页，释放cursor当前的页，取得下一页
    CflPageManager::PutPage(page);
    page_no++;
    row_no = 0;
    page = CflPageManager::GetPage(cfl_table_->GetStorage(), page_no);
    //后续页是否存在
    if (page != NULL)
    {
      //获取数据
      cfl_cursor_page_set(cursor_, page);
      cfl_cursor_page_no_set(cursor_, page_no);
      cfl_cursor_row_no_set(cursor_, row_no);
    }
    else
    {
      cfl_cursor_position_set(cursor_, CFL_CURSOR_AFTER_END);
      cfl_cursor_page_no_set(cursor_, 0);
      cfl_cursor_row_no_set(cursor_, 0);
      over = true;
    }
  }

  return 0;
}

int
ha_cfl::fetch()
{
  uint32_t row_no, row_count;
  CflPage  *page;
  uint8_t  *buffer;
  uint8_t  *nth_off;
  uint8_t  *row;
  uint32_t offset;

  page = cfl_cursor_page_get(cursor_);
  DBUG_ASSERT(page != NULL);

  buffer = (uint8_t*)page->page();
  row_count = cfl_page_read_row_count(buffer);

  row_no = cfl_cursor_row_no_get(cursor_);
  DBUG_ASSERT(row_no < row_count);

  nth_off = cfl_page_nth_row_offset(buffer, row_no);
  offset = cfl_page_read_row_offset(nth_off);
  row = buffer + offset;

  uint32_t offset_next;
  nth_off = cfl_page_nth_row_offset(buffer, row_no + 1);
  offset_next = cfl_page_read_row_offset(nth_off);

  cfl_cursor_row_set(cursor_, row);
  cfl_cursor_row_length_set(cursor_, offset_next - offset);

  return 0;
}

int
ha_cfl::index_init(uint idx, bool sorted)
{
  DBUG_ENTER("ha_cfl::index_init");

  DBUG_ASSERT(idx == 0);
  active_index= idx;

  DBUG_RETURN(0);
}

int
ha_cfl::index_end()
{
  DBUG_ENTER("ha_cfl::index_end");
  DBUG_RETURN(0);
}

int
ha_cfl::index_read(uchar * buf, const uchar * key, uint key_len,
                   enum ha_rkey_function find_flag)
{
  DBUG_ENTER("ha_cfl::index_read");
  my_bitmap_map *old_map;
  int rc = 0;

  DBUG_ASSERT(table->key_info != NULL);
  KEY *index = table->key_info + active_index;

  //longlong dtpack = my_datetime_packed_from_binary(key, 6);
  //MYSQL_TIME ltime;
  //TIME_from_longlong_datetime_packed(&ltime, dtpack);
  struct timeval tv;
  cfl_dt_t cdt;
  my_timestamp_from_binary(&tv, key, 6);
  cfl_tv2cdt(cdt, tv);
  cfl_dti_t dti;
  dti = cfl_t2i(&cdt);

  old_map= dbug_tmp_use_all_columns(table, table->write_set);

  switch (find_flag)
  {
  case HA_READ_KEY_EXACT:
  {
    break;
  }
  case HA_READ_AFTER_KEY:
  {
    break;
  }
  default:
  {
    DBUG_ASSERT(false);
  }
  }

  dbug_tmp_restore_column_map(table->write_set, old_map);
  DBUG_RETURN(rc);
}

int
ha_cfl::index_next(uchar *buf)
{
  int rc = 0;
  DBUG_ENTER("ha_cfl::index_next");
  //MYSQL_INDEX_READ_ROW_START(table_share->db.str, table_share->table_name.str);
  bool over = true;
  if (over)
  {
    //没有更多的数据
    rc= HA_ERR_END_OF_FILE;
    //更新cfl_rnd的
    MYSQL_READ_ROW_DONE(rc);
    DBUG_RETURN(rc);
  }

  DBUG_RETURN(rc);
}

/**
  @brief
*/

int ha_cfl::index_prev(uchar *buf)
{
  int rc;
  DBUG_ENTER("ha_cfl::index_prev");
  MYSQL_INDEX_READ_ROW_START(table_share->db.str, table_share->table_name.str);
  rc= HA_ERR_WRONG_COMMAND;
  MYSQL_INDEX_READ_ROW_DONE(rc);
  DBUG_RETURN(rc);
}


/**
  @brief
  index_first() asks for the first key in the index.

  @details
  Called from opt_range.cc, opt_sum.cc, sql_handler.cc, and sql_select.cc.

  @see
  opt_range.cc, opt_sum.cc, sql_handler.cc and sql_select.cc
*/
int ha_cfl::index_first(uchar *buf)
{
  int rc;
  DBUG_ENTER("ha_cfl::index_first");
  MYSQL_INDEX_READ_ROW_START(table_share->db.str, table_share->table_name.str);
  rc= HA_ERR_WRONG_COMMAND;
  MYSQL_INDEX_READ_ROW_DONE(rc);
  DBUG_RETURN(rc);
}


/**
  @brief
  index_last() asks for the last key in the index.

  @details
  Called from opt_range.cc, opt_sum.cc, sql_handler.cc, and sql_select.cc.

  @see
  opt_range.cc, opt_sum.cc, sql_handler.cc and sql_select.cc
*/
int ha_cfl::index_last(uchar *buf)
{
  int rc;
  DBUG_ENTER("ha_cfl::index_last");
  MYSQL_INDEX_READ_ROW_START(table_share->db.str, table_share->table_name.str);
  rc= HA_ERR_WRONG_COMMAND;
  MYSQL_INDEX_READ_ROW_DONE(rc);
  DBUG_RETURN(rc);
}

void
ha_cfl::statistic()
{
  //全部信息为测试信息
  stats.records = 100;
  stats.deleted = 0;
  stats.mean_rec_length = 20;
  stats.create_time = 0;
  stats.check_time = 0;
  stats.update_time = 0;
  stats.block_size = 0;
  //
  stats.data_file_length = 10000000;
  stats.index_file_length = 10000000;

  //仿照
  stats.data_file_length = 327680;
  stats.max_data_file_length = 0;
  stats.index_file_length = 163840;
  stats.max_index_file_length = (ulonglong)10344644715844964239;
  stats.delete_length = 0;
  stats.auto_increment_value = 0;
  stats.records = 100;
  stats.deleted = 0;
  stats.mean_rec_length = 40;
  stats.create_time = 0;
  stats.check_time = 0;
  stats.update_time = 0;
  stats.block_size = 16384;
  stats.mrr_length_per_rec = 14;
}

