# Copyright (c) 2006, 2010, Oracle and/or its affiliates. All rights reserved.
# 
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; version 2 of the License.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA  02110-1301 USA

SET(CFL_PLUGIN_DYNAMIC "ha_cfl")
SET(CFL_SOURCES ha_cfl.cc cfl_buffer.cc cfl_page.cc cfl_table.cc cfl_index.cc cfl_data.cc cfl_insert_buffer.cc cfl_row.cc cfl_file.cc)
MYSQL_ADD_PLUGIN(cfl ${CFL_SOURCES} STORAGE_ENGINE MODULE_ONLY)
