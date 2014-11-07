-- Licensed to the Apache Software Foundation (ASF) under one or more
-- contributor license agreements.  See the NOTICE file distributed with
-- this work for additional information regarding copyright ownership.
-- The ASF licenses this file to You under the Apache License, Version 2.0
-- (the "License"); you may not use this file except in compliance with
-- the License.  You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

create user sqooptest identified by 12345 default tablespace users;
alter user sqooptest quota unlimited on users;
grant create session to sqooptest;
grant create procedure to sqooptest;
grant alter session to sqooptest;
grant select on v_$instance to sqooptest;
grant select on dba_tables to sqooptest;
grant select on dba_tab_columns to sqooptest;
grant select on dba_objects to sqooptest;
grant select on dba_extents to sqooptest;
grant select on dba_segments to sqooptest;
grant select on dba_constraints to sqooptest;
grant select on v_$database to sqooptest;
grant select on v_$parameter to sqooptest;
grant select on v_$session to sqooptest;
grant select on v_$sql to sqooptest;
grant create table to sqooptest;
grant select on dba_tab_partitions to sqooptest;
grant select on dba_tab_subpartitions to sqooptest;
grant select on dba_indexes to sqooptest;
grant select on dba_ind_columns to sqooptest;
grant select any table to sqooptest;
grant create any table to sqooptest;
grant insert any table to sqooptest;
grant alter any table to sqooptest;

create user sqooptest2 identified by ABCDEF default tablespace users;
alter user sqooptest2 quota unlimited on users;
grant create session to sqooptest2;
grant create procedure to sqooptest2;
grant alter session to sqooptest2;
grant select on v_$instance to sqooptest2;
grant select on dba_tables to sqooptest2;
grant select on dba_tab_columns to sqooptest2;
grant select on dba_objects to sqooptest2;
grant select on dba_extents to sqooptest2;
grant select on dba_segments to sqooptest2;
grant select on dba_constraints to sqooptest2;
grant select on v_$database to sqooptest2;
grant select on v_$parameter to sqooptest2;
grant select on v_$session to sqooptest2;
grant select on v_$sql to sqooptest2;
grant create table to sqooptest2;
grant select on dba_tab_partitions to sqooptest2;
grant select on dba_tab_subpartitions to sqooptest2;
grant select on dba_indexes to sqooptest2;
grant select on dba_ind_columns to sqooptest2;
grant select any table to sqooptest2;
grant create any table to sqooptest2;
grant insert any table to sqooptest2;
grant alter any table to sqooptest2;
