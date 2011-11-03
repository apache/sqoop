-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.
CREATE TABLE IF NOT EXISTS `FIELD_WITH_NL_REPLACEMENT_HIVE_IMPORT` ( `DATA_COL0` STRING, `DATA_COL1` INT, `DATA_COL2` STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY '\001' LINES TERMINATED BY '\012' STORED AS TEXTFILE;
LOAD DATA INPATH 'file:BASEPATH/sqoop/warehouse/FIELD_WITH_NL_REPLACEMENT_HIVE_IMPORT' INTO TABLE `FIELD_WITH_NL_REPLACEMENT_HIVE_IMPORT`;
