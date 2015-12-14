.. Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.


==========
Repository
==========

This repository contains additional information regarding Sqoop.


Sqoop Schema
------------

The DDL queries that create the Sqoop repository schema in Derby database create the following tables:



SQ_SYSTEM
+++++++++
Store for various state information

      +----------------------------+
      | SQ_SYSTEM                  |
      +============================+
      | SQM_ID: BIGINT PK          |
      +----------------------------+
      | SQM_KEY: VARCHAR(64)       |
      +----------------------------+
      | SQM_VALUE: VARCHAR(64)     |
      +----------------------------+




SQ_DIRECTION
++++++++++++
Directions

      +---------------------------------------+-------------+
      | SQ_DIRECTION                          |             |
      +=======================================+=============+
      | SQD_ID: BIGINT PK AUTO-GEN            |             |
      +---------------------------------------+-------------+
      | SQD_NAME: VARCHAR(64)                 | "FROM"|"TO" |
      +---------------------------------------+-------------+




SQ_CONFIGURABLE
+++++++++++++++
Configurable registration

      +-----------------------------+----------------------+
      | SQ_CONFIGURABLE             |                      |
      +=============================+======================+
      | SQC_ID: BIGINT PK AUTO-GEN  |                      |
      +-----------------------------+----------------------+
      | SQC_NAME: VARCHAR(64)       |                      |
      +-----------------------------+----------------------+
      | SQC_CLASS: VARCHAR(255)     |                      |
      +-----------------------------+----------------------+
      | SQC_TYPE: VARCHAR(32)       | "CONNECTOR"|"DRIVER" |
      +-----------------------------+----------------------+
      | SQC_VERSION: VARCHAR(64)    |                      |
      +-----------------------------+----------------------+




SQ_CONNECTOR_DIRECTIONS
+++++++++++++++++++++++
Connector directions

      +------------------------------+------------------------------+
      | SQ_CONNECTOR_DIRECTIONS      |                              |
      +==============================+==============================+
      | SQCD_ID: BIGINT PK AUTO-GEN  |                              |
      +------------------------------+------------------------------+
      | SQCD_CONNECTOR: BIGINT       | FK SQCD_CONNECTOR(SQC_ID)    |
      +------------------------------+------------------------------+
      | SQCD_DIRECTION: BIGINT       | FK SQCD_DIRECTION(SQD_ID)    |
      +------------------------------+------------------------------+




SQ_CONFIG
+++++++++
Config details

      +-------------------------------------+------------------------------------------------------+
      | SQ_CONFIG                           |                                                      |
      +=====================================+======================================================+
      | SQ_CFG_ID: BIGINT PK AUTO-GEN       |                                                      |
      +-------------------------------------+------------------------------------------------------+
      | SQ_CFG_CONNECTOR: BIGINT            | FK SQ_CFG_CONNECTOR(SQC_ID), NULL for driver         |
      +-------------------------------------+------------------------------------------------------+
      | SQ_CFG_NAME: VARCHAR(64)            |                                                      |
      +-------------------------------------+------------------------------------------------------+
      | SQ_CFG_TYPE: VARCHAR(32)            | "LINK"|"JOB"                                         |
      +-------------------------------------+------------------------------------------------------+
      | SQ_CFG_INDEX: SMALLINT              |                                                      |
      +-------------------------------------+------------------------------------------------------+




SQ_CONFIG_DIRECTIONS
++++++++++++++++++++
Connector directions

      +------------------------------+------------------------------+
      | SQ_CONNECTOR_DIRECTIONS      |                              |
      +==============================+==============================+
      | SQCD_ID: BIGINT PK AUTO-GEN  |                              |
      +------------------------------+------------------------------+
      | SQCD_CONFIG: BIGINT          | FK SQCD_CONFIG(SQ_CFG_ID)    |
      +------------------------------+------------------------------+
      | SQCD_DIRECTION: BIGINT       | FK SQCD_DIRECTION(SQD_ID)    |
      +------------------------------+------------------------------+




SQ_INPUT
++++++++
Input details

      +----------------------------+--------------------------+
      | SQ_INPUT                   |                          |
      +============================+==========================+
      | SQI_ID: BIGINT PK AUTO-GEN |                          |
      +----------------------------+--------------------------+
      | SQI_NAME: VARCHAR(64)      |                          |
      +----------------------------+--------------------------+
      | SQI_CONFIG: BIGINT         | FK SQ_CONFIG(SQ_CFG_ID)  |
      +----------------------------+--------------------------+
      | SQI_INDEX: SMALLINT        |                          |
      +----------------------------+--------------------------+
      | SQI_TYPE: VARCHAR(32)      | "STRING"|"MAP"           |
      +----------------------------+--------------------------+
      | SQI_STRMASK: BOOLEAN       |                          |
      +----------------------------+--------------------------+
      | SQI_STRLENGTH: SMALLINT    |                          |
      +----------------------------+--------------------------+
      | SQI_ENUMVALS: VARCHAR(100) |                          |
      +----------------------------+--------------------------+




SQ_LINK
+++++++
Stored links

      +-----------------------------------+--------------------------+
      | SQ_LINK                           |                          |
      +===================================+==========================+
      | SQ_LNK_ID: BIGINT PK AUTO-GEN     |                          |
      +-----------------------------------+--------------------------+
      | SQ_LNK_NAME: VARCHAR(64)          |                          |
      +-----------------------------------+--------------------------+
      | SQ_LNK_CONNECTOR: BIGINT          | FK SQ_CONNECTOR(SQC_ID)  |
      +-----------------------------------+--------------------------+
      | SQ_LNK_CREATION_USER: VARCHAR(32) |                          |
      +-----------------------------------+--------------------------+
      | SQ_LNK_CREATION_DATE: TIMESTAMP   |                          |
      +-----------------------------------+--------------------------+
      | SQ_LNK_UPDATE_USER: VARCHAR(32)   |                          |
      +-----------------------------------+--------------------------+
      | SQ_LNK_UPDATE_DATE: TIMESTAMP     |                          |
      +-----------------------------------+--------------------------+
      | SQ_LNK_ENABLED: BOOLEAN           |                          |
      +-----------------------------------+--------------------------+




SQ_JOB
++++++
Stored jobs

      +--------------------------------+-----------------------+
      | SQ_JOB                         |                       |
      +================================+=======================+
      | SQB_ID: BIGINT PK AUTO-GEN     |                       |
      +--------------------------------+-----------------------+
      | SQB_NAME: VARCHAR(64)          |                       |
      +--------------------------------+-----------------------+
      | SQB_FROM_LINK: BIGINT          | FK SQ_LINK(SQ_LNK_ID) |
      +--------------------------------+-----------------------+
      | SQB_TO_LINK: BIGINT            | FK SQ_LINK(SQ_LNK_ID) |
      +--------------------------------+-----------------------+
      | SQB_CREATION_USER: VARCHAR(32) |                       |
      +--------------------------------+-----------------------+
      | SQB_CREATION_DATE: TIMESTAMP   |                       |
      +--------------------------------+-----------------------+
      | SQB_UPDATE_USER: VARCHAR(32)   |                       |
      +--------------------------------+-----------------------+
      | SQB_UPDATE_DATE: TIMESTAMP     |                       |
      +--------------------------------+-----------------------+
      | SQB_ENABLED: BOOLEAN           |                       |
      +--------------------------------+-----------------------+




SQ_LINK_INPUT
+++++++++++++
N:M relationship link and input

      +----------------------------+-----------------------+
      | SQ_LINK_INPUT              |                       |
      +============================+=======================+
      | SQ_LNKI_LINK: BIGINT PK    | FK SQ_LINK(SQ_LNK_ID) |
      +----------------------------+-----------------------+
      | SQ_LNKI_INPUT: BIGINT PK   | FK SQ_INPUT(SQI_ID)   |
      +----------------------------+-----------------------+
      | SQ_LNKI_VALUE: LONG VARCHAR|                       |
      +----------------------------+-----------------------+




SQ_JOB_INPUT
++++++++++++
N:M relationship job and input

      +----------------------------+---------------------+
      | SQ_JOB_INPUT               |                     |
      +============================+=====================+
      | SQBI_JOB: BIGINT PK        | FK SQ_JOB(SQB_ID)   |
      +----------------------------+---------------------+
      | SQBI_INPUT: BIGINT PK      | FK SQ_INPUT(SQI_ID) |
      +----------------------------+---------------------+
      | SQBI_VALUE: LONG VARCHAR   |                     |
      +----------------------------+---------------------+




SQ_SUBMISSION
+++++++++++++
List of submissions

      +-----------------------------------+-------------------+
      | SQ_JOB_SUBMISSION                 |                   |
      +===================================+===================+
      | SQS_ID: BIGINT PK                 |                   |
      +-----------------------------------+-------------------+
      | SQS_JOB: BIGINT                   | FK SQ_JOB(SQB_ID) |
      +-----------------------------------+-------------------+
      | SQS_STATUS: VARCHAR(20)           |                   |
      +-----------------------------------+-------------------+
      | SQS_CREATION_USER: VARCHAR(32)    |                   |
      +-----------------------------------+-------------------+
      | SQS_CREATION_DATE: TIMESTAMP      |                   |
      +-----------------------------------+-------------------+
      | SQS_UPDATE_USER: VARCHAR(32)      |                   |
      +-----------------------------------+-------------------+
      | SQS_UPDATE_DATE: TIMESTAMP        |                   |
      +-----------------------------------+-------------------+
      | SQS_EXTERNAL_ID: VARCHAR(50)      |                   |
      +-----------------------------------+-------------------+
      | SQS_EXTERNAL_LINK: VARCHAR(150)   |                   |
      +-----------------------------------+-------------------+
      | SQS_EXCEPTION: VARCHAR(150)       |                   |
      +-----------------------------------+-------------------+
      | SQS_EXCEPTION_TRACE: VARCHAR(750) |                   |
      +-----------------------------------+-------------------+




SQ_COUNTER_GROUP
++++++++++++++++
List of counter groups

      +----------------------------+
      | SQ_COUNTER_GROUP           |
      +============================+
      | SQG_ID: BIGINT PK          |
      +----------------------------+
      | SQG_NAME: VARCHAR(75)      |
      +----------------------------+




SQ_COUNTER
++++++++++
List of counters

      +----------------------------+
      | SQ_COUNTER                 |
      +============================+
      | SQR_ID: BIGINT PK          |
      +----------------------------+
      | SQR_NAME: VARCHAR(75)      |
      +----------------------------+




SQ_COUNTER_SUBMISSION
+++++++++++++++++++++
N:M Relationship

      +----------------------------+--------------------------------+
      | SQ_COUNTER_SUBMISSION      |                                |
      +============================+================================+
      | SQRS_GROUP: BIGINT PK      | FK SQ_COUNTER_GROUP(SQR_ID)    |
      +----------------------------+--------------------------------+
      | SQRS_COUNTER: BIGINT PK    | FK SQ_COUNTER(SQR_ID)          |
      +----------------------------+--------------------------------+
      | SQRS_SUBMISSION: BIGINT PK | FK SQ_SUBMISSION(SQS_ID)       |
      +----------------------------+--------------------------------+
      | SQRS_VALUE: BIGINT         |                                |
      +----------------------------+--------------------------------+
