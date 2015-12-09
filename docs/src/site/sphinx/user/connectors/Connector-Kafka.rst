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


===============
Kafka Connector
===============

Currently, only the TO direction is supported.

.. contents::
   :depth: 3

-----
Usage
-----

To use the Kafka Connector, create a link for the connector and a job that uses the link.

**Link Configuration**
++++++++++++++++++++++

Inputs associated with the link configuration include:

+----------------------+---------+-----------------------------------------------------------+-------------------------------------+
| Input                | Type    | Description                                               | Example                             |
+======================+=========+===========================================================+=====================================+
| Broker list          | String  | Comma separated list of kafka brokers.                    | example.com:10000,example.com:11000 |
|                      |         | *Required*.                                               |                                     |
+----------------------+---------+-----------------------------------------------------------+-------------------------------------+
| Zookeeper connection | String  | Comma separated list of zookeeper servers in your quorum. | /etc/conf/hadoop                    |
|                      |         | *Required*.                                               |                                     |
+----------------------+---------+-----------------------------------------------------------+-------------------------------------+

**TO Job Configuration**
++++++++++++++++++++++++

Inputs associated with the Job configuration for the FROM direction include:

+-------+---------+---------------------------------+----------+
| Input | Type    | Description                     | Example  |
+=======+=========+=================================+==========+
| topic | String  | The Kafka topic to transfer to. | my topic |
|       |         | *Required*.                     |          |
+-------+---------+---------------------------------+----------+

------
Loader
------

During the *loading* phase, Kafka is written to directly from each loader. The order in which data is loaded into Kafka is not guaranteed.
