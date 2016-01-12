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


=================
S3 Import to HDFS
=================

.. contents::
   :depth: 3

This section contains detailed description for example use case of transferring data from S3 to HDFS.

--------
Use case
--------

You have directory on S3 where some external process is creating new text files. New files are added to this directory, but existing files are never altered. They can only be removed after some period of time. Data from all new files needs to be transferred to a single HDFS directory. Preserving file names is not required and multiple source files can be merged to single file on HDFS.

-------------
Configuration
-------------

We will use HDFS connector for both ``From`` and ``To`` sides of the data transfer. In order to create link for S3 you need to have S3 bucket name and S3 access and secret keys. Please follow S3 documentation to retrieve S3 credentials if you don’t have them already.

.. code-block:: bash

  sqoop:000> create link -c hdfs-connector

* Our example uses ``s3link`` for the link name
* Specify HDFS URI in form of ``s3a://$BUCKET_NAME`` where ``$BUCKET_NAME`` is name of the S3 bucket
* Use Override configuration option and specify ``fs.s3a.access.key`` and ``fs.s3a.secret.key`` with your S3 access and secret key respectively.

Next step is to create link for HDFS

.. code-block:: bash

  sqoop:000> create link -c hdfs-connector

Our example uses hdfslink for the link name
If your Sqoop server is running on node that has HDFS and mapreduce client configuration deployed, you can safely keep all options blank and use defaults for them.

With having links for both HDFS and S3, you can create job that will transfer data from S3 to HDFS:

.. code-block:: bash

  sqoop:000> create job -f s3link -t hdfslink

* Our example uses ``s3import`` for the job name
* Input directory should point to a directory inside your S3 bucket where new files are generated
* Make sure to choose mode ``NEW_FILES`` for Incremental type
* Final destination for the imported files can be specified in Output directory
* Make sure to enable Append mode, so that Sqoop can upload newly created files to the same directory on HDFS
* Configure the remaining options as you see fit

Then finally you can start the job by issuing following command:

.. code-block:: bash

  sqoop:000> start job -j s3import

You can run the job ``s3import`` periodically and only newly created files will be transferred.