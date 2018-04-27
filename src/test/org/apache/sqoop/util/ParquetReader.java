/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.sqoop.util;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import parquet.avro.AvroParquetReader;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.List;

import static org.apache.sqoop.util.FileSystemUtil.isFile;
import static org.apache.sqoop.util.FileSystemUtil.listFiles;

public class ParquetReader implements AutoCloseable {

  private final Path pathToRead;

  private final Configuration configuration;

  private final Deque<Path> filesToRead;

  private parquet.hadoop.ParquetReader<GenericRecord> reader;

  public ParquetReader(Path pathToRead, Configuration configuration) {
    this.pathToRead = pathToRead;
    this.configuration = configuration;
    this.filesToRead = new ArrayDeque<>(determineFilesToRead());
    initReader(filesToRead.removeFirst());
  }

  public ParquetReader(Path pathToRead) {
    this(pathToRead, new Configuration());
  }

  public GenericRecord next() throws IOException {
    GenericRecord result = reader.read();
    if (result != null) {
      return result;
    }
    if (!filesToRead.isEmpty()) {
      initReader(filesToRead.removeFirst());
      return next();
    }

    return null;
  }

  public List<GenericRecord> readAll() {
    List<GenericRecord> result = new ArrayList<>();

    GenericRecord record;
    try {
      while ((record = next()) != null) {
        result.add(record);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      close();
    }

    return result;
  }

  public List<String> readAllInCsv() {
    List<String> result = new ArrayList<>();

    for (GenericRecord record : readAll()) {
      result.add(convertToCsv(record));
    }

    return result;
  }

  private String convertToCsv(GenericRecord record) {
    StringBuilder result = new StringBuilder();
    for (int i = 0; i < record.getSchema().getFields().size(); i++) {
      result.append(record.get(i));
      result.append(",");
    }
    result.deleteCharAt(result.length() - 1);
    return result.toString();
  }

  private void initReader(Path file) {
    try {
      if (reader != null) {
        reader.close();
      }
      this.reader = AvroParquetReader.<GenericRecord>builder(file).build();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private Collection<Path> determineFilesToRead() {
    try {
      if (isFile(pathToRead, configuration)) {
        return Collections.singletonList(pathToRead);
      }

      return listFiles(pathToRead, configuration);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() {
    if (reader != null) {
      try {
        reader.close();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
