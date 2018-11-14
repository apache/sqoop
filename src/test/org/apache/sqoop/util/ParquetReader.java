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

import org.apache.avro.Conversions;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.hadoop.Footer;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.hadoop.util.HiddenFileFilter;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;

import static java.util.Arrays.asList;
import static org.apache.sqoop.util.FileSystemUtil.isFile;
import static org.apache.sqoop.util.FileSystemUtil.listFiles;

public class ParquetReader implements AutoCloseable {

  private final Path pathToRead;

  private final Configuration configuration;

  private final Deque<Path> filesToRead;

  private org.apache.parquet.hadoop.ParquetReader<GenericRecord> reader;

  public ParquetReader(Path pathToRead, Configuration configuration) {
    this.pathToRead = pathToRead;
    this.configuration = configuration;
    this.filesToRead = new ArrayDeque<>(determineFilesToRead());
    initReader(filesToRead.removeFirst());
  }

  public ParquetReader(Path pathToRead) {
    this(pathToRead, new Configuration());
  }

  private GenericRecord next() throws IOException {
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

  public List<String> readAllInCsvSorted() {
    List<String> result = readAllInCsv();
    Collections.sort(result);

    return result;
  }

  public CompressionCodecName getCodec() {
    ParquetMetadata parquetMetadata = getParquetMetadata();

    Iterator<BlockMetaData> blockMetaDataIterator = parquetMetadata.getBlocks().iterator();
    if (blockMetaDataIterator.hasNext()) {
      BlockMetaData blockMetaData = blockMetaDataIterator.next();

      Iterator<ColumnChunkMetaData> columnChunkMetaDataIterator = blockMetaData.getColumns().iterator();

      if (columnChunkMetaDataIterator.hasNext()) {
        ColumnChunkMetaData columnChunkMetaData = columnChunkMetaDataIterator.next();

        return columnChunkMetaData.getCodec();
      }
    }

    return null;
  }

  public MessageType readParquetSchema() {
    try {
      ParquetMetadata parquetMetadata = getParquetMetadata();

      return parquetMetadata.getFileMetaData().getSchema();
    } finally {
      close();
    }
  }

  private ParquetMetadata getParquetMetadata() {
    return getFooters().stream().findFirst().get().getParquetMetadata();
  }

  private List<Footer> getFooters() {
    final List<Footer> footers;
    try {
      FileSystem fs = pathToRead.getFileSystem(configuration);
      List<FileStatus> statuses = asList(fs.listStatus(pathToRead, HiddenFileFilter.INSTANCE));
      footers = ParquetFileReader.readAllFootersInParallelUsingSummaryFiles(configuration, statuses, false);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return footers;
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
      GenericData.get().addLogicalTypeConversion(new Conversions.DecimalConversion());
      this.reader = AvroParquetReader.<GenericRecord>builder(file).withDataModel(GenericData.get()).build();
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
