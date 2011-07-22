/**
 * Licensed to Cloudera, Inc. under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Cloudera, Inc. licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cloudera.sqoop.util;

import java.io.IOException;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import com.cloudera.sqoop.manager.ImportJobContext;
import com.cloudera.sqoop.SqoopOptions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Utilities used when appending imported files to an existing dir.
 */
public class AppendUtils {

  public static final Log LOG = LogFactory.getLog(AppendUtils.class.getName());

  private static final SimpleDateFormat DATE_FORM = new SimpleDateFormat(
      "ddHHmmssSSS");
  private static final String TEMP_IMPORT_ROOT = "_sqoop";

  private static final int PARTITION_DIGITS = 5;
  private static final String FILEPART_SEPARATOR = "-";
  private static final String FILEEXT_SEPARATOR = ".";

  private ImportJobContext context = null;

  public AppendUtils(ImportJobContext context) {
    this.context = context;
  }

  /**
   * Moves the imported files from temporary directory to specified target-dir,
   * renaming partition number if appending file exists.
   */
  public void append() throws IOException {

    SqoopOptions options = context.getOptions();
    FileSystem fs = FileSystem.get(options.getConf());
    Path tempDir = context.getDestination();

    // Try in this order: target-dir or warehouse-dir
    Path userDestDir = null;
    if (options.getTargetDir() != null) {
      userDestDir = new Path(options.getTargetDir());
    } else if (options.getWarehouseDir() != null) {
      userDestDir = new Path(options.getWarehouseDir(),
          context.getTableName());
    } else {
      userDestDir = new Path(context.getTableName());
    }

    int nextPartition = 0;

    if (!fs.exists(tempDir)) {
      // This occurs if there was no source (tmp) dir. This might happen
      // if the import was an HBase-target import, but the user specified
      // --append anyway. This is a warning, not an error.
      LOG.warn("Cannot append files to target dir; no such directory: "
          + tempDir);
      return;
    }

    // Create target directory.
    if (!fs.exists(userDestDir)) {
      LOG.info("Creating missing output directory - " + userDestDir.getName());
      fs.mkdirs(userDestDir);
      nextPartition = 0;
    } else {
      LOG.info("Appending to directory " + userDestDir.getName());
      // Get the right next partition for the imported files
      nextPartition = getNextPartition(fs, userDestDir);
    }

    // move files
    moveFiles(fs, tempDir, userDestDir, nextPartition);

    // delete temporary path
    LOG.debug("Deleting temporary folder " + tempDir.getName());
    fs.delete(tempDir, true);
  }

  /**
   * Returns the greatest partition number available for appending, for data
   * files in targetDir.
   */
  private int getNextPartition(FileSystem fs, Path targetDir)
      throws IOException {

    int nextPartition = 0;
    FileStatus[] existingFiles = fs.listStatus(targetDir);
    if (existingFiles != null && existingFiles.length > 0) {
      Pattern patt = Pattern.compile("part.*-([0-9][0-9][0-9][0-9][0-9]).*");
      for (FileStatus fileStat : existingFiles) {
        if (!fileStat.isDir()) {
          String filename = fileStat.getPath().getName();
          Matcher mat = patt.matcher(filename);
          if (mat.matches()) {
            int thisPart = Integer.parseInt(mat.group(1));
            if (thisPart >= nextPartition) {
              nextPartition = thisPart;
              nextPartition++;
            }
          }
        }
      }
    }

    if (nextPartition > 0) {
      LOG.info("Using found partition " + nextPartition);
    }

    return nextPartition;
  }

  /**
   * Move files from source to target using a specified starting partition.
   */
  private void moveFiles(FileSystem fs, Path sourceDir, Path targetDir,
      int partitionStart) throws IOException {

    NumberFormat numpart = NumberFormat.getInstance();
    numpart.setMinimumIntegerDigits(PARTITION_DIGITS);
    numpart.setGroupingUsed(false);
    Pattern patt = Pattern.compile("part.*-([0-9][0-9][0-9][0-9][0-9]).*");
    FileStatus[] tempFiles = fs.listStatus(sourceDir);

    if (null == tempFiles) {
      // If we've already checked that the dir exists, and now it can't be
      // listed, this is a genuine error (permissions, fs integrity, or other).
      throw new IOException("Could not list files from " + sourceDir);
    }

    // Move and rename files & directories from temporary to target-dir thus
    // appending file's next partition
    for (FileStatus fileStat : tempFiles) {
      if (!fileStat.isDir()) {
        // Move imported data files
        String filename = fileStat.getPath().getName();
        Matcher mat = patt.matcher(filename);
        if (mat.matches()) {
          String name = getFilename(filename);
          String fileToMove = name.concat(numpart.format(partitionStart++));
          String extension = getFileExtension(filename);
          if (extension != null) {
            fileToMove = fileToMove.concat(extension);
          }
          LOG.debug("Filename: " + filename + " repartitioned to: "
              + fileToMove);
          fs.rename(fileStat.getPath(), new Path(targetDir, fileToMove));
        }
      } else {
        // Move directories (_logs & any other)
        String dirName = fileStat.getPath().getName();
        Path path = new Path(targetDir, dirName);
        int dirNumber = 0;
        while (fs.exists(path)) {
          path = new Path(targetDir, dirName.concat("-").concat(
              numpart.format(dirNumber++)));
        }
        LOG.debug("Directory: " + dirName + " renamed to: " + path.getName());
        fs.rename(fileStat.getPath(), path);
      }
    }
  }

  /** returns the name component of a file. */
  private String getFilename(String filename) {
    String result = null;
    int pos = filename.lastIndexOf(FILEPART_SEPARATOR);
    if (pos != -1) {
      result = filename.substring(0, pos + 1);
    } else {
      pos = filename.lastIndexOf(FILEEXT_SEPARATOR);
      if (pos != -1) {
        result = filename.substring(0, pos);
      } else {
        result = filename;
      }
    }
    return result;
  }

  /** returns the extension component of a filename. */
  private String getFileExtension(String filename) {
    int pos = filename.lastIndexOf(FILEEXT_SEPARATOR);
    if (pos != -1) {
      return filename.substring(pos, filename.length());
    } else {
      return null;
    }
  }

  /**
   * Creates a unique path object inside the sqoop temporary directory.
   * 
   * @param tableName
   * @return a path pointing to the temporary directory
   */
  public static Path getTempAppendDir(String tableName) {
    String timeId = DATE_FORM.format(new Date(System.currentTimeMillis()));
    String tempDir = TEMP_IMPORT_ROOT + Path.SEPARATOR + timeId + tableName;
    return new Path(tempDir);
  }

}
