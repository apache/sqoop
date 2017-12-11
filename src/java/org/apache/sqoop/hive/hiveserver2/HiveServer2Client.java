package org.apache.sqoop.hive.hiveserver2;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.FileOutputCommitter;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.sqoop.SqoopOptions;
import org.apache.sqoop.db.JdbcConnectionFactory;
import org.apache.sqoop.hive.HiveClient;
import org.apache.sqoop.hive.TableDefWriter;
import org.apache.sqoop.io.CodecMap;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import static java.util.Arrays.asList;

public class HiveServer2Client implements HiveClient {

  private static final Log LOG = LogFactory.getLog(HiveServer2Client.class.getName());

  private final SqoopOptions sqoopOptions;

  private final TableDefWriter tableDefWriter;

  private final JdbcConnectionFactory hs2ConnectionFactory;

  public HiveServer2Client(SqoopOptions sqoopOptions, TableDefWriter tableDefWriter, JdbcConnectionFactory hs2ConnectionFactory) {
    this.sqoopOptions = sqoopOptions;
    this.tableDefWriter = tableDefWriter;
    this.hs2ConnectionFactory = hs2ConnectionFactory;
  }

  @Override
  public void importTable() throws IOException {
    String createTableStmt = tableDefWriter.getCreateTableStmt();
    String loadDataStmt = tableDefWriter.getLoadDataStmt();
    executeHiveImport(asList(createTableStmt, loadDataStmt));
  }

  @Override
  public void createTable() throws IOException {
    String createTableStmt = tableDefWriter.getCreateTableStmt();
    executeHiveImport(asList(createTableStmt));
  }

  private void executeHiveImport(List<String> commands) throws IOException {
    LOG.debug("Hive.inputTable: " + sqoopOptions.getTableName());
    LOG.debug("Hive.outputTable: " + sqoopOptions.getHiveTableName());

    Path finalPath = tableDefWriter.getFinalPath();

    removeTempLogs(finalPath);

    indexLzoFiles(finalPath);

    try {
      executeCommands(commands);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }

    cleanUp(finalPath);
  }

  private void executeCommands(List<String> commands) throws SQLException {
    try (Connection hs2Connection = hs2ConnectionFactory.createConnection()) {
      for (String command : commands) {
        try (PreparedStatement statement = hs2Connection.prepareStatement(command)) {
          statement.execute();
        } catch (SQLException e) {
          LOG.error("Error executing command", e);
        }
      }
    } catch (SQLException e) {
      LOG.error("Error establishing connection to HiveServer2.", e);
    }
  }

  private void indexLzoFiles(Path finalPath) throws IOException {
    String codec = sqoopOptions.getCompressionCodec();
    if (codec != null && (codec.equals(CodecMap.LZOP)
        || codec.equals(CodecMap.getCodecClassName(CodecMap.LZOP)))) {
      try {
        Tool tool = ReflectionUtils.newInstance(Class.
            forName("com.hadoop.compression.lzo.DistributedLzoIndexer").
            asSubclass(Tool.class), sqoopOptions.getConf());
        ToolRunner.run(sqoopOptions.getConf(), tool,
            new String[]{finalPath.toString()});
      } catch (Exception ex) {
        LOG.error("Error indexing lzo files", ex);
        throw new IOException("Error indexing lzo files", ex);
      }
    }
  }

  private void removeTempLogs(Path tablePath) throws IOException {
    FileSystem fs = tablePath.getFileSystem(sqoopOptions.getConf());
    Path logsPath = new Path(tablePath, "_logs");
    if (fs.exists(logsPath)) {
      LOG.info("Removing temporary files from import process: " + logsPath);
      if (!fs.delete(logsPath, true)) {
        LOG.warn("Could not delete temporary files; "
            + "continuing with import, but it may fail.");
      }
    }
  }

  private void cleanUp(Path outputPath) throws IOException {
    FileSystem fs = outputPath.getFileSystem(sqoopOptions.getConf());

    // HIVE is not always removing input directory after LOAD DATA statement
    // (which is our export directory). We're removing export directory in case
    // that is blank for case that user wants to periodically populate HIVE
    // table (for example with --hive-overwrite).
    try {
      if (outputPath != null && fs.exists(outputPath)) {
        FileStatus[] statuses = fs.listStatus(outputPath);
        if (statuses.length == 0) {
          LOG.info("Export directory is empty, removing it.");
          fs.delete(outputPath, true);
        } else if (statuses.length == 1 && statuses[0].getPath().getName().equals(FileOutputCommitter.SUCCEEDED_FILE_NAME)) {
          LOG.info("Export directory is contains the _SUCCESS file only, removing the directory.");
          fs.delete(outputPath, true);
        } else {
          LOG.info("Export directory is not empty, keeping it.");
        }
      }
    } catch (IOException e) {
      LOG.error("Issue with cleaning (safe to ignore)", e);
    }
  }

}
