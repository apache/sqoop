/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
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
package org.apache.sqoop.mapreduce.db;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.sqoop.mapreduce.DBWritable;

import org.apache.sqoop.config.ConfigurationHelper;
import org.apache.sqoop.mapreduce.db.DataDrivenDBRecordReader;
import org.apache.sqoop.validation.ValidationException;

/**
 * A InputFormat that reads input data from an SQL table.
 * Operates like DBInputFormat, but instead of using LIMIT and OFFSET to
 * demarcate splits, it tries to generate WHERE clauses which separate the
 * data into roughly equivalent shards.
 */
public class DataDrivenDBInputFormat<T extends DBWritable>
      extends DBInputFormat<T> implements Configurable  {

  private static final Log LOG =
      LogFactory.getLog(DataDrivenDBInputFormat.class);

  /**
   * If users are providing their own query, the following string is expected
   * to appear in the WHERE clause, which will be substituted with a pair of
   * conditions on the input to allow input splits to parallelise the import.
   */
  public static final String SUBSTITUTE_TOKEN = "$CONDITIONS";

  /**
   * @return the DBSplitter implementation to use to divide the table/query
   * into InputSplits.
   */
  protected DBSplitter getSplitter(int sqlDataType) {
    return getSplitter(sqlDataType, 0);
  }

  /**
   * @return the DBSplitter implementation to use to divide the table/query
   * into InputSplits.
   */
  protected DBSplitter getSplitter(int sqlDataType, long splitLimit) {
    switch (sqlDataType) {
    case Types.NUMERIC:
    case Types.DECIMAL:
      if(splitLimit >= 0) {
        throw new IllegalArgumentException("split-limit is supported only with Integer and Date columns");
      }
      return new BigDecimalSplitter();

    case Types.BIT:
    case Types.BOOLEAN:
      if(splitLimit >= 0) {
        throw new IllegalArgumentException("split-limit is supported only with Integer and Date columns");
      }
      return new BooleanSplitter();

    case Types.INTEGER:
    case Types.TINYINT:
    case Types.SMALLINT:
    case Types.BIGINT:
      return new IntegerSplitter();

    case Types.REAL:
    case Types.FLOAT:
    case Types.DOUBLE:
      if(splitLimit >= 0) {
        throw new IllegalArgumentException("split-limit is supported only with Integer and Date columns");
      }
      return new FloatSplitter();

    case Types.NVARCHAR:
    case Types.NCHAR:
      if(splitLimit >= 0) {
        throw new IllegalArgumentException("split-limit is supported only with Integer and Date columns");
      }
      return new NTextSplitter();

    case Types.CHAR:
    case Types.VARCHAR:
    case Types.LONGVARCHAR:
      if(splitLimit >= 0) {
         throw new IllegalArgumentException("split-limit is supported only with Integer and Date columns");
      }
      return new TextSplitter();

    case Types.DATE:
    case Types.TIME:
    case Types.TIMESTAMP:
      return new DateSplitter();

    default:
      // TODO: Support BINARY, VARBINARY, LONGVARBINARY, DISTINCT, CLOB,
      // BLOB, ARRAY, STRUCT, REF, DATALINK, and JAVA_OBJECT.
      if(splitLimit >= 0) {
        throw new IllegalArgumentException("split-limit is supported only with Integer and Date columns");
      }
      return null;
    }
  }

  @Override
  /** {@inheritDoc} */
  public List<InputSplit> getSplits(JobContext job) throws IOException {

    int targetNumTasks = ConfigurationHelper.getJobNumMaps(job);
    String boundaryQuery = getDBConf().getInputBoundingQuery();

    long splitLimit = org.apache.sqoop.config.ConfigurationHelper
      .getSplitLimit(job.getConfiguration());
    // If user do not forced us to use his boundary query and we don't have to
    // bacause there is only one mapper we will return single split that
    // separates nothing. This can be considerably more optimal for a large
    // table with no index.
    if (1 == targetNumTasks
            && (boundaryQuery == null || boundaryQuery.isEmpty())
            && splitLimit <= 0) {
      List<InputSplit> singletonSplit = new ArrayList<InputSplit>();
      singletonSplit.add(new org.apache.sqoop.mapreduce.db.
          DataDrivenDBInputFormat.DataDrivenDBInputSplit("1=1", "1=1"));
      return singletonSplit;
    }

    ResultSet results = null;
    Statement statement = null;
    Connection connection = getConnection();
    try {
      statement = connection.createStatement();

      String query = getBoundingValsQuery();
      LOG.info("BoundingValsQuery: " + query);

      results = statement.executeQuery(query);
      results.next();

      // Based on the type of the results, use a different mechanism
      // for interpolating split points (i.e., numeric splits, text splits,
      // dates, etc.)
      int sqlDataType = results.getMetaData().getColumnType(1);
      boolean isSigned = results.getMetaData().isSigned(1);

      // MySQL has an unsigned integer which we need to allocate space for
      if (sqlDataType == Types.INTEGER && !isSigned){
          sqlDataType = Types.BIGINT;
      }

      DBSplitter splitter = getSplitter(sqlDataType, splitLimit);
      if (null == splitter) {
        throw new IOException("Sqoop does not have the splitter for the given"
          + " SQL data type. Please use either different split column (argument"
          + " --split-by) or lower the number of mappers to 1. Unknown SQL data"
          + " type: " + sqlDataType);
      }

      try {
        return splitter.split(job.getConfiguration(), results,
                  getDBConf().getInputOrderBy());
      } catch (ValidationException e) {
        throw new IOException(e);
      }
    } catch (SQLException e) {
      throw new IOException(e);
    } finally {
      // More-or-less ignore SQL exceptions here, but log in case we need it.
      try {
        if (null != results) {
          results.close();
        }
      } catch (SQLException se) {
        LOG.debug("SQLException closing resultset: " + se.toString());
      }

      try {
        if (null != statement) {
          statement.close();
        }
      } catch (SQLException se) {
        LOG.debug("SQLException closing statement: " + se.toString());
      }

      try {
        connection.commit();
        closeConnection();
      } catch (SQLException se) {
        LOG.debug("SQLException committing split transaction: "
            + se.toString());
      }
    }
  }

  /**
   * @return a query which returns the minimum and maximum values for
   * the order-by column.
   *
   * The min value should be in the first column, and the
   * max value should be in the second column of the results.
   */
  protected String getBoundingValsQuery() {
    // If the user has provided a query, use that instead.
    String userQuery = getDBConf().getInputBoundingQuery();
    if (null != userQuery) {
      return userQuery;
    }

    // Auto-generate one based on the table name we've been provided with.
    StringBuilder query = new StringBuilder();

    String splitCol = getDBConf().getInputOrderBy();
    query.append("SELECT MIN(").append(splitCol).append("), ");
    query.append("MAX(").append(splitCol).append(") FROM ");
    query.append(getDBConf().getInputTableName());
    String conditions = getDBConf().getInputConditions();
    if (null != conditions) {
      query.append(" WHERE ( " + conditions + " )");
    }

    return query.toString();
  }

  protected RecordReader<LongWritable, T> createDBRecordReader(
      DBInputSplit split, Configuration conf) throws IOException {

    DBConfiguration dbConf = getDBConf();
    @SuppressWarnings("unchecked")
    Class<T> inputClass = (Class<T>) (dbConf.getInputClass());
    String dbProductName = getDBProductName();

    LOG.debug("Creating db record reader for db product: " + dbProductName);

    try {
      return new DataDrivenDBRecordReader<T>(split, inputClass,
          conf, getConnection(), dbConf, dbConf.getInputConditions(),
          dbConf.getInputFieldNames(), dbConf.getInputTableName(),
          dbProductName);
    } catch (SQLException ex) {
      throw new IOException(ex);
    }
  }


  /*
   * Set the user-defined bounding query to use with a user-defined query.
   * This *must* include the substring "$CONDITIONS"
   * (DataDrivenDBInputFormat.SUBSTITUTE_TOKEN) inside the WHERE clause,
   * so that DataDrivenDBInputFormat knows where to insert split clauses.
   * e.g., "SELECT foo FROM mytable WHERE $CONDITIONS"
   * This will be expanded to something like:
   * SELECT foo FROM mytable WHERE (id &gt; 100) AND (id &lt; 250)
   * inside each split.
   */
  public static void setBoundingQuery(Configuration conf, String query) {
    if (null != query) {
      // If the user's settng a query, warn if they don't allow conditions.
      if (query.indexOf(SUBSTITUTE_TOKEN) == -1) {
        LOG.warn("Could not find " + SUBSTITUTE_TOKEN + " token in query: "
            + query + "; splits may not partition data.");
      }
    }

    conf.set(DBConfiguration.INPUT_BOUNDING_QUERY, query);
  }

  // Configuration methods override superclass to ensure that the proper
  // DataDrivenDBInputFormat gets used.

  /** Note that the "orderBy" column is called the "splitBy" in this version.
    * We reuse the same field, but it's not strictly ordering it
    * -- just partitioning the results.
    */
  public static void setInput(Job job,
      Class<? extends DBWritable> inputClass,
      String tableName, String conditions,
      String splitBy, String... fieldNames) {
    DBInputFormat.setInput(job, inputClass, tableName, conditions,
        splitBy, fieldNames);
    job.setInputFormatClass(DataDrivenDBInputFormat.class);
  }

  /** setInput() takes a custom query and a separate "bounding query" to use
      instead of the custom "count query" used by DBInputFormat.
    */
  public static void setInput(Job job,
      Class<? extends DBWritable> inputClass,
      String inputQuery, String inputBoundingQuery) {
    DBInputFormat.setInput(job, inputClass, inputQuery, "");
    job.getConfiguration().set(DBConfiguration.INPUT_BOUNDING_QUERY,
        inputBoundingQuery);
    job.setInputFormatClass(DataDrivenDBInputFormat.class);
  }


  /**
   * A InputSplit that spans a set of rows.
   */
  public static class DataDrivenDBInputSplit
      extends DBInputFormat.DBInputSplit {

    private String lowerBoundClause;
    private String upperBoundClause;

    /**
     * Default Constructor.
     */
    public DataDrivenDBInputSplit() {
    }

    /**
     * Convenience Constructor.
     * @param lower the string to be put in the WHERE clause to guard
     * on the 'lower' end.
     * @param upper the string to be put in the WHERE clause to guard
     * on the 'upper' end.
     */
    public DataDrivenDBInputSplit(final String lower, final String upper) {
      this.lowerBoundClause = lower;
      this.upperBoundClause = upper;

      LOG.debug("Creating input split with lower bound '" + lower
        + "' and upper bound '" + upper + "'");
    }

    /**
     * @return The total row count in this split.
     */
    public long getLength() throws IOException {
      return 0; // unfortunately, we don't know this.
    }

    @Override
    /** {@inheritDoc} */
    public void readFields(DataInput input) throws IOException {
      this.lowerBoundClause = Text.readString(input);
      this.upperBoundClause = Text.readString(input);
    }

    @Override
    /** {@inheritDoc} */
    public void write(DataOutput output) throws IOException {
      Text.writeString(output, this.lowerBoundClause);
      Text.writeString(output, this.upperBoundClause);
    }

    public String getLowerClause() {
      return lowerBoundClause;
    }

    public String getUpperClause() {
      return upperBoundClause;
    }

    @Override
    public String toString() {
      return this.lowerBoundClause + " AND " + this.upperBoundClause;
    }
  }

}
