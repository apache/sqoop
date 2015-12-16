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
package org.apache.sqoop.connector.jdbc;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.TimeZone;

import org.apache.sqoop.common.SqoopException;
import org.apache.sqoop.error.code.GenericJdbcConnectorError;
import org.apache.sqoop.job.etl.Partition;

@edu.umd.cs.findbugs.annotations.SuppressWarnings("DB_DUPLICATE_SWITCH_CLAUSES")
public class GenericJdbcPartition extends Partition {

  private String condition;
  private List<Integer> sqlTypes;
  private List<Object> params;


  public GenericJdbcPartition() {
    sqlTypes = new ArrayList<>();
    params = new ArrayList<>();
  }

  public void setCondition(String condition) {
    this.condition = condition;
  }

  public String getCondition() {
    return condition;
  }

  public void addParamsToPreparedStatement(PreparedStatement ps) throws SQLException {
    for (int i = 0; i < params.size(); i++) {
      ps.setObject(i + 1, params.get(i));
    }
  }

  public void addParam(int sqlType, Object param) {
    sqlTypes.add(sqlType);
    params.add(param);
  }


  public List<Object> getParams() {
    return params;
  }

  public List<Integer> getSqlTypes() {
    return sqlTypes;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    int numParams = in.readInt();
    condition = in.readUTF();
    for (int i = 0; i < numParams; i++) {
      int type = in.readInt();
      sqlTypes.add(type);
      switch (type) {
        case Types.TINYINT:
        case Types.SMALLINT:
        case Types.INTEGER:
        case Types.BIGINT:
          params.add(in.readLong());
          break;
        case Types.REAL:
        case Types.FLOAT:
          params.add(in.readDouble());
          break;
        case Types.DOUBLE:
        case Types.NUMERIC:
        case Types.DECIMAL:
          params.add(new BigDecimal(in.readUTF()));
          break;
        case Types.BIT:
        case Types.BOOLEAN:
          params.add(in.readBoolean());
          break;
        case Types.DATE:
          params.add(new java.sql.Date(in.readLong()));
          break;
        case Types.TIME:
          params.add(new java.sql.Time(in.readLong()));
          break;
        case Types.TIMESTAMP:
          params.add(new java.sql.Timestamp(in.readLong()));
          break;
        case Types.CHAR:
        case Types.VARCHAR:
        case Types.LONGVARCHAR:
          params.add(in.readUTF());
          break;
        default:
          throw new SqoopException(
            GenericJdbcConnectorError.GENERIC_JDBC_CONNECTOR_0011,
            String.valueOf(type));
      }
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(sqlTypes.size());
    out.writeUTF(condition);
    for (int i = 0; i < sqlTypes.size(); i++) {
      int type = sqlTypes.get(i);
      out.writeInt(type);
      switch (type) {
        case Types.TINYINT:
        case Types.SMALLINT:
        case Types.INTEGER:
        case Types.BIGINT:
          out.writeLong((Long) params.get(i));
          break;
        case Types.REAL:
        case Types.FLOAT:
          out.writeDouble((Double) params.get(i));
          break;
        case Types.DOUBLE:
        case Types.NUMERIC:
        case Types.DECIMAL:
          out.writeUTF(params.get(i).toString());
          break;
        case Types.BIT:
        case Types.BOOLEAN:
          out.writeBoolean((Boolean) params.get(i));
          break;
        case Types.DATE:
          out.writeLong(((java.sql.Date) params.get(i)).getTime());
          break;
        case Types.TIME:
          out.writeLong(((java.sql.Time) params.get(i)).getTime());
          break;
        case Types.TIMESTAMP:
          out.writeLong(((java.sql.Timestamp) params.get(i)).getTime());
          break;
        case Types.CHAR:
        case Types.VARCHAR:
        case Types.LONGVARCHAR:
          out.writeUTF((String) params.get(i));
          break;
        default:
          throw new SqoopException(
            GenericJdbcConnectorError.GENERIC_JDBC_CONNECTOR_0011,
            String.valueOf(type));
      }
    }
  }

  @Override
  public String toString(){
    return asStringWithTimezone(TimeZone.getTimeZone("UTC"));
  }

  public String asStringWithTimezone(TimeZone timeZone) {
    DateFormat dateDateFormat = new SimpleDateFormat("yyyy-MM-dd");

    DateFormat timeDateFormat = new SimpleDateFormat("HH:mm:ss");
    timeDateFormat.setTimeZone(timeZone);

    DateFormat timestampDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    timestampDateFormat.setTimeZone(timeZone);

    String condition = getCondition();
    for (int j = 0; j < getParams().size(); j++) {
      Object param = getParams().get(j);
      int sqlType = getSqlTypes().get(j);
      switch (sqlType) {
        case Types.TINYINT:
        case Types.SMALLINT:
        case Types.INTEGER:
        case Types.BIGINT:
        case Types.REAL:
        case Types.FLOAT:
        case Types.DOUBLE:
        case Types.NUMERIC:
        case Types.DECIMAL:
        case Types.BIT:
        case Types.BOOLEAN:
          condition = condition.replaceFirst("\\?", param.toString());
          break;
        case Types.DATE:
          condition = condition.replaceFirst("\\?", "'" + dateDateFormat.format(param) + "'");
          break;
        case Types.TIME:
          condition = condition.replaceFirst("\\?", "'" + timeDateFormat.format(param) + "'");
          break;
        case Types.TIMESTAMP:
          condition = condition.replaceFirst("\\?", "'" + timestampDateFormat.format(param) + "'");
          break;
        case Types.CHAR:
        case Types.VARCHAR:
        case Types.LONGVARCHAR:
        default:
          condition = condition.replaceFirst("\\?", "'" + param.toString() + "'");
          break;
      }
    }
    return condition;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    GenericJdbcPartition that = (GenericJdbcPartition) o;

    if (getCondition() != null ? !getCondition().equals(that.getCondition())
      : that.getCondition() != null)
      return false;
    if (!getSqlTypes().equals(that.getSqlTypes())) return false;
    return getParams().equals(that.getParams());

  }

  @Override
  public int hashCode() {
    int result = getCondition() != null ? getCondition().hashCode() : 0;
    result = 31 * result + getSqlTypes().hashCode();
    result = 31 * result + getParams().hashCode();
    return result;
  }
}