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

package org.apache.sqoop.orm;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.BytesWritable;
import org.apache.sqoop.mapreduce.ImportJobBase;

import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.manager.ConnManager;
import com.cloudera.sqoop.lib.BigDecimalSerializer;
import com.cloudera.sqoop.lib.BooleanParser;
import com.cloudera.sqoop.lib.DelimiterSet;
import com.cloudera.sqoop.lib.FieldFormatter;
import com.cloudera.sqoop.lib.JdbcWritableBridge;
import com.cloudera.sqoop.lib.LargeObjectLoader;
import com.cloudera.sqoop.lib.LobSerializer;
import com.cloudera.sqoop.lib.RecordParser;
import com.cloudera.sqoop.lib.BlobRef;
import com.cloudera.sqoop.lib.ClobRef;
import com.cloudera.sqoop.lib.SqoopRecord;

/**
 * Creates an ORM class to represent a table from a database.
 */
public class ClassWriter {

  public static final Log LOG = LogFactory.getLog(ClassWriter.class.getName());

  // The following are keywords and cannot be used for class, method, or field
  // names.
  public static final HashSet<String> JAVA_RESERVED_WORDS;

  static {
    JAVA_RESERVED_WORDS = new HashSet<String>();

    JAVA_RESERVED_WORDS.add("abstract");
    JAVA_RESERVED_WORDS.add("assert");
    JAVA_RESERVED_WORDS.add("boolean");
    JAVA_RESERVED_WORDS.add("break");
    JAVA_RESERVED_WORDS.add("byte");
    JAVA_RESERVED_WORDS.add("case");
    JAVA_RESERVED_WORDS.add("catch");
    JAVA_RESERVED_WORDS.add("char");
    JAVA_RESERVED_WORDS.add("class");
    JAVA_RESERVED_WORDS.add("const");
    JAVA_RESERVED_WORDS.add("continue");
    JAVA_RESERVED_WORDS.add("default");
    JAVA_RESERVED_WORDS.add("do");
    JAVA_RESERVED_WORDS.add("double");
    JAVA_RESERVED_WORDS.add("else");
    JAVA_RESERVED_WORDS.add("enum");
    JAVA_RESERVED_WORDS.add("extends");
    JAVA_RESERVED_WORDS.add("false");
    JAVA_RESERVED_WORDS.add("final");
    JAVA_RESERVED_WORDS.add("finally");
    JAVA_RESERVED_WORDS.add("float");
    JAVA_RESERVED_WORDS.add("for");
    JAVA_RESERVED_WORDS.add("goto");
    JAVA_RESERVED_WORDS.add("if");
    JAVA_RESERVED_WORDS.add("implements");
    JAVA_RESERVED_WORDS.add("import");
    JAVA_RESERVED_WORDS.add("instanceof");
    JAVA_RESERVED_WORDS.add("int");
    JAVA_RESERVED_WORDS.add("interface");
    JAVA_RESERVED_WORDS.add("long");
    JAVA_RESERVED_WORDS.add("native");
    JAVA_RESERVED_WORDS.add("new");
    JAVA_RESERVED_WORDS.add("null");
    JAVA_RESERVED_WORDS.add("package");
    JAVA_RESERVED_WORDS.add("private");
    JAVA_RESERVED_WORDS.add("protected");
    JAVA_RESERVED_WORDS.add("public");
    JAVA_RESERVED_WORDS.add("return");
    JAVA_RESERVED_WORDS.add("short");
    JAVA_RESERVED_WORDS.add("static");
    JAVA_RESERVED_WORDS.add("strictfp");
    JAVA_RESERVED_WORDS.add("super");
    JAVA_RESERVED_WORDS.add("switch");
    JAVA_RESERVED_WORDS.add("synchronized");
    JAVA_RESERVED_WORDS.add("this");
    JAVA_RESERVED_WORDS.add("throw");
    JAVA_RESERVED_WORDS.add("throws");
    JAVA_RESERVED_WORDS.add("transient");
    JAVA_RESERVED_WORDS.add("true");
    JAVA_RESERVED_WORDS.add("try");
    JAVA_RESERVED_WORDS.add("void");
    JAVA_RESERVED_WORDS.add("volatile");
    JAVA_RESERVED_WORDS.add("while");

    // not strictly reserved words, but collides with
    // our imports
    JAVA_RESERVED_WORDS.add("Text");
  }

  public static final String PROPERTY_CODEGEN_METHODS_MAXCOLS =
      "codegen.methods.maxcols";

  /**
   * This version number is injected into all generated Java classes to denote
   * which version of the ClassWriter's output format was used to generate the
   * class.
   *
   * If the way that we generate classes changes, bump this number.
   * This number is retrieved by the SqoopRecord.getClassFormatVersion()
   * method.
   */
  public static final int CLASS_WRITER_VERSION = 3;

  /**
   * Default maximum number of columns per method.
   */
  public static final int MAX_COLUMNS_PER_METHOD_DEFAULT = 500;

  /**
   * This number confines the number of allowed columns in a single method.
   * It allows code generation to scale to thousands of columns without
   * running into "code too large" exceptions.
   */
  private int maxColumnsPerMethod;

  private SqoopOptions options;
  private ConnManager connManager;
  private String tableName;
  private CompilationManager compileManager;
  private boolean bigDecimalFormatString;

  /**
   * Creates a new ClassWriter to generate an ORM class for a table
   * or arbitrary query.
   * @param opts program-wide options
   * @param connMgr the connection manager used to describe the table.
   * @param table the name of the table to read. If null, query is taken
   * from the SqoopOptions.
   */
  public ClassWriter(final SqoopOptions opts, final ConnManager connMgr,
      final String table, final CompilationManager compMgr) {
    this.options = opts;
    this.connManager = connMgr;
    this.tableName = table;
    this.compileManager = compMgr;
    this.bigDecimalFormatString = this.options.getConf().getBoolean(
        ImportJobBase.PROPERTY_BIGDECIMAL_FORMAT,
        ImportJobBase.PROPERTY_BIGDECIMAL_FORMAT_DEFAULT);
    this.maxColumnsPerMethod = this.options.getConf().getInt(
        PROPERTY_CODEGEN_METHODS_MAXCOLS,
        MAX_COLUMNS_PER_METHOD_DEFAULT);
  }

  /**
   * Given some character that can't be in an identifier,
   * try to map it to a string that can.
   *
   * @param c a character that can't be in a Java identifier
   * @return a string of characters that can, or null if there's
   * no good translation.
   */
  public static String getIdentifierStrForChar(char c) {
    if (Character.isJavaIdentifierPart(c)) {
      return "" + c;
    } else if (Character.isWhitespace(c)) {
      // Eliminate whitespace.
      return null;
    } else {
      // All other characters map to underscore.
      return "_";
    }
  }

  /**
   * @param word a word to test.
   * @return true if 'word' is reserved the in Java language.
   */
  private static boolean isReservedWord(String word) {
    return JAVA_RESERVED_WORDS.contains(word);
  }

  /**
   * Coerce a candidate name for an identifier into one which is a valid
   * Java or Avro identifier.
   *
   * Ensures that the returned identifier matches [A-Za-z_][A-Za-z0-9_]*
   * and is not a reserved word.
   *
   * @param candidate A string we want to use as an identifier
   * @return A string naming an identifier which compiles and is
   *   similar to the candidate.
   */
  public static String toIdentifier(String candidate) {
    StringBuilder sb = new StringBuilder();
    boolean first = true;
    for (char c : candidate.toCharArray()) {
      if (Character.isJavaIdentifierStart(c) && first) {
        // Ok for this to be the first character of the identifier.
        sb.append(c);
        first = false;
      } else if (Character.isJavaIdentifierPart(c) && !first) {
        // Ok for this character to be in the output identifier.
        sb.append(c);
      } else {
        // We have a character in the original that can't be
        // part of this identifier we're building.
        // If it's just not allowed to be the first char, add a leading '_'.
        // If we have a reasonable translation (e.g., '-' -> '_'), do that.
        // Otherwise, drop it.
        if (first && Character.isJavaIdentifierPart(c)
            && !Character.isJavaIdentifierStart(c)) {
          sb.append("_");
          sb.append(c);
          first = false;
        } else {
          // Try to map this to a different character or string.
          // If we can't just give up.
          String translated = getIdentifierStrForChar(c);
          if (null != translated) {
            sb.append(translated);
            first = false;
          }
        }
      }
    }
    return sb.toString();
  }

  /**
   * Coerce a candidate name for an identifier into one which will
   * definitely compile.
   *
   * Ensures that the returned identifier matches [A-Za-z_][A-Za-z0-9_]*
   * and is not a reserved word.
   *
   * @param candidate A string we want to use as an identifier
   * @return A string naming an identifier which compiles and is
   *   similar to the candidate.
   */
  public static String toJavaIdentifier(String candidate) {
    String output = toIdentifier(candidate);
    if (isReservedWord(output)) {
      // e.g., 'class' -> '_class';
      return "_" + output;

    /*
     * We're using candidate.startsWith instead of output.startsWith on purpose
     * to preserve backward compatibility as much as possible. For example
     * column "9isLegalInSql" was translated into "_9isLegalInSql" in original
     * code and will translate to same "_9isLegalInSql" now. However it would
     * be translated to "__9isLegalInSql" (notice that there are two "_" at the
     * begging) if we would use output.startsWith instead.
     */
    } else if (candidate.startsWith("_")) {
      return "_" + output;
    }

    return output;
  }

  private String toJavaType(String columnName, int sqlType) {
    Properties mapping = options.getMapColumnJava();

    if (mapping.containsKey(columnName)) {
      String type = mapping.getProperty(columnName);
      if (LOG.isDebugEnabled()) {
        LOG.info("Overriding type of column " + columnName + " to " + type);
      }
      return type;
    }

    return connManager.toJavaType(tableName, columnName, sqlType);
  }

  /**
   * @param javaType
   * @return the name of the method of JdbcWritableBridge to read an entry
   * with a given java type.
   */
  private String dbGetterForType(String javaType) {
    // All Class-based types (e.g., java.math.BigDecimal) are handled with
    // "readBar" where some.package.foo.Bar is the canonical class name.  Turn
    // the javaType string into the getter type string.

    String [] parts = javaType.split("\\.");
    if (parts.length == 0) {
      LOG.error("No ResultSet method for Java type " + javaType);
      return null;
    }

    String lastPart = parts[parts.length - 1];
    try {
      String getter = "read" + Character.toUpperCase(lastPart.charAt(0))
          + lastPart.substring(1);
      return getter;
    } catch (StringIndexOutOfBoundsException oob) {
      // lastPart.*() doesn't work on empty strings.
      LOG.error("Could not infer JdbcWritableBridge getter for Java type "
          + javaType);
      return null;
    }
  }

  /**
   * @param javaType
   * @return the name of the method of JdbcWritableBridge to write an entry
   * with a given java type.
   */
  private String dbSetterForType(String javaType) {
    // TODO(aaron): Lots of unit tests needed here.
    // See dbGetterForType() for the logic used here; it's basically the same.

    String [] parts = javaType.split("\\.");
    if (parts.length == 0) {
      LOG.error("No PreparedStatement Set method for Java type " + javaType);
      return null;
    }

    String lastPart = parts[parts.length - 1];
    try {
      String setter = "write" + Character.toUpperCase(lastPart.charAt(0))
          + lastPart.substring(1);
      return setter;
    } catch (StringIndexOutOfBoundsException oob) {
      // lastPart.*() doesn't work on empty strings.
      LOG.error("Could not infer PreparedStatement setter for Java type "
          + javaType);
      return null;
    }
  }

  private String stringifierForType(String javaType, String colName) {
    if (javaType.equals("String")) {
      // Check if it is null, and write the null representation in such case
      String r = colName  + "==null?\"" + this.options.getNullStringValue()
          + "\":" + colName;
      return r;
    } else if (javaType.equals("java.math.BigDecimal")
        && this.bigDecimalFormatString) {
      // Use toPlainString method for BigDecimals if option is set
      String r = colName  + "==null?\"" + this.options.getNullNonStringValue()
          + "\":" + colName + ".toPlainString()";
      return r;
    } else {
      // This is an object type -- just call its toString() in a null-safe way.
      // Also check if it is null, and instead write the null representation
      // in such case
      String r = colName  + "==null?\"" + this.options.getNullNonStringValue()
          + "\":" + "\"\" + " + colName;
      return r;
    }
  }

  /**
   * @param javaType the type to read
   * @param inputObj the name of the DataInput to read from
   * @param colName the column name to read
   * @return the line of code involving a DataInput object to read an entry
   * with a given java type.
   */
  private String rpcGetterForType(String javaType, String inputObj,
      String colName) {
    if (javaType.equals("Integer")) {
      return "    this." + colName + " = Integer.valueOf(" + inputObj
          + ".readInt());\n";
    } else if (javaType.equals("Long")) {
      return "    this." + colName + " = Long.valueOf(" + inputObj
          + ".readLong());\n";
    } else if (javaType.equals("Float")) {
      return "    this." + colName + " = Float.valueOf(" + inputObj
          + ".readFloat());\n";
    } else if (javaType.equals("Double")) {
      return "    this." + colName + " = Double.valueOf(" + inputObj
          + ".readDouble());\n";
    } else if (javaType.equals("Boolean")) {
      return "    this." + colName + " = Boolean.valueOf(" + inputObj
          + ".readBoolean());\n";
    } else if (javaType.equals("String")) {
      return "    this." + colName + " = Text.readString(" + inputObj + ");\n";
    } else if (javaType.equals("java.sql.Date")) {
      return "    this." + colName + " = new Date(" + inputObj
          + ".readLong());\n";
    } else if (javaType.equals("java.sql.Time")) {
      return "    this." + colName + " = new Time(" + inputObj
          + ".readLong());\n";
    } else if (javaType.equals("java.sql.Timestamp")) {
      return "    this." + colName + " = new Timestamp(" + inputObj
          + ".readLong());\n" + "    this." + colName + ".setNanos("
          + inputObj + ".readInt());\n";
    } else if (javaType.equals("java.math.BigDecimal")) {
      return "    this." + colName + " = "
          + BigDecimalSerializer.class.getCanonicalName()
          + ".readFields(" + inputObj + ");\n";
    } else if (javaType.equals(ClobRef.class.getName())) {
      return "    this." + colName + " = "
          + LobSerializer.class.getCanonicalName()
          + ".readClobFields(" + inputObj + ");\n";
    } else if (javaType.equals(BlobRef.class.getName())) {
      return "    this." + colName + " = "
          + LobSerializer.class.getCanonicalName()
          + ".readBlobFields(" + inputObj + ");\n";
    } else if (javaType.equals(BytesWritable.class.getName())) {
      return "    this." + colName + " = new BytesWritable();\n"
          + "    this." + colName + ".readFields(" + inputObj + ");\n";
    } else {
      LOG.error("No ResultSet method for Java type " + javaType);
      return null;
    }
  }

  /**
   * Deserialize a possibly-null value from the DataInput stream.
   * @param javaType name of the type to deserialize if it's not null.
   * @param inputObj name of the DataInput to read from
   * @param colName the column name to read.
   * @return
   */
  private String rpcGetterForMaybeNull(String javaType, String inputObj,
      String colName) {
    return "    if (" + inputObj + ".readBoolean()) { \n"
        + "        this." + colName + " = null;\n"
        + "    } else {\n"
        + rpcGetterForType(javaType, inputObj, colName)
        + "    }\n";
  }

  /**
   * @param javaType the type to write
   * @param outputObj the name of the DataOutput to write to
   * @param colName the column name to write
   * @return the line of code involving a DataOutput object to write an entry
   * with a given java type.
   */
  private String rpcSetterForType(String javaType, String outputObj,
      String colName) {
    if (javaType.equals("Integer")) {
      return "    " + outputObj + ".writeInt(this." + colName + ");\n";
    } else if (javaType.equals("Long")) {
      return "    " + outputObj + ".writeLong(this." + colName + ");\n";
    } else if (javaType.equals("Boolean")) {
      return "    " + outputObj + ".writeBoolean(this." + colName + ");\n";
    } else if (javaType.equals("Float")) {
      return "    " + outputObj + ".writeFloat(this." + colName + ");\n";
    } else if (javaType.equals("Double")) {
      return "    " + outputObj + ".writeDouble(this." + colName + ");\n";
    } else if (javaType.equals("String")) {
      return "    Text.writeString(" + outputObj + ", " + colName + ");\n";
    } else if (javaType.equals("java.sql.Date")) {
      return "    " + outputObj + ".writeLong(this." + colName
          + ".getTime());\n";
    } else if (javaType.equals("java.sql.Time")) {
      return "    " + outputObj + ".writeLong(this." + colName
          + ".getTime());\n";
    } else if (javaType.equals("java.sql.Timestamp")) {
      return "    " + outputObj + ".writeLong(this." + colName
          + ".getTime());\n" + "    " + outputObj + ".writeInt(this." + colName
          + ".getNanos());\n";
    } else if (javaType.equals(BytesWritable.class.getName())) {
      return "    this." + colName + ".write(" + outputObj + ");\n";
    } else if (javaType.equals("java.math.BigDecimal")) {
      return "    " + BigDecimalSerializer.class.getCanonicalName()
          + ".write(this." + colName + ", " + outputObj + ");\n";
    } else if (javaType.equals(ClobRef.class.getName())) {
      return "    " + LobSerializer.class.getCanonicalName()
          + ".writeClob(this." + colName + ", " + outputObj + ");\n";
    } else if (javaType.equals(BlobRef.class.getName())) {
      return "    " + LobSerializer.class.getCanonicalName()
          + ".writeBlob(this." + colName + ", " + outputObj + ");\n";
    } else {
      throw new IllegalArgumentException(
          "No ResultSet method for Java type " + javaType);
    }
  }

  /**
   * Serialize a possibly-null value to the DataOutput stream. First a boolean
   * isNull is written, followed by the contents itself (if not null).
   * @param javaType name of the type to deserialize if it's not null.
   * @param outputObj name of the DataInput to read from
   * @param colName the column name to read.
   * @return
   */
  private String rpcSetterForMaybeNull(String javaType, String outputObj,
      String colName) {
    return "    if (null == this." + colName + ") { \n"
        + "        " + outputObj + ".writeBoolean(true);\n"
        + "    } else {\n"
        + "        " + outputObj + ".writeBoolean(false);\n"
        + rpcSetterForType(javaType, outputObj, colName)
        + "    }\n";
  }

  /**
   * Get the number of methods that should be generated for a particular column
   * set.
   * @param colNames
   * @param size
   * @return
   */
  private int getNumberOfMethods(String[] colNames, int size) {
    int extra = 0;
    if (colNames.length % size != 0) {
      extra = 1;
    }
    return colNames.length / size + extra;
  }

  /**
   * Get the top boundary when iterating through columns on a
   * per method basis.
   * @param colNames
   * @param methodNumber
   * @param size
   * @return
   */
  private int topBoundary(String[] colNames, int methodNumber, int size) {
    return (colNames.length > (methodNumber + 1) * size)
            ? (methodNumber + 1) * size : colNames.length;
  }

  /**
   * Generate a member field, getter, setter and with method for each column.
   * @param columnTypes - mapping from column names to sql types
   * @param colNames - ordered list of column names for table
   * @param className - name of the generated class
   * @param sb - StringBuilder to append code to
   */
  private void generateFields(Map<String, Integer> columnTypes,
      String [] colNames, String className, StringBuilder sb) {

    for (String col : colNames) {
      int sqlType = columnTypes.get(col);
      String javaType = toJavaType(col, sqlType);
      if (null == javaType) {
        LOG.error("Cannot resolve SQL type " + sqlType);
        continue;
      }

      sb.append("  private " + javaType + " " + col + ";\n");
      sb.append("  public " + javaType + " get_" + col + "() {\n");
      sb.append("    return " + col + ";\n");
      sb.append("  }\n");
      sb.append("  public void set_" + col + "(" + javaType + " " + col
          + ") {\n");
      sb.append("    this." + col + " = " + col + ";\n");
      sb.append("  }\n");
      sb.append("  public " + className + " with_" + col + "(" + javaType + " "
          + col + ") {\n");
      sb.append("    this." + col + " = " + col + ";\n");
      sb.append("    return this;\n");
      sb.append("  }\n");
    }
  }

  /**
   * Generate an equals method that compares the fields for each column.
   * @param columnTypes - mapping from column names to sql types
   * @param colNames - ordered list of column names for table
   * @param className - name of the generated class
   * @param sb - StringBuilder to append code to
   */
  private void generateEquals(Map<String, Integer> columnTypes,
      String [] colNames, String className, StringBuilder sb) {

    int numberOfMethods =
            this.getNumberOfMethods(colNames, maxColumnsPerMethod);

    sb.append("  public boolean equals(Object o) {\n");
    if (numberOfMethods > 1) {
      sb.append("    boolean equal = true;\n");
      for (int i = 0; i < numberOfMethods; ++i) {
        sb.append("    equal = equal && this.equals" + i + "(o);\n");
      }
      sb.append("    return equal;\n");
    } else {
      myGenerateEquals(columnTypes, colNames, className, sb, 0,
              maxColumnsPerMethod, false);
    }
    sb.append("  }\n");

    for (int i = 0; i < numberOfMethods; ++i) {
      myGenerateEquals(columnTypes, colNames, className, sb, i,
              maxColumnsPerMethod, true);
    }
  }

  /**
   * Generate an equals method that compares the fields for each column.
   * @param columnTypes - mapping from column names to sql types
   * @param colNames - ordered list of column names for table
   * @param className - name of the generated class
   * @param sb - StringBuilder to append code to
   * @param methodNumber - method number
   * @param size - number of columns per method
   * @param wrapInMethod - wrap body in a method.
   */
  private void myGenerateEquals(Map<String, Integer> columnTypes,
                                String[] colNames, String className,
                                StringBuilder sb, int methodNumber, int size,
                                boolean wrapInMethod) {

    if (wrapInMethod) {
      sb.append("  public boolean equals" + methodNumber + "(Object o) {\n");
    }

    sb.append("    if (this == o) {\n");
    sb.append("      return true;\n");
    sb.append("    }\n");
    sb.append("    if (!(o instanceof " + className + ")) {\n");
    sb.append("      return false;\n");
    sb.append("    }\n");
    sb.append("    " + className + " that = (" + className + ") o;\n");
    sb.append("    boolean equal = true;\n");
    for (int i = size * methodNumber;
         i < topBoundary(colNames, methodNumber, size); ++i) {
      String col = colNames[i];
      int sqlType = columnTypes.get(col);
      String javaType = toJavaType(col, sqlType);
      if (null == javaType) {
        LOG.error("Cannot resolve SQL type " + sqlType);
        continue;
      }
      sb.append("    equal = equal && (this." + col + " == null ? that." + col
          + " == null : this." + col + ".equals(that." + col + "));\n");
    }
    sb.append("    return equal;\n");

    if (wrapInMethod) {
      sb.append("  }\n");
    }
  }

  /**
   * Generate the readFields() method used by the database.
   * @param columnTypes - mapping from column names to sql types
   * @param colNames - ordered list of column names for table.
   * @param sb - StringBuilder to append code to
   */
  private void generateDbRead(Map<String, Integer> columnTypes,
      String [] colNames, StringBuilder sb) {

    int numberOfMethods =
            this.getNumberOfMethods(colNames, maxColumnsPerMethod);

    sb.append("  public void readFields(ResultSet __dbResults) ");
    sb.append("throws SQLException {\n");
    // Save ResultSet object cursor for use in LargeObjectLoader
    // if necessary.
    sb.append("    this.__cur_result_set = __dbResults;\n");
    if (numberOfMethods > 1) {
      for (int i = 0; i < numberOfMethods; ++i) {
        sb.append("    this.readFields" + i + "(__dbResults);\n");
      }
    } else {
      myGenerateDbRead(columnTypes, colNames, sb, 0,
              maxColumnsPerMethod, false);
    }
    sb.append("  }\n");

    for (int i = 0; i < numberOfMethods; ++i) {
      myGenerateDbRead(columnTypes, colNames, sb, i,
              maxColumnsPerMethod, true);
    }
  }

  /**
   * Generate the readFields() method used by the database.
   * @param columnTypes - mapping from column names to sql types
   * @param colNames - ordered list of column names for table.
   * @param sb - StringBuilder to append code to
   * @param methodNumber - method number
   * @param size - number of columns per method
   * @param wrapInMethod - wrap body in a method.
   */
  private void myGenerateDbRead(Map<String, Integer> columnTypes,
                                String[] colNames, StringBuilder sb,
                                int methodNumber, int size,
                                boolean wrapInMethod) {

    if (wrapInMethod) {
      sb.append("  public void readFields" + methodNumber
              + "(ResultSet __dbResults) ");
      sb.append("throws SQLException {\n");
    }

    for (int i = methodNumber * size;
         i < topBoundary(colNames, methodNumber, size); ++i) {
      String col = colNames[i];

      int sqlType = columnTypes.get(col);
      String javaType = toJavaType(col, sqlType);
      if (null == javaType) {
        LOG.error("No Java type for SQL type " + sqlType
            + " for column " + col);
        continue;
      }

      String getterMethod = dbGetterForType(javaType);
      if (null == getterMethod) {
        LOG.error("No db getter method for Java type " + javaType);
        continue;
      }

      sb.append("    this." + col + " = JdbcWritableBridge." +  getterMethod
          + "(" + (i + 1) + ", __dbResults);\n");
    }

    if (wrapInMethod) {
      sb.append("  }\n");
    }
  }

  /**
   * Generate the loadLargeObjects() method called by the mapper to load
   * delayed objects (that require the Context from the mapper).
   */
  private void generateLoadLargeObjects(Map<String, Integer> columnTypes,
      String [] colNames, StringBuilder sb) {

    int numberOfMethods =
            this.getNumberOfMethods(colNames, maxColumnsPerMethod);

    // This method relies on the __cur_result_set field being set by
    // readFields() method generated by generateDbRead().

    sb.append("  public void loadLargeObjects(LargeObjectLoader __loader)\n");
    sb.append("      throws SQLException, IOException, ");
    sb.append("InterruptedException {\n");

    if (numberOfMethods > 1) {
      for (int i = 0; i < numberOfMethods; ++i) {
        sb.append("    this.loadLargeObjects" + i + "(__loader);\n");
      }
    } else {
      myGenerateLoadLargeObjects(columnTypes, colNames, sb, 0,
              maxColumnsPerMethod, false);
    }

    sb.append("  }\n");

    for (int i = 0; i < numberOfMethods; ++i) {
      myGenerateLoadLargeObjects(columnTypes, colNames, sb, i,
              maxColumnsPerMethod, true);
    }
  }

  /**
   * Generate the loadLargeObjects() method called by the mapper to load
   * delayed objects (that require the Context from the mapper).
   */
  private void myGenerateLoadLargeObjects(Map<String, Integer> columnTypes,
                                          String[] colNames, StringBuilder sb,
                                          int methodNumber, int size,
                                          boolean wrapInMethod) {

    // This method relies on the __cur_result_set field being set by
    // readFields() method generated by generateDbRead().

    if (wrapInMethod) {
      sb.append("  public void loadLargeObjects" + methodNumber
              + "(LargeObjectLoader __loader)\n");
      sb.append("      throws SQLException, IOException, ");
      sb.append("InterruptedException {\n");
    }

    for (int i = methodNumber * size;
         i < topBoundary(colNames, methodNumber, size); ++i) {
      String col = colNames[i];

      int sqlType = columnTypes.get(col);
      String javaType = toJavaType(col, sqlType);
      if (null == javaType) {
        LOG.error("No Java type for SQL type " + sqlType
            + " for column " + col);
        continue;
      }

      String getterMethod = dbGetterForType(javaType);
      if ("readClobRef".equals(getterMethod)
          || "readBlobRef".equals(getterMethod)) {
        // This field is a blob/clob field with delayed loading.  Call the
        // appropriate LargeObjectLoader method (which has the same name as a
        // JdbcWritableBridge method).
        sb.append("    this." + col + " = __loader." + getterMethod
            + "(" + (i + 1) + ", this.__cur_result_set);\n");
      }
    }

    if (wrapInMethod) {
      sb.append("  }\n");
    }
  }

  /**
   * Generate the write() method used by the database.
   * @param columnTypes - mapping from column names to sql types
   * @param colNames - ordered list of column names for table.
   * @param sb - StringBuilder to append code to
   */
  private void generateDbWrite(Map<String, Integer> columnTypes,
      String [] colNames, StringBuilder sb) {

    int numberOfMethods =
            this.getNumberOfMethods(colNames, maxColumnsPerMethod);

    sb.append("  public void write(PreparedStatement __dbStmt) "
        + "throws SQLException {\n");
    sb.append("    write(__dbStmt, 0);\n");
    sb.append("  }\n\n");

    sb.append("  public int write(PreparedStatement __dbStmt, int __off) "
        + "throws SQLException {\n");

    if (numberOfMethods > 1) {
      for (int i = 0; i < numberOfMethods; ++i) {
        sb.append("    write" + i + "(__dbStmt, __off);\n");
      }
    } else {
      myGenerateDbWrite(columnTypes, colNames, sb, 0,
              maxColumnsPerMethod, false);
    }

    sb.append("    return " + colNames.length + ";\n");
    sb.append("  }\n");

    for (int i = 0; i < numberOfMethods; ++i) {
      myGenerateDbWrite(columnTypes, colNames, sb, i,
              maxColumnsPerMethod, true);
    }
  }

  /**
   * Generate the write() method used by the database.
   * @param columnTypes - mapping from column names to sql types
   * @param colNames - ordered list of column names for table.
   * @param sb - StringBuilder to append code to
   * @param methodNumber - method number
   * @param size - number of columns per method
   * @param wrapInMethod - wrap body in a method.
   */
  private void myGenerateDbWrite(Map<String, Integer> columnTypes,
                                 String[] colNames, StringBuilder sb,
                                 int methodNumber, int size,
                                 boolean wrapInMethod) {

    if (wrapInMethod) {
      sb.append("  public void write" + methodNumber
              + "(PreparedStatement __dbStmt, int __off) "
              + "throws SQLException {\n");
    }

    for (int i = methodNumber * size;
         i < topBoundary(colNames, methodNumber, size); ++i) {
      String col = colNames[i];

      int sqlType = columnTypes.get(col);
      String javaType = toJavaType(col, sqlType);
      if (null == javaType) {
        LOG.error("No Java type for SQL type " + sqlType
            + " for column " + col);
        continue;
      }

      String setterMethod = dbSetterForType(javaType);
      if (null == setterMethod) {
        LOG.error("No db setter method for Java type " + javaType);
        continue;
      }

      sb.append("    JdbcWritableBridge." + setterMethod + "(" + col + ", "
          + (i + 1) + " + __off, " + sqlType + ", __dbStmt);\n");
    }

    if (wrapInMethod) {
      sb.append("  }\n");
    }
  }

  /**
   * Generate the readFields() method used by the Hadoop RPC system.
   * @param columnTypes - mapping from column names to sql types
   * @param colNames - ordered list of column names for table.
   * @param sb - StringBuilder to append code to
   */
  private void generateHadoopRead(Map<String, Integer> columnTypes,
      String [] colNames, StringBuilder sb) {

    int numberOfMethods =
            this.getNumberOfMethods(colNames, maxColumnsPerMethod);

    sb.append("  public void readFields(DataInput __dataIn) "
        + "throws IOException {\n");

    for (int i = 0; i < numberOfMethods; ++i) {
      sb.append("this.readFields" + i + "(__dataIn);");
    }

    sb.append("  }\n");

    for (int i = 0; i < numberOfMethods; ++i) {
      myGenerateHadoopRead(columnTypes, colNames, sb, i,
              maxColumnsPerMethod, true);
    }
  }

  /**
   * Generate the readFields() method used by the Hadoop RPC system.
   * @param columnTypes - mapping from column names to sql types
   * @param colNames - ordered list of column names for table.
   * @param sb - StringBuilder to append code to
   * @param methodNumber - method number
   * @param size - number of columns per method
   * @param wrapInMethod - wrap body in a method.
   */
  private void myGenerateHadoopRead(Map<String, Integer> columnTypes,
                                    String[] colNames, StringBuilder sb,
                                    int methodNumber, int size,
                                    boolean wrapInMethod) {
    if (wrapInMethod) {
      sb.append("  public void readFields" + methodNumber
              + "(DataInput __dataIn) " + "throws IOException {\n");
    }

    for (int i = methodNumber * size;
         i < topBoundary(colNames, methodNumber, size); ++i) {
      String col = colNames[i];
      int sqlType = columnTypes.get(col);
      String javaType = toJavaType(col, sqlType);
      if (null == javaType) {
        LOG.error("No Java type for SQL type " + sqlType
            + " for column " + col);
        continue;
      }

      String getterMethod = rpcGetterForMaybeNull(javaType, "__dataIn", col);
      if (null == getterMethod) {
        LOG.error("No RPC getter method for Java type " + javaType);
        continue;
      }

      sb.append(getterMethod);
    }

    if (wrapInMethod) {
      sb.append("  }\n");
    }
  }

  /**
   * Generate the clone() method.
   * @param columnTypes - mapping from column names to sql types
   * @param colNames - ordered list of column names for table.
   * @param sb - StringBuilder to append code to
   */
  private void generateCloneMethod(Map<String, Integer> columnTypes,
      String [] colNames, StringBuilder sb) {

    int numberOfMethods =
            this.getNumberOfMethods(colNames, maxColumnsPerMethod);

    TableClassName tableNameInfo = new TableClassName(options);
    String className = tableNameInfo.getShortClassForTable(tableName);

    sb.append("  public Object clone() throws CloneNotSupportedException {\n");
    sb.append("    " + className + " o = (" + className + ") super.clone();\n");

    if (numberOfMethods > 1) {
      for (int i = 0; i < numberOfMethods; ++i) {
        sb.append("    this.clone" + i + "(o);");
      }
    } else {
      myGenerateCloneMethod(columnTypes, colNames, sb, 0,
              maxColumnsPerMethod, false);
    }

    sb.append("    return o;\n");
    sb.append("  }\n\n");

    for (int i = 0; i < numberOfMethods; ++i) {
      myGenerateCloneMethod(columnTypes, colNames, sb, i,
              maxColumnsPerMethod, true);
    }
  }

  /**
   * Generate the clone() method.
   * @param columnTypes - mapping from column names to sql types
   * @param colNames - ordered list of column names for table.
   * @param sb - StringBuilder to append code to
   * @param methodNumber - method number
   * @param size - number of columns per method
   * @param wrapInMethod - wrap body in a method.
   */
  private void myGenerateCloneMethod(Map<String, Integer> columnTypes,
                                     String[] colNames, StringBuilder sb,
                                     int methodNumber, int size,
                                     boolean wrapInMethod) {
    TableClassName tableNameInfo = new TableClassName(options);
    String className = tableNameInfo.getShortClassForTable(tableName);

    if (wrapInMethod) {
      sb.append("  public void clone" + methodNumber
              + "(" + className + " o) throws CloneNotSupportedException {\n");
    }

    // For each field that is mutable, we need to perform the deep copy.
    for (int i = methodNumber * size;
         i < topBoundary(colNames, methodNumber, size); ++i) {
      String colName = colNames[i];
      int sqlType = columnTypes.get(colName);
      String javaType = toJavaType(colName, sqlType);
      if (null == javaType) {
        continue;
      } else if (javaType.equals("java.sql.Date")
          || javaType.equals("java.sql.Time")
          || javaType.equals("java.sql.Timestamp")
          || javaType.equals(ClobRef.class.getName())
          || javaType.equals(BlobRef.class.getName())) {
        sb.append("    o." + colName + " = (o." + colName + " != null) ? ("
            + javaType + ") o." + colName + ".clone() : null;\n");
      } else if (javaType.equals(BytesWritable.class.getName())) {
        sb.append("    o." + colName + " = (o." + colName + " != null) ? "
            + "new BytesWritable(Arrays.copyOf(" + colName + ".getBytes(), "
            + colName + ".getLength())) : null;\n");
      }
    }

    if (wrapInMethod) {
      sb.append("  }\n\n");
    }
  }

  /**
   * Generate the setField() method.
   * @param columnTypes - mapping from column names to sql types
   * @param colNames - ordered list of column names for table.
   * @param sb - StringBuilder to append code to
   */
  private void generateSetField(Map<String, Integer> columnTypes,
      String [] colNames, StringBuilder sb) {

    int numberOfMethods =
            this.getNumberOfMethods(colNames, maxColumnsPerMethod);

    sb.append("  public void setField(String __fieldName, Object __fieldVal) "
        + "{\n");
    if (numberOfMethods > 1) {
      boolean first = true;
      for (int i = 0; i < numberOfMethods; ++i) {
        if (!first) {
          sb.append("    else");
        }
        sb.append("    if (this.setField" + i
                + "(__fieldName, __fieldVal)) {\n");
        sb.append("      return;\n");
        sb.append("    }\n");
        first = false;
      }
    } else {
      boolean first = true;
      for (String colName : colNames) {
        int sqlType = columnTypes.get(colName);
        String javaType = toJavaType(colName, sqlType);
        if (null == javaType) {
          continue;
        } else {
          if (!first) {
            sb.append("    else");
          }

          sb.append("    if (\"" + colName + "\".equals(__fieldName)) {\n");
          sb.append("      this." + colName + " = (" + javaType
              + ") __fieldVal;\n");
          sb.append("    }\n");
          first = false;
        }
      }
    }
    sb.append("    else {\n");
    sb.append("      throw new RuntimeException(");
    sb.append("\"No such field: \" + __fieldName);\n");
    sb.append("    }\n");
    sb.append("  }\n");

    for (int i = 0; i < numberOfMethods; ++i) {
      myGenerateSetField(columnTypes, colNames, sb, i, maxColumnsPerMethod);
    }
  }

  /**
   * Generate the setField() method.
   * @param columnTypes - mapping from column names to sql types
   * @param colNames - ordered list of column names for table.
   * @param sb - StringBuilder to append code to
   * @param methodNumber - method number
   * @param size - number of columns per method
   */
  private void myGenerateSetField(Map<String, Integer> columnTypes,
                                  String[] colNames, StringBuilder sb,
                                  int methodNumber, int size) {
    sb.append("  public boolean setField" + methodNumber
            + "(String __fieldName, Object __fieldVal) {\n");

    boolean first = true;
    for (int i = methodNumber * size;
         i < topBoundary(colNames, methodNumber, size); ++i) {
      String colName = colNames[i];
      int sqlType = columnTypes.get(colName);
      String javaType = toJavaType(colName, sqlType);
      if (null == javaType) {
        continue;
      } else {
        if (!first) {
          sb.append("    else");
        }

        sb.append("    if (\"" + colName + "\".equals(__fieldName)) {\n");
        sb.append("      this." + colName + " = (" + javaType
            + ") __fieldVal;\n");
        sb.append("      return true;\n");
        sb.append("    }\n");
        first = false;
      }
    }
    sb.append("    else {\n");
    sb.append("      return false;");
    sb.append("    }\n");
    sb.append("  }\n");
  }

  /**
   * Generate the getFieldMap() method.
   * @param columnTypes - mapping from column names to sql types
   * @param colNames - ordered list of column names for table.
   * @param sb - StringBuilder to append code to
   */
  private void generateGetFieldMap(Map<String, Integer> columnTypes,
      String [] colNames, StringBuilder sb) {
    int numberOfMethods =
            this.getNumberOfMethods(colNames, maxColumnsPerMethod);

    sb.append("  public Map<String, Object> getFieldMap() {\n");
    sb.append("    Map<String, Object> __sqoop$field_map = "
        + "new TreeMap<String, Object>();\n");
    if (numberOfMethods > 1) {
      for (int i = 0; i < numberOfMethods; ++i) {
        sb.append("    this.getFieldMap" + i + "(__sqoop$field_map);\n");
      }
    } else {
      myGenerateGetFieldMap(columnTypes, colNames, sb, 0,
              maxColumnsPerMethod, false);
    }
    sb.append("    return __sqoop$field_map;\n");
    sb.append("  }\n\n");

    for (int i = 0; i < numberOfMethods; ++i) {
      myGenerateGetFieldMap(columnTypes, colNames, sb, i,
              maxColumnsPerMethod, true);
    }
  }

  /**
   * Generate the getFieldMap() method.
   * @param columnTypes - mapping from column names to sql types
   * @param colNames - ordered list of column names for table.
   * @param sb - StringBuilder to append code to
   * @param methodNumber - method number
   * @param size - number of columns per method
   * @param wrapInMethod - wrap body in a method.
   */
  private void myGenerateGetFieldMap(Map<String, Integer> columnTypes,
                                     String[] colNames, StringBuilder sb,
                                     int methodNumber, int size,
                                     boolean wrapInMethod) {
    if (wrapInMethod) {
      sb.append("  public void getFieldMap" + methodNumber
              + "(Map<String, Object> __sqoop$field_map) {\n");
    }

    for (int i = methodNumber * size;
         i < topBoundary(colNames, methodNumber, size); ++i) {
      String colName = colNames[i];
      sb.append("    __sqoop$field_map.put(\"" + colName + "\", this."
          + colName + ");\n");
    }

    if (wrapInMethod) {
      sb.append("  }\n\n");
    }
  }

  /**
   * Generate the toString() method.
   * @param columnTypes - mapping from column names to sql types
   * @param colNames - ordered list of column names for table.
   * @param sb - StringBuilder to append code to
   */
  private void generateToString(Map<String, Integer> columnTypes,
      String [] colNames, StringBuilder sb) {

    int numberOfMethods =
            this.getNumberOfMethods(colNames, maxColumnsPerMethod);

    // Save the delimiters to the class.
    sb.append("  private static final DelimiterSet __outputDelimiters = ");
    sb.append(options.getOutputDelimiters().formatConstructor() + ";\n");

    // The default toString() method itself follows. This just calls
    // the delimiter-specific toString() with the default delimiters.
    // Also appends an end-of-record delimiter to the line.
    sb.append("  public String toString() {\n");
    sb.append("    return toString(__outputDelimiters, true);\n");
    sb.append("  }\n");

    // This toString() variant, though, accepts delimiters as arguments.
    sb.append("  public String toString(DelimiterSet delimiters) {\n");
    sb.append("    return toString(delimiters, true);\n");
    sb.append("  }\n");

    // This variant allows the user to specify whether or not an end-of-record
    // delimiter should be appended.
    sb.append("  public String toString(boolean useRecordDelim) {\n");
    sb.append("    return toString(__outputDelimiters, useRecordDelim);\n");
    sb.append("  }\n");


    // This toString() variant allows the user to specify delimiters, as well
    // as whether or not the end-of-record delimiter should be added to the
    // string.  Use 'false' to do reasonable things with TextOutputFormat,
    // which appends its own newline.
    sb.append("  public String toString(DelimiterSet delimiters, ");
    sb.append("boolean useRecordDelim) {\n");
    sb.append("    StringBuilder __sb = new StringBuilder();\n");
    sb.append("    char fieldDelim = delimiters.getFieldsTerminatedBy();\n");

    if (numberOfMethods > 1) {
      for (int i = 0; i < numberOfMethods; ++i) {
        sb.append("    this.toString" + i
                + "(delimiters, __sb, fieldDelim);\n");
      }
    } else {
      myGenerateToString(columnTypes, colNames, sb, true, 0,
              maxColumnsPerMethod, false);
    }

    sb.append("    if (useRecordDelim) {\n");
    sb.append("      __sb.append(delimiters.getLinesTerminatedBy());\n");
    sb.append("    }\n");
    sb.append("    return __sb.toString();\n");
    sb.append("  }\n");

    boolean first = true;
    for (int i = 0; i < numberOfMethods; ++i) {
      myGenerateToString(columnTypes, colNames, sb, first, i,
              maxColumnsPerMethod, true);
      first = false;
    }
  }

  /**
   * Generate the toString() method.
   * @param columnTypes - mapping from column names to sql types
   * @param colNames - ordered list of column names for table.
   * @param sb - StringBuilder to append code to
   * @param methodNumber - method number
   * @param size - number of columns per method
   * @param wrapInMethod - wrap body in a method.
   */
  private void myGenerateToString(Map<String, Integer> columnTypes,
                                  String[] colNames, StringBuilder sb,
                                  boolean first, int methodNumber, int size,
                                  boolean wrapInMethod) {
    // This toString() variant allows the user to specify delimiters, as well
    // as whether or not the end-of-record delimiter should be added to the
    // string.  Use 'false' to do reasonable things with TextOutputFormat,
    // which appends its own newline.
    if (wrapInMethod) {
      sb.append("  public void toString" + methodNumber
              + "(DelimiterSet delimiters, ");
      sb.append("StringBuilder __sb, char fieldDelim) {\n");
    }

    for (int i = methodNumber * size;
         i < topBoundary(colNames, methodNumber, size); ++i) {
      String col = colNames[i];
      int sqlType = columnTypes.get(col);
      String javaType = toJavaType(col, sqlType);
      if (null == javaType) {
        LOG.error("No Java type for SQL type " + sqlType
            + " for column " + col);
        continue;
      }

      if (!first) {
        // print inter-field tokens.
        sb.append("    __sb.append(fieldDelim);\n");
      }

      first = false;

      String stringExpr = stringifierForType(javaType, col);
      if (null == stringExpr) {
        LOG.error("No toString method for Java type " + javaType);
        continue;
      }

      if (javaType.equals("String") && options.doHiveDropDelims()) {
        sb.append("    // special case for strings hive, dropping"
            + "delimiters \\n,\\r,\\01 from strings\n");
        sb.append("    __sb.append(FieldFormatter.hiveStringDropDelims("
            + stringExpr + ", delimiters));\n");
      } else if (javaType.equals("String")
          && options.getHiveDelimsReplacement() != null) {
        sb.append("    // special case for strings hive, replacing "
            + "delimiters \\n,\\r,\\01 with '"
            + options.getHiveDelimsReplacement() + "' from strings\n");
        sb.append("    __sb.append(FieldFormatter.hiveStringReplaceDelims("
            + stringExpr + ", \"" + options.getHiveDelimsReplacement() + "\", "
            + "delimiters));\n");
      } else {
        sb.append("    __sb.append(FieldFormatter.escapeAndEnclose("
            + stringExpr + ", delimiters));\n");
      }
    }

    if (wrapInMethod) {
      sb.append("  }\n");
    }
  }

  /**
   * Helper method for generateParser(). Writes out the parse() method for one
   * particular type we support as an input string-ish type.
   */
  private void generateParseMethod(String typ, StringBuilder sb) {
    sb.append("  public void parse(" + typ + " __record) "
        + "throws RecordParser.ParseError {\n");
    sb.append("    if (null == this.__parser) {\n");
    sb.append("      this.__parser = new RecordParser(__inputDelimiters);\n");
    sb.append("    }\n");
    sb.append("    List<String> __fields = "
        + "this.__parser.parseRecord(__record);\n");
    sb.append("    __loadFromFields(__fields);\n");
    sb.append("  }\n\n");
  }

  /**
   * Helper method for parseColumn(). Interpret the string null representation
   * for a particular column.
   */
  private void parseNullVal(String javaType, String colName, StringBuilder sb) {
    if (javaType.equals("String")) {
      sb.append("    if (__cur_str.equals(\""
         + this.options.getInNullStringValue() + "\")) { this.");
      sb.append(colName);
      sb.append(" = null; } else {\n");
    } else {
      sb.append("    if (__cur_str.equals(\""
         + this.options.getInNullNonStringValue());
      sb.append("\") || __cur_str.length() == 0) { this.");
      sb.append(colName);
      sb.append(" = null; } else {\n");
    }
  }

  /**
   * Helper method for generateParser(). Generates the code that loads one
   * field of a specified name and type from the next element of the field
   * strings list.
   */
  private void parseColumn(String colName, int colType, StringBuilder sb) {
    // assume that we have __it and __cur_str vars, based on
    // __loadFromFields() code.
    sb.append("    __cur_str = __it.next();\n");
    String javaType = toJavaType(colName, colType);

    parseNullVal(javaType, colName, sb);
    if (javaType.equals("String")) {
      // TODO(aaron): Distinguish between 'null' and null. Currently they both
      // set the actual object to null.
      sb.append("      this." + colName + " = __cur_str;\n");
    } else if (javaType.equals("Integer")) {
      sb.append("      this." + colName + " = Integer.valueOf(__cur_str);\n");
    } else if (javaType.equals("Long")) {
      sb.append("      this." + colName + " = Long.valueOf(__cur_str);\n");
    } else if (javaType.equals("Float")) {
      sb.append("      this." + colName + " = Float.valueOf(__cur_str);\n");
    } else if (javaType.equals("Double")) {
      sb.append("      this." + colName + " = Double.valueOf(__cur_str);\n");
    } else if (javaType.equals("Boolean")) {
      sb.append("      this." + colName
          + " = BooleanParser.valueOf(__cur_str);\n");
    } else if (javaType.equals("java.sql.Date")) {
      sb.append("      this." + colName
          + " = java.sql.Date.valueOf(__cur_str);\n");
    } else if (javaType.equals("java.sql.Time")) {
      sb.append("      this." + colName
          + " = java.sql.Time.valueOf(__cur_str);\n");
    } else if (javaType.equals("java.sql.Timestamp")) {
      sb.append("      this." + colName
          + " = java.sql.Timestamp.valueOf(__cur_str);\n");
    } else if (javaType.equals("java.math.BigDecimal")) {
      sb.append("      this." + colName
          + " = new java.math.BigDecimal(__cur_str);\n");
    } else if (javaType.equals(ClobRef.class.getName())) {
      sb.append("      this." + colName + " = ClobRef.parse(__cur_str);\n");
    } else if (javaType.equals(BlobRef.class.getName())) {
      sb.append("      this." + colName + " = BlobRef.parse(__cur_str);\n");
    } else if (javaType.equals(BytesWritable.class.getName())) {
      // Get the unsigned byte[] from the hex string representation
      // We cannot use Byte.parse() which always assumes a signed decimal byte
      sb.append("      String[] strByteVal = __cur_str.trim().split(\" \");\n");
      sb.append("      byte [] byteVal = new byte[strByteVal.length];\n");
      sb.append("      for (int i = 0; i < byteVal.length; ++i) {\n");
      sb.append("          byteVal[i] = "
              + "(byte)Integer.parseInt(strByteVal[i], 16);\n");
      sb.append("      }\n");
      sb.append("      this." + colName + " = new BytesWritable(byteVal);\n");
    } else {
      LOG.error("No parser available for Java type " + javaType);
    }

    sb.append("    }\n\n"); // the closing '{' based on code in parseNullVal();
  }

  /**
   * Generate the parse() method.
   * @param columnTypes - mapping from column names to sql types
   * @param colNames - ordered list of column names for table.
   * @param sb - StringBuilder to append code to
   */
  private void generateParser(Map<String, Integer> columnTypes,
      String [] colNames, StringBuilder sb) {

    int numberOfMethods =
            this.getNumberOfMethods(colNames, maxColumnsPerMethod);

    // Embed into the class the delimiter characters to use when parsing input
    // records.  Note that these can differ from the delims to use as output
    // via toString(), if the user wants to use this class to convert one
    // format to another.
    sb.append("  private static final DelimiterSet __inputDelimiters = ");
    sb.append(options.getInputDelimiters().formatConstructor() + ";\n");

    // The parser object which will do the heavy lifting for field splitting.
    sb.append("  private RecordParser __parser;\n");

    // Generate wrapper methods which will invoke the parser.
    generateParseMethod("Text", sb);
    generateParseMethod("CharSequence", sb);
    generateParseMethod("byte []", sb);
    generateParseMethod("char []", sb);
    generateParseMethod("ByteBuffer", sb);
    generateParseMethod("CharBuffer", sb);

    // The wrapper methods call __loadFromFields() to actually interpret the
    // raw field data as string, int, boolean, etc. The generation of this
    // method is type-dependent for the fields.
    sb.append("  private void __loadFromFields(List<String> fields) {\n");
    sb.append("    Iterator<String> __it = fields.listIterator();\n");
    if (numberOfMethods > 1) {
      for (int i = 0; i < numberOfMethods; ++i) {
        sb.append("    this.__loadFromFields" + i + "(__it);\n");
      }
    } else {
      myGenerateParser(columnTypes, colNames, sb, 0,
              maxColumnsPerMethod, false);
    }
    sb.append("  }\n\n");

    for (int i = 0; i < numberOfMethods; ++i) {
      myGenerateParser(columnTypes, colNames, sb, i,
              maxColumnsPerMethod, true);
    }
  }

  /**
   * Generate the parse() method.
   * @param columnTypes - mapping from column names to sql types
   * @param colNames - ordered list of column names for table.
   * @param sb - StringBuilder to append code to
   * @param methodNumber - method number
   * @param size - number of columns per method
   * @param wrapInMethod - wrap body in a method.
   */
  private void myGenerateParser(Map<String, Integer> columnTypes,
                                String[] colNames, StringBuilder sb,
                                int methodNumber, int size,
                                boolean wrapInMethod) {
    // The wrapper methods call __loadFromFields() to actually interpret the
    // raw field data as string, int, boolean, etc. The generation of this
    // method is type-dependent for the fields.
    if (wrapInMethod) {
      sb.append("  private void __loadFromFields" + methodNumber
              + "(Iterator<String> __it) {\n");
    }
    sb.append("    String __cur_str = null;\n");
    sb.append("    try {\n");
    for (int i = methodNumber * size;
         i < topBoundary(colNames, methodNumber, size); ++i) {
      String colName = colNames[i];
      int colType = columnTypes.get(colName);
      parseColumn(colName, colType, sb);
    }
    sb.append("    } catch (RuntimeException e) {");
    sb.append("    throw new RuntimeException("
        + "\"Can't parse input data: '\" + __cur_str + \"'\", e);");
    sb.append("    }");
    if (wrapInMethod) {
      sb.append("  }\n\n");
    }
  }

  /**
   * Generate the write() method used by the Hadoop RPC system.
   * @param columnTypes - mapping from column names to sql types
   * @param colNames - ordered list of column names for table.
   * @param sb - StringBuilder to append code to
   */
  private void generateHadoopWrite(Map<String, Integer> columnTypes,
      String [] colNames, StringBuilder sb) {

    int numberOfMethods =
            this.getNumberOfMethods(colNames, maxColumnsPerMethod);

    sb.append("  public void write(DataOutput __dataOut) "
        + "throws IOException {\n");

    if (numberOfMethods > 1) {
      for (int i = 0; i < numberOfMethods; ++i) {
        sb.append("    this.write" + i + "(__dataOut);\n");
      }
    } else {
      myGenerateHadoopWrite(columnTypes, colNames, sb, 0,
              maxColumnsPerMethod, false);
    }

    sb.append("  }\n");

    for (int i = 0; i < numberOfMethods; ++i) {
      myGenerateHadoopWrite(columnTypes, colNames, sb, i,
              maxColumnsPerMethod, true);
    }
  }

  /**
   * Generate the write() method used by the Hadoop RPC system.
   * @param columnTypes - mapping from column names to sql types
   * @param colNames - ordered list of column names for table.
   * @param sb - StringBuilder to append code to
   * @param methodNumber - method number
   * @param size - number of columns per method
   * @param wrapInMethod - wrap body in a method.
   */
  private void myGenerateHadoopWrite(Map<String, Integer> columnTypes,
                                     String[] colNames, StringBuilder sb,
                                     int methodNumber, int size,
                                     boolean wrapInMethod) {
    if (wrapInMethod) {
      sb.append("  public void write" + methodNumber + "(DataOutput __dataOut) "
          + "throws IOException {\n");
    }

    for (int i = methodNumber * size;
         i < topBoundary(colNames, methodNumber, size); ++i) {
      String col = colNames[i];
      int sqlType = columnTypes.get(col);
      String javaType = toJavaType(col, sqlType);
      if (null == javaType) {
        LOG.error("No Java type for SQL type " + sqlType
            + " for column " + col);
        continue;
      }

      String setterMethod = rpcSetterForMaybeNull(javaType, "__dataOut", col);
      if (null == setterMethod) {
        LOG.error("No RPC setter method for Java type " + javaType);
        continue;
      }

      sb.append(setterMethod);
    }

    if (wrapInMethod) {
      sb.append("  }\n");
    }
  }

  /**
   * Create a list of identifiers to use based on the true column names
   * of the table.
   * @param colNames the actual column names of the table.
   * @return a list of column names in the same order which are
   * cleaned up to be used as identifiers in the generated Java class.
   */
  private String [] cleanColNames(String [] colNames) {
    String [] cleanedColNames = new String[colNames.length];
    for (int i = 0; i < colNames.length; i++) {
      String col = colNames[i];
      String identifier = toJavaIdentifier(col);
      cleanedColNames[i] = identifier;
    }

    return cleanedColNames;
  }

  /**
   * Made this a separate method to overcome the 150 line limit of checkstyle.
   */
  private void logORMSelfGenerationMessage() {
    LOG.info("The connection manager declares that it self manages mapping"
        + " between records & fields and rows & columns.  No class will"
        + " will be generated.");
  }

  /**
   * Generate the ORM code for the class.
   */
  public void generate() throws IOException {
    Map<String, Integer> columnTypes = getColumnTypes();
    if (connManager.isORMFacilitySelfManaged()) {
      logORMSelfGenerationMessage();
      return;
    }
    if (columnTypes == null) {
      throw new IOException("No columns to generate for ClassWriter");
    }

    String[] colNames = getColumnNames(columnTypes);

    // Column number should be more than 0
    if (colNames == null || colNames.length == 0) {
      throw new IllegalArgumentException("There is no column found in the "
              + "target table " + tableName
              + ". Please ensure that your table name is correct.");
    }

    // Translate all the column names into names that are safe to
    // use as identifiers.
    String [] cleanedColNames = cleanColNames(colNames);
    Set<String> uniqColNames = new HashSet<String>();
    for (int i = 0; i < colNames.length; i++) {
      String identifier = cleanedColNames[i];

      if (identifier.isEmpty()) { // Name can't be blank
        throw new IllegalArgumentException("We found column without column "
                + "name. Please verify that you've entered all column names "
                + "in your query if using free form query import (consider "
                + "adding clause AS if you're using column transformation)");
      }
      // Guarantee uniq col identifier
      if (uniqColNames.contains(identifier)) {
          throw new IllegalArgumentException("Duplicate Column identifier "
              + "specified: '" + identifier + "'");
      }
      uniqColNames.add(identifier);
      // Make sure the col->type mapping holds for the new identifier name, too
      String col = colNames[i];
      Integer type = columnTypes.get(col);
      if (type == null) {
        // column doesn't have a type, means that is illegal column name!
        throw new IllegalArgumentException("Column name '" + col
            + "' not in table");
      }
      columnTypes.put(identifier, type);
    }

    // Check that all explicitly mapped columns are present in result set
    Properties mapping = options.getMapColumnJava();
    if (mapping != null && !mapping.isEmpty()) {
      for(Object column : mapping.keySet()) {
        if (!uniqColNames.contains((String)column)) {
        throw new IllegalArgumentException(
            "No column by the name "
            + column
            + "found while importing data; expecting one of "
            + uniqColNames);
        }
      }
    }

    // The db write() method may use column names in a different
    // order. If this is set in the options, pull it out here and
    // make sure we format the column names to identifiers in the same way
    // as we do for the ordinary column list.
    String [] dbWriteColNames = options.getDbOutputColumns();
    String [] cleanedDbWriteColNames = null;
    if (null == dbWriteColNames) {
      cleanedDbWriteColNames = cleanedColNames;
    } else {
      cleanedDbWriteColNames = cleanColNames(dbWriteColNames);
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("selected columns:");
      for (String col : cleanedColNames) {
        LOG.debug("  " + col);
      }

      if (cleanedDbWriteColNames != cleanedColNames) {
        // dbWrite() has a different set of columns than the rest of the
        // generators.
        LOG.debug("db write column order:");
        for (String dbCol : cleanedDbWriteColNames) {
          LOG.debug("  " + dbCol);
        }
      }
    }

    // Generate the Java code.
    StringBuilder sb = generateClassForColumns(columnTypes,
        cleanedColNames, cleanedDbWriteColNames);
    // Write this out to a file in the jar output directory.
    // We'll move it to the user-visible CodeOutputDir after compiling.
    String codeOutDir = options.getJarOutputDir();
    // Get the class name to generate, which includes package components.
    String className = new TableClassName(options).getClassForTable(tableName);
    // Convert the '.' characters to '/' characters.
    String sourceFilename = className.replace('.', File.separatorChar)
        + ".java";
    String filename = codeOutDir + sourceFilename;

    if (LOG.isDebugEnabled()) {
      LOG.debug("Writing source file: " + filename);
      LOG.debug("Table name: " + tableName);
      StringBuilder sbColTypes = new StringBuilder();
      for (String col : colNames) {
        Integer colType = columnTypes.get(col);
        sbColTypes.append(col + ":" + colType + ", ");
      }
      String colTypeStr = sbColTypes.toString();
      LOG.debug("Columns: " + colTypeStr);
      LOG.debug("sourceFilename is " + sourceFilename);
    }

    compileManager.addSourceFile(sourceFilename);

    // Create any missing parent directories.
    File file = new File(filename);
    File dir = file.getParentFile();
    if (null != dir && !dir.exists()) {
      boolean mkdirSuccess = dir.mkdirs();
      if (!mkdirSuccess) {
        LOG.debug("Could not create directory tree for " + dir);
      }
    }

    OutputStream ostream = null;
    Writer writer = null;
    try {
      ostream = new FileOutputStream(filename);
      writer = new OutputStreamWriter(ostream);
      writer.append(sb.toString());
    } finally {
      if (null != writer) {
        try {
          writer.close();
        } catch (IOException ioe) { // ignored because we're closing.
        }
      }
      if (null != ostream) {
        try {
          ostream.close();
        } catch (IOException ioe) { // ignored because we're closing.
        }
      }
    }
  }

  protected String[] getColumnNames(Map<String, Integer> columnTypes) {
    String [] colNames = options.getColumns();
    if (null == colNames) {
      if (null != tableName) {
        // Table-based import. Read column names from table.
        colNames = connManager.getColumnNames(tableName);
      } else if (options.getCall() != null) {
        // Read procedure arguments from metadata
        colNames = connManager.getColumnNamesForProcedure(
            this.options.getCall());
      } else {
        // Infer/assign column names for arbitrary query.
        colNames = connManager.getColumnNamesForQuery(
            this.options.getSqlQuery());
      }
    } else {
      // These column names were provided by the user. They may not be in
      // the same case as the keys in the columnTypes map. So make sure
      // we add the appropriate aliases in that map.
      for (String userColName : colNames) {
        for (Map.Entry<String, Integer> typeEntry : columnTypes.entrySet()) {
          String typeColName = typeEntry.getKey();
          if (typeColName.equalsIgnoreCase(userColName)
              && !typeColName.equals(userColName)) {
            // We found the correct-case equivalent.
            columnTypes.put(userColName, typeEntry.getValue());
            // No need to continue iteration; only one could match.
            // Also, the use of put() just invalidated the iterator.
            break;
          }
        }
      }
    }
    return colNames;
  }

  protected Map<String, Integer> getColumnTypes() throws IOException {
    if (options.getCall() == null) {
      return connManager.getColumnTypes(tableName, options.getSqlQuery());
    } else {
      return connManager.getColumnTypesForProcedure(options.getCall());
    }
  }

  /**
   * Generate the ORM code for a table object containing the named columns.
   * @param columnTypes - mapping from column names to sql types
   * @param colNames - ordered list of column names for table.
   * @param dbWriteColNames - ordered list of column names for the db
   * write() method of the class.
   * @return - A StringBuilder that contains the text of the class code.
   */
  private StringBuilder generateClassForColumns(
      Map<String, Integer> columnTypes,
      String [] colNames, String [] dbWriteColNames) {
    if (colNames.length ==0) {
      throw new IllegalArgumentException("Attempted to generate class with "
          + "no columns!");
    }
    StringBuilder sb = new StringBuilder();
    sb.append("// ORM class for table '" + tableName + "'\n");
    sb.append("// WARNING: This class is AUTO-GENERATED. "
        + "Modify at your own risk.\n");
    sb.append("//\n");
    sb.append("// Debug information:\n");
    sb.append("// Generated date: " + (new Date()) + "\n");
    sb.append("// For connector: " + connManager.getClass().getCanonicalName()
      + "\n");

    TableClassName tableNameInfo = new TableClassName(options);

    String packageName = tableNameInfo.getPackageForTable();
    if (null != packageName) {
      sb.append("package ");
      sb.append(packageName);
      sb.append(";\n");
    }

    sb.append("import org.apache.hadoop.io.BytesWritable;\n");
    sb.append("import org.apache.hadoop.io.Text;\n");
    sb.append("import org.apache.hadoop.io.Writable;\n");
    sb.append("import org.apache.hadoop.mapred.lib.db.DBWritable;\n");
    sb.append("import " + JdbcWritableBridge.class.getCanonicalName() + ";\n");
    sb.append("import " + DelimiterSet.class.getCanonicalName() + ";\n");
    sb.append("import " + FieldFormatter.class.getCanonicalName() + ";\n");
    sb.append("import " + RecordParser.class.getCanonicalName() + ";\n");
    sb.append("import " + BooleanParser.class.getCanonicalName() + ";\n");
    sb.append("import " + BlobRef.class.getCanonicalName() + ";\n");
    sb.append("import " + ClobRef.class.getCanonicalName() + ";\n");
    sb.append("import " + LargeObjectLoader.class.getCanonicalName() + ";\n");
    sb.append("import " + SqoopRecord.class.getCanonicalName() + ";\n");
    sb.append("import java.sql.PreparedStatement;\n");
    sb.append("import java.sql.ResultSet;\n");
    sb.append("import java.sql.SQLException;\n");
    sb.append("import java.io.DataInput;\n");
    sb.append("import java.io.DataOutput;\n");
    sb.append("import java.io.IOException;\n");
    sb.append("import java.nio.ByteBuffer;\n");
    sb.append("import java.nio.CharBuffer;\n");
    sb.append("import java.sql.Date;\n");
    sb.append("import java.sql.Time;\n");
    sb.append("import java.sql.Timestamp;\n");
    sb.append("import java.util.Arrays;\n");
    sb.append("import java.util.Iterator;\n");
    sb.append("import java.util.List;\n");
    sb.append("import java.util.Map;\n");
    sb.append("import java.util.TreeMap;\n");
    sb.append("\n");

    String className = tableNameInfo.getShortClassForTable(tableName);
    sb.append("public class " + className + " extends SqoopRecord "
        + " implements DBWritable, Writable {\n");
    sb.append("  private final int PROTOCOL_VERSION = "
        + CLASS_WRITER_VERSION + ";\n");
    sb.append(
        "  public int getClassFormatVersion() { return PROTOCOL_VERSION; }\n");
    sb.append("  protected ResultSet __cur_result_set;\n");
    generateFields(columnTypes, colNames, className, sb);
    generateEquals(columnTypes, colNames, className, sb);
    generateDbRead(columnTypes, colNames, sb);
    generateLoadLargeObjects(columnTypes, colNames, sb);
    generateDbWrite(columnTypes, dbWriteColNames, sb);
    generateHadoopRead(columnTypes, colNames, sb);
    generateHadoopWrite(columnTypes, colNames, sb);
    generateToString(columnTypes, colNames, sb);
    generateParser(columnTypes, colNames, sb);
    generateCloneMethod(columnTypes, colNames, sb);
    generateGetFieldMap(columnTypes, colNames, sb);
    generateSetField(columnTypes, colNames, sb);

    // TODO(aaron): Generate hashCode(), compareTo(), equals() so it can be a
    // WritableComparable

    sb.append("}\n");

    return sb;
  }
}
