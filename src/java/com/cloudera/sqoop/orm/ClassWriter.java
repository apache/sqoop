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
package com.cloudera.sqoop.orm;

import java.util.HashSet;

import com.cloudera.sqoop.SqoopOptions;
import com.cloudera.sqoop.manager.ConnManager;

/**
 * @deprecated Moving to use org.apache.sqoop namespace.
 */
public class ClassWriter
    extends org.apache.sqoop.orm.ClassWriter {

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
  }

  public static final int CLASS_WRITER_VERSION =
    org.apache.sqoop.orm.ClassWriter.CLASS_WRITER_VERSION;

  public static String toIdentifier(String candidate) {
    return org.apache.sqoop.orm.ClassWriter.toIdentifier(candidate);
  }

  public static String toJavaIdentifier(String candidate) {
    return org.apache.sqoop.orm.ClassWriter.toJavaIdentifier(candidate);
  }

  public static String getIdentifierStrForChar(char c) {
    return org.apache.sqoop.orm.ClassWriter.getIdentifierStrForChar(c);
  }

  public ClassWriter(final SqoopOptions opts, final ConnManager connMgr,
      final String table, final CompilationManager compMgr) {
    super(opts, connMgr, table, compMgr);
  }

}
