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

package org.apache.sqoop.util;

import java.io.IOException;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RawLocalFileSystem;
import org.junit.Test;
import org.junit.Before;
import static org.junit.Assert.*;

public class TestFileSystemUtil {
  private Configuration conf;

  @Before
  public void setUp() {
    conf = new Configuration();
    conf.set("fs.my.impl", MyFileSystem.class.getName());
  }

  @Test
  public void testMakeQualifiedWhenPathIsNullThenReturnsNull() throws IOException {
    assertNull(FileSystemUtil.makeQualified(null, conf));
  }

  @Test
  public void testMakeQualifiedWhenPathIsRelativeThenReturnDefault() throws IOException {
    Path actual = FileSystemUtil.makeQualified(new Path("foo/bar"), conf);
    assertEquals("file", actual.toUri().getScheme());
  }

  @Test
  public void testMakeQualifiedWhenPathHasCustomSchemaThenReturnSameSchema() throws IOException {
    Path actual = FileSystemUtil.makeQualified(new Path("my:/foo/bar"), conf);
    assertEquals("my", actual.toUri().getScheme());
  }

  @Test(expected = IOException.class)
  public void testMakeQualifiedWhenPathHasBadSchemaThenThrowsIOException() throws IOException {
    FileSystemUtil.makeQualified(new Path("nosuchfs://foo/bar"), conf);
  }

  public static final class MyFileSystem extends RawLocalFileSystem {
    @Override
    public URI getUri() { return URI.create("my:///"); }
  }
}
