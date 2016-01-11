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
package org.apache.sqoop.classloader;

import java.io.IOException;
import java.io.InputStream;
import java.net.FileNameMap;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLStreamHandler;

/**
 * Generates URLs which are efficient, using the in-memory bytecode to access the resources.
 */
@edu.umd.cs.findbugs.annotations.SuppressWarnings("SIC_INNER_SHOULD_BE_STATIC_ANON")
public class ConnectorURLFactory implements URLFactory {
  private URLStreamHandler handler;

  public ConnectorURLFactory(final ConnectorClassLoader loader) {
    handler = new URLStreamHandler() {
      @Override
      protected URLConnection openConnection(final URL u) throws IOException {
        final String resource = u.getPath();
        return new URLConnection(u) {
          public void connect() {
          }

          public String getContentType() {
            FileNameMap fileNameMap = java.net.URLConnection.getFileNameMap();
            String contentType = fileNameMap.getContentTypeFor(resource);
            if (contentType == null) {
              contentType = "text/plain";
            }
            return contentType;
          }

          public InputStream getInputStream() throws IOException {
            InputStream is = loader.getByteStream(resource);
            if (is == null) {
              throw new IOException("loader.getByteStream() returned null for " + resource);
            }
            return is;
          }
        };
      }
    };
  }

  public URL getURL(String codebase, String resource) throws MalformedURLException {
    String base = resource.endsWith(".class") ? "": codebase + "/";
    URL url =  new URL(null, PROTOCOL + ":/" + base + resource, handler);
    return url;
  }

  public URL getCodeBase(String jar) throws MalformedURLException {
    return new URL(null, PROTOCOL + ":" + jar, handler);
  }
}
