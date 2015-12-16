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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.security.CodeSource;
import java.security.ProtectionDomain;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.jar.Attributes;
import java.util.jar.Attributes.Name;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import java.util.jar.JarInputStream;
import java.util.jar.Manifest;

import org.apache.log4j.Logger;

import sun.misc.CompoundEnumeration;

/**
 * A {@link URLClassLoader} for connector isolation. Classes from the
 * connector JARs are loaded in preference to the parent loader.
 */
public class ConnectorClassLoader extends URLClassLoader {
  /**
   * Default value of the system classes if the user did not override them.
   * JDK classes, sqoop classes and resources, and some select third-party
   * classes are considered system classes, and are not loaded by the
   * connector classloader.
   */
  public static final String SYSTEM_CLASSES_DEFAULT;

  private static final String PROPERTIES_FILE =
      "org.apache.sqoop.connector-classloader.properties";
  private static final String SYSTEM_CLASSES_DEFAULT_KEY =
      "system.classes.default";

  private static final String MANIFEST = "META-INF/MANIFEST.MF";
  private static final String CLASS = ".class";
  private static final String LIB_PREFIX = "lib/";

  private Map<String, ByteCode> byteCodeCache = new HashMap<String, ByteCode>();
  private Set<String> jarNames = new LinkedHashSet<String>();
  private Map<String, ProtectionDomain> pdCache = new HashMap<String, ProtectionDomain>();
  private URLFactory urlFactory;

  private static final Logger LOG = Logger.getLogger(ConnectorClassLoader.class);

  private static final FilenameFilter JAR_FILENAME_FILTER =
    new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        return name.endsWith(".jar") || name.endsWith(".JAR");
      }
  };

  private static class ByteCode {
    public byte[] bytes;
    public String codebase;
    public Manifest manifest;

    public ByteCode(byte[] bytes,
        String codebase, Manifest manifest) {
      this.bytes = bytes;
      this.codebase = codebase;
      this.manifest = manifest;
    }
  }

  static {
    try (InputStream is = ConnectorClassLoader.class.getClassLoader()
        .getResourceAsStream(PROPERTIES_FILE);) {
      if (is == null) {
        throw new ExceptionInInitializerError("properties file " +
            PROPERTIES_FILE + " is not found");
      }
      Properties props = new Properties();
      props.load(is);
      // get the system classes default
      String systemClassesDefault =
          props.getProperty(SYSTEM_CLASSES_DEFAULT_KEY);
      if (systemClassesDefault == null) {
        throw new ExceptionInInitializerError("property " +
            SYSTEM_CLASSES_DEFAULT_KEY + " is not found");
      }
      SYSTEM_CLASSES_DEFAULT = systemClassesDefault;
    } catch (IOException e) {
      throw new ExceptionInInitializerError(e);
    }
  }

  private final ClassLoader parent;
  private final List<String> systemClasses;

  public ConnectorClassLoader(URL[] urls, ClassLoader parent,
      List<String> systemClasses, boolean overrideDefaultSystemClasses) throws IOException {
    super(urls, parent);
    if (LOG.isDebugEnabled()) {
      LOG.debug("urls: " + Arrays.toString(urls));
      LOG.debug("system classes: " + systemClasses);
    }
    this.parent = parent;
    if (parent == null) {
      throw new IllegalArgumentException("No parent classloader!");
    }
    // if the caller-specified system classes are null or empty, use the default
    this.systemClasses = new ArrayList<String>();
    if (systemClasses != null && !systemClasses.isEmpty()) {
      this.systemClasses.addAll(systemClasses);
    }
    if (!overrideDefaultSystemClasses || this.systemClasses.isEmpty()) {
      this.systemClasses.addAll(Arrays.asList(SYSTEM_CLASSES_DEFAULT.split("\\s*,\\s*")));
    }
    LOG.info("system classes: " + this.systemClasses);

    urlFactory = new ConnectorURLFactory(this);
    load(urls);
  }

  public ConnectorClassLoader(String classpath, ClassLoader parent,
      List<String> systemClasses) throws IOException {
    this(constructUrlsFromClasspath(classpath), parent, systemClasses, true);
  }

  public ConnectorClassLoader(String classpath, ClassLoader parent,
      List<String> systemClasses, boolean overrideDefaultSystemClasses) throws IOException {
    this(constructUrlsFromClasspath(classpath), parent, systemClasses, overrideDefaultSystemClasses);
  }

  static URL[] constructUrlsFromClasspath(String classpath)
      throws MalformedURLException {
    List<URL> urls = new ArrayList<URL>();
    for (String element : classpath.split(File.pathSeparator)) {
      if (element.endsWith("/*")) {
        String dir = element.substring(0, element.length() - 1);
        File[] files = new File(dir).listFiles(JAR_FILENAME_FILTER);
        if (files != null) {
          for (File file : files) {
            urls.add(file.toURI().toURL());
          }
        }
      } else {
        File file = new File(element);
        if (file.exists()) {
          urls.add(new File(element).toURI().toURL());
        }
      }
    }
    return urls.toArray(new URL[urls.size()]);
  }

  @Override
  public URL getResource(String name) {
    URL url = null;

    if (!isSystemClass(name, systemClasses)) {
      url= findResource(name);
      if (url == null && name.startsWith("/")) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Remove leading / off " + name);
        }
        url= findResource(name.substring(1));
      }
    }

    if (url == null) {
      url= parent.getResource(name);
    }

    if (url != null) {
      if (LOG.isDebugEnabled()) {
        LOG.debug("getResource("+name+")=" + url);
      }
    }

    return url;
  }

  @SuppressWarnings("rawtypes")
  @Override
  public Enumeration<URL> getResources(String name) throws IOException {
    Enumeration[] tmp = new Enumeration[2];

    if (!isSystemClass(name, systemClasses)) {
      tmp[0]= findResources(name);
      if (!tmp[0].hasMoreElements() && name.startsWith("/")) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Remove leading / off " + name);
        }
        tmp[0]= findResources(name.substring(1));
      }
    }

    tmp[1]= parent.getResources(name);

    return new CompoundEnumeration<>(tmp);
  }

  @Override
  public Class<?> loadClass(String name) throws ClassNotFoundException {
    return this.loadClass(name, false);
  }

  @Override
  protected synchronized Class<?> loadClass(String name, boolean resolve)
      throws ClassNotFoundException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Loading class: " + name);
    }

    Class<?> c = findLoadedClass(name);
    ClassNotFoundException ex = null;

    if (c == null && !isSystemClass(name, systemClasses)) {
      // Try to load class from this classloader's URLs. Note that this is like
      // the servlet spec, not the usual Java 2 behaviour where we ask the
      // parent to attempt to load first.
      try {
        c = findClass(name);
        if (LOG.isDebugEnabled() && c != null) {
          LOG.debug("Loaded class: " + name + " ");
        }
      } catch (ClassNotFoundException e) {
        if (LOG.isDebugEnabled()) {
          LOG.debug(e);
        }
        ex = e;
      }
    }

    if (c == null) { // try parent
      c = parent.loadClass(name);
      if (LOG.isDebugEnabled() && c != null) {
        LOG.debug("Loaded class from parent: " + name + " ");
      }
    }

    if (c == null) {
      throw ex != null ? ex : new ClassNotFoundException(name);
    }

    if (resolve) {
      resolveClass(c);
    }

    return c;
  }

  @Override
  protected Class<?> findClass(String name) throws ClassNotFoundException {
    // Make sure not to load duplicate classes.
    Class<?> cls = findLoadedClass(name);
    if (cls != null) {
      return cls;
    }

    // Look up the class in the byte codes.
    String cache = name.replace('.', '/') + CLASS;
    cache = resolve(cache);
    if (cache != null) {
      ByteCode bytecode = byteCodeCache.get(cache);
      // Use a protectionDomain to associate the codebase with the class.
      ProtectionDomain pd = pdCache.get(bytecode.codebase);
      if (pd == null) {
        try {
          URL url = urlFactory.getCodeBase(bytecode.codebase);
          CodeSource source = new CodeSource(url, (java.security.cert.Certificate[]) null);
          pd = new ProtectionDomain(source, null, this, null);
          pdCache.put(bytecode.codebase, pd);
        } catch (MalformedURLException mux) {
          throw new ClassNotFoundException(name, mux);
        }
      }

      byte bytes[] = bytecode.bytes;
      int i = name.lastIndexOf('.');
      if (i != -1) {
        String pkgname = name.substring(0, i);
        // Check if package already loaded.
        Package pkg = getPackage(pkgname);
        Manifest man = bytecode.manifest;
        if (pkg != null) {
          // Package found, so check package sealing.
          if (pkg.isSealed()) {
            // Verify that code source URL is the same.
            if (!pkg.isSealed(pd.getCodeSource().getLocation())) {
              throw new SecurityException("sealing violation: package " + pkgname + " is sealed");
            }
          } else {
            // Make sure we are not attempting to seal the package at this code source URL.
            if ((man != null) && isSealed(pkgname, man)) {
              throw new SecurityException("sealing violation: can't seal package " + pkgname + ": already loaded");
            }
          }
        } else {
          if (man != null) {
            definePackage(pkgname, man, pd.getCodeSource().getLocation());
          } else {
            definePackage(pkgname, null, null, null, null, null, null, null);
          }
        }
      }

      return defineClass(name, bytes, 0, bytes.length, pd);
    }

    return null;
  }

  @Override
  public URL findResource(final String name) {
    LOG.debug("Finding resource: " + name);

    try {
      // Do we have the named resource in our cache? If so, construct a 'sqoopconnector:' URL.
      String resource = resolve(name);
      if (resource != null) {
        ByteCode entry = byteCodeCache.get(resource);
        return urlFactory.getURL(entry.codebase, name);
      }
    } catch (MalformedURLException mux) {
      LOG.debug("Unable to find resource: " + name + " due to " + mux);
    }

    return null;
  }

  @Override
  @edu.umd.cs.findbugs.annotations.SuppressWarnings("SIC_INNER_SHOULD_BE_STATIC_ANON")
  public Enumeration<URL> findResources(final String name) throws IOException {
    LOG.debug("Finding resources: " + name);

    final List<URL> resources = new ArrayList<URL>();
    ByteCode entry = byteCodeCache.get(name);
    if (entry != null) {
      URL url = urlFactory.getURL(entry.codebase, name);
      LOG.debug("Adding " + url + " to resources list for " + name);
      resources.add(url);
    }

    Iterator<String> jarNameIter = jarNames.iterator();
    while (jarNameIter.hasNext()) {
      String resource = jarNameIter.next() + "/" + name;
      entry = byteCodeCache.get(resource);
      if (entry != null) {
        URL url = urlFactory.getURL(entry.codebase, name);
        LOG.debug("Adding " + url + " to resources list for " + name);
        resources.add(url);
      }
    }

    final Iterator<URL> resIter = resources.iterator();
    return new Enumeration<URL>() {
      public boolean hasMoreElements() {
        return resIter.hasNext();
      }
      public URL nextElement() {
        return resIter.next();
      }
    };
  }

  /**
   * Return resources from the appropriate codebase.
   */
  public InputStream getByteStream(String resource) {
    InputStream result = null;

    if (!isSystemClass(resource, systemClasses)) {
      // Make resource canonical (remove ., .., etc).
      resource = canon(resource);

      ByteCode bytecode = null;
      String name = resolve(resource);
      if (name != null) {
        bytecode = byteCodeCache.get(name);
      }

      if (bytecode != null) {
        result = new ByteArrayInputStream(bytecode.bytes);
      }

      if (result == null) {
        if (jarNames.contains(resource)) {
          // resource wanted is an actual jar
          LOG.debug("Loading resource file directly: " + resource);
          result = super.getResourceAsStream(resource);
        }
      }
    }

    if (result == null) {
      // Delegate to parent.
      result = parent.getResourceAsStream(resource);
    }

    return result;
  }

  private boolean isSealed(String name, Manifest man) {
    String path = name.concat("/");
    Attributes attr = man.getAttributes(path);
    String sealed = null;
    if (attr != null) {
      sealed = attr.getValue(Name.SEALED);
    }
    if (sealed == null) {
      if ((attr = man.getMainAttributes()) != null) {
        sealed = attr.getValue(Name.SEALED);
      }
    }
    return "true".equalsIgnoreCase(sealed);
  }

  /**
   * Make a path canonical, removing . and ..
   */
  private String canon(String path) {
    path = path.replaceAll("/\\./", "/");
    String canon = path;
    String next;
    do {
      next = canon;
      canon = canon.replaceFirst("([^/]*/\\.\\./)", "");
    } while (!next.equals(canon));
    return canon;
  }

  /**
   * Resolve a resource name.
   */
  private String resolve(String name) {
    if (name.startsWith("/")) {
      name = name.substring(1);
    }

    String resource = null;
    if (byteCodeCache.containsKey(name)) {
      resource = name;
    }

    if (resource == null) {
      Iterator<String> jarNameIter = jarNames.iterator();
      while (jarNameIter.hasNext()) {
        String tmp = jarNameIter.next() + "/" + name;
        if (byteCodeCache.containsKey(tmp)) {
          resource = tmp;
          break;
        }
      }
    }
    return resource;
  }

  private void load(URL[] urls) throws IOException {
    for (URL url : urls) {
      String jarName = url.getPath();
      JarFile jarFile = null;
      try {
        jarFile = new JarFile(jarName);
        Manifest manifest = jarFile.getManifest();

        Enumeration<JarEntry> entryEnum = jarFile.entries();
        while (entryEnum.hasMoreElements()) {
          JarEntry entry = entryEnum.nextElement();
          if (entry.isDirectory()) {
            continue;
          }

          String entryName = entry.getName();
          InputStream is = jarFile.getInputStream(entry);
          if (is == null) {
            throw new IOException("Unable to load resource " + entryName);
          }
          try {
            if (entryName.startsWith(LIB_PREFIX)) {
              LOG.debug("Caching " + entryName);
              loadBytesFromJar(is, entryName);
            } else if (entryName.endsWith(CLASS)) {
              // A plain vanilla class file rooted at the top of the jar file.
              loadBytes(entry, is, "/", manifest);
              LOG.debug("Loaded class: " + jarFile.getName() + "!/" + entry.getName());
            } else {
              // A resource
              loadBytes(entry, is, "/", manifest);
              LOG.debug("Loaded resource: " + jarFile.getName() + "!/" + entry.getName());
            }
          } finally {
            is.close();
          }
        }
      } finally {
        if (jarFile != null) {
          try {
            jarFile.close();
          } catch (IOException e) {
            LOG.debug("Exception closing jarFile: " + jarName, e);
          }
        }
      }
    }
  }

  private void loadBytesFromJar(InputStream is, String jar) throws IOException {
    JarInputStream jis = new JarInputStream(is);
    Manifest manifest = jis.getManifest();
    JarEntry entry = null;
    while ((entry = jis.getNextJarEntry()) != null) {
      loadBytes(entry, jis, jar, manifest);
    }

    if (manifest != null) {
      entry = new JarEntry(MANIFEST);
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      manifest.write(baos);
      ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
      loadBytes(entry, bais, jar, manifest);
    }
  }

  private void loadBytes(JarEntry entry, InputStream is, String jar, Manifest man) throws IOException {
    String entryName = entry.getName();
    int index = entryName.lastIndexOf('.');

    // Add package handling to avoid NullPointerException
    // after calls to getPackage method of this ClassLoader
    int index2 = entryName.lastIndexOf('/', index - 1);
    if (entryName.endsWith(CLASS) && index2 > -1) {
      String packageName = entryName.substring(0, index2).replace('/', '.');
      if (getPackage(packageName) == null) {
        if (man != null) {
          definePackage(packageName, man, urlFactory.getCodeBase(jar));
        } else {
          definePackage(packageName, null, null, null, null, null, null, null);
        }
      }
    }

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    copy(is, baos);

    // If entry is a class, check to see that it hasn't been defined
    // already. Class names must be unique within a classloader because
    // they are cached inside the VM until the classloader is released.
    if (entryName.endsWith(CLASS)) {
      if (byteCodeCache.containsKey(entryName)) {
        return;
      }
      byteCodeCache.put(entryName, new ByteCode(baos.toByteArray(), jar, man));
    } else {
      // Another kind of resource. Cache this by name and prefixed by the jar name.
      String localname = jar + "/" + entryName;
      byte[] bytes = baos.toByteArray();
      byteCodeCache.put(localname, new ByteCode(bytes, jar, man));
      // Keep a set of jar names.
      jarNames.add(jar);
    }
  }

  /**
   * Copying InputStream to OutputStream. Both streams are left open after copy.
   * @param in Source of bytes to copy.
   * @param out Destination of bytes to copy.
   * @throws IOException
   */
  private static void copy(InputStream in, OutputStream out) throws IOException {
    byte[] buf = new byte[1024];
    while (true) {
      int len = in.read(buf);
      if (len < 0) {
        break;
      }
      out.write(buf, 0, len);
    }
  }

  /**
   * Checks if a class should be included as a system class.
   *
   * A class is a system class if and only if the longest match pattern is positive.
   *
   * @param name the class name to check
   * @param systemClasses a list of system class configurations.
   * @return true if the class is a system class
   */
  public static boolean isSystemClass(String name, List<String> systemClasses) {
    boolean result = false;
    if (systemClasses != null) {
      String canonicalName = name.replace('/', '.');
      while (canonicalName.startsWith(".")) {
        canonicalName=canonicalName.substring(1);
      }
      int maxMatchLength = -1;
      for (String c : systemClasses) {
        boolean shouldInclude = true;
        if (c.startsWith("-")) {
          c = c.substring(1);
          shouldInclude = false;
        }
        if (canonicalName.startsWith(c)) {
          if (   c.endsWith(".")                                   // package
              || canonicalName.length() == c.length()              // class
              ||    canonicalName.length() > c.length()            // nested
                 && canonicalName.charAt(c.length()) == '$' ) {
            if (c.length() > maxMatchLength) {
              maxMatchLength = c.length();

              if (shouldInclude) {
                result = true;
              } else {
                result = false;
              }
            } else if (c.length() == maxMatchLength) {
              if (!shouldInclude) {
                result = false;
              }
            }
          }
        }
      }
    }
    return result;
  }
}
