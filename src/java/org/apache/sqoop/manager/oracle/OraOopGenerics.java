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

package org.apache.sqoop.manager.oracle;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Generic class to hold list of objects.
 */
public class OraOopGenerics {

  /**
   * Generic class to hold list of objects.
   */
  public static class ObjectList<T> {

    private List<T> objects;

    public ObjectList() {

      this.objects = new ArrayList<T>();
    }

    public void add(T item) {

      this.objects.add(item);
    }

    public int size() {

      return this.objects.size();
    }

    public T get(int index) {

      return this.objects.get(index);
    }

    public Iterator<T> iterator() {

      return this.objects.iterator();
    }

  }

}
