/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.sqoop.hbase;

import com.cloudera.sqoop.lib.FieldMappable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.sqoop.util.ExpectedLogMessage;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static java.util.Collections.singletonList;
import static org.mockito.Matchers.anyMap;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class TestHBasePutProcessor {

  @Rule
  public ExpectedLogMessage expectedLogMessage = new ExpectedLogMessage();

  private Configuration configuration;
  private Connection hbaseConnection;
  private PutTransformer putTransformer;
  private BufferedMutator bufferedMutator;
  private FieldMappable fieldMappable;

  private HBasePutProcessor hBasePutProcessor;

  @Before
  public void before() {
    configuration = mock(Configuration.class);
    hbaseConnection = mock(Connection.class);
    putTransformer = mock(PutTransformer.class);
    bufferedMutator = mock(BufferedMutator.class);
    fieldMappable = mock(FieldMappable.class);

    hBasePutProcessor = new HBasePutProcessor(configuration, putTransformer, hbaseConnection, bufferedMutator);
  }

  @Test
  public void testNoMutationIsDoneWhenNullListIsReceived() throws Exception {
    when(putTransformer.getMutationCommand(anyMap())).thenReturn(null);
    verifyNoMoreInteractions(bufferedMutator);

    hBasePutProcessor.accept(fieldMappable);
  }

  @Test
  public void testNoMutationIsDoneWhenListContainingNullsIsReceived() throws Exception {
    List<Mutation> inputList = Arrays.asList(null, null, null);
    when(putTransformer.getMutationCommand(anyMap())).thenReturn(inputList);
    verifyNoMoreInteractions(bufferedMutator);

    hBasePutProcessor.accept(fieldMappable);
  }

  @Test
  public void testNoMutationIsDoneWhenListContainingUnknownMutationIsReceived() throws Exception {
    List<Mutation> inputList = singletonList(mock(Mutation.class));
    when(putTransformer.getMutationCommand(anyMap())).thenReturn(inputList);
    verifyNoMoreInteractions(bufferedMutator);

    hBasePutProcessor.accept(fieldMappable);
  }

  @Test
  public void testWarningIsLoggedWhenListContainingEmptyPutIsReceived() throws Exception {
    Mutation emptyPutMutation = mock(Put.class);
    when(emptyPutMutation.getRow()).thenReturn("emptyPutMutation".getBytes());
    when(emptyPutMutation.isEmpty()).thenReturn(true);
    List<Mutation> inputList = singletonList(emptyPutMutation);
    when(putTransformer.getMutationCommand(anyMap())).thenReturn(inputList);
    verifyNoMoreInteractions(bufferedMutator);
    expectedLogMessage.expectWarn("Could not insert row with no columns for row-key column: emptyPutMutation");

    hBasePutProcessor.accept(fieldMappable);
  }

  @Test
  public void testWarningIsLoggedWhenListContainingEmptyDeleteIsReceived() throws Exception {
    Mutation emptyDeleteMutation = mock(Delete.class);
    when(emptyDeleteMutation.getRow()).thenReturn("emptyDeleteMutation".getBytes());
    when(emptyDeleteMutation.isEmpty()).thenReturn(true);
    List<Mutation> inputList = singletonList(emptyDeleteMutation);
    when(putTransformer.getMutationCommand(anyMap())).thenReturn(inputList);
    verifyNoMoreInteractions(bufferedMutator);
    expectedLogMessage.expectWarn("Could not delete row with no columns for row-key column: emptyDeleteMutation");

    hBasePutProcessor.accept(fieldMappable);
  }

  @Test
  public void testMutationIsDoneForAllElementsWhenListContainingValidMutationsIsReceived() throws Exception {
    Mutation aPutMutation = mock(Put.class);
    Mutation aDeleteMutation = mock(Delete.class);
    Mutation anotherPutMutation = mock(Put.class);
    List<Mutation> inputList = Arrays.asList(aPutMutation, aDeleteMutation, anotherPutMutation);
    when(putTransformer.getMutationCommand(anyMap())).thenReturn(inputList);

    hBasePutProcessor.accept(fieldMappable);

    verify(bufferedMutator).mutate(aPutMutation);
    verify(bufferedMutator).mutate(aDeleteMutation);
    verify(bufferedMutator).mutate(anotherPutMutation);
  }

}