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

package org.apache.sqoop.util;

import org.apache.log4j.Appender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;
import org.apache.log4j.spi.ThrowableInformation;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.mockito.ArgumentCaptor;

import static org.apache.commons.lang.StringUtils.EMPTY;
import static org.apache.commons.lang.StringUtils.contains;
import static org.apache.commons.lang.StringUtils.defaultString;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.atMost;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class ExpectedLogMessage implements TestRule {

  private static class LoggingEventMatcher extends TypeSafeMatcher<LoggingEvent> {

    private final String msg;

    private final Level level;

    private LoggingEventMatcher(String msg, Level level) {
      this.msg = msg;
      this.level = level;
    }

    @Override
    public boolean matchesSafely(LoggingEvent o) {
      return contains(extractEventMessage(o), msg) && level.equals(o.getLevel());
    }

    @Override
    public void describeTo(org.hamcrest.Description description) {
      description.appendText(eventToString(msg, level));
    }

    @Override
    protected void describeMismatchSafely(LoggingEvent item, org.hamcrest.Description mismatchDescription) {
      mismatchDescription.appendText(eventToString(extractEventMessage(item), item.getLevel()));
    }

    private String extractEventMessage(LoggingEvent item) {
      final String eventMsg = item.getRenderedMessage();
      final String exceptionMessage = extractExceptionMessage(item.getThrowableInformation());

      return eventMsg + exceptionMessage;
    }

    private String extractExceptionMessage(ThrowableInformation throwableInfo) {
      if (throwableInfo == null) {
        return EMPTY;
      }

      Throwable throwable = throwableInfo.getThrowable();
      if (throwable == null) {
        return EMPTY;
      }

      return defaultString(throwable.getMessage());
    }

    private String eventToString(String msg, Level level) {
      return "Log entry [ " + msg + ", " + level + " ]";
    }

  }

  private Matcher<LoggingEvent> loggingEventMatcher;

  @Override
  public Statement apply(final Statement base, Description description) {
    return new Statement() {
      @Override
      public void evaluate() throws Throwable {

        Logger rootLogger = Logger.getRootLogger();
        Appender mockAppender = mock(Appender.class);
        rootLogger.addAppender(mockAppender);

        try {
          base.evaluate();
          if (loggingEventMatcher != null) {
            ArgumentCaptor<LoggingEvent> argumentCaptor = ArgumentCaptor.forClass(LoggingEvent.class);
            verify(mockAppender, atMost(Integer.MAX_VALUE)).doAppend(argumentCaptor.capture());
            assertThat(argumentCaptor.getAllValues(), hasItem(loggingEventMatcher));
          }
        } finally {
          rootLogger.removeAppender(mockAppender);
          loggingEventMatcher = null;
        }
      }
    };
  }

  public void expectFatal(String msg) {
    loggingEventMatcher = new LoggingEventMatcher(msg, Level.FATAL);
  }

  public void expectError(String msg) {
    loggingEventMatcher = new LoggingEventMatcher(msg, Level.ERROR);
  }

  public void expectWarn(String msg) {
    loggingEventMatcher = new LoggingEventMatcher(msg, Level.WARN);
  }

  public void expectInfo(String msg) {
    loggingEventMatcher = new LoggingEventMatcher(msg, Level.INFO);
  }

  public void expectDebug(String msg) {
    loggingEventMatcher = new LoggingEventMatcher(msg, Level.DEBUG);
  }

  public void expectTrace(String msg) {
    loggingEventMatcher = new LoggingEventMatcher(msg, Level.TRACE);
  }

}
