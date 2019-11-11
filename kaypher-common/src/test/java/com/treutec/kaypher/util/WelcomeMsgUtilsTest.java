/*
 * Copyright 2019 Treu Techologies
 *
 * See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.treutec.kaypher.util;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import java.io.PrintWriter;
import java.io.StringWriter;
import org.easymock.EasyMock;
import org.easymock.EasyMockRunner;
import org.easymock.Mock;
import org.easymock.MockType;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(EasyMockRunner.class)
public class WelcomeMsgUtilsTest {

  private StringWriter stringWriter;
  private PrintWriter realPrintWriter;
  @Mock(MockType.NICE)
  private PrintWriter mockPrintWriter;

  @Before
  public void setUp() {
    stringWriter = new StringWriter();
    realPrintWriter = new PrintWriter(stringWriter);
  }

  @Test
  public void shouldOutputLongWelcomeMessageIfConsoleIsWideEnough() {
    // When:
    WelcomeMsgUtils.displayWelcomeMessage(80, realPrintWriter);

    // Then:
    assertThat(stringWriter.toString(), is(
        "                  \n"
        + "                  ===========================================\n"
        + "                  =  Kaypher - Cypher for Kafka Graphs      =\n"
        + "                  ===========================================\n"
        + "\n"
        + "Copyright 2019 Treu Technologies\n"
        + "\n")
    );
  }

  @Test
  public void shouldOutputShortWelcomeMessageIfConsoleNotWideEnough() {
    // When:
    WelcomeMsgUtils.displayWelcomeMessage(35, realPrintWriter);

    // Then:
    assertThat(stringWriter.toString(), is("KAYPHER, Copyright 2019 Treu Technologies\n\n"));
  }

  @Test
  public void shouldFlushWriterWhenOutputtingLongMessage() {
    // Given:
    mockPrintWriter.flush();
    EasyMock.expectLastCall();
    EasyMock.replay(mockPrintWriter);

    // When:
    WelcomeMsgUtils.displayWelcomeMessage(80, mockPrintWriter);

    // Then:
    EasyMock.verify(mockPrintWriter);
  }

  @Test
  public void shouldFlushWriterWhenOutputtingShortMessage() {
    // Given:
    mockPrintWriter.flush();
    EasyMock.expectLastCall();
    EasyMock.replay(mockPrintWriter);

    // When:
    WelcomeMsgUtils.displayWelcomeMessage(10, mockPrintWriter);

    // Then:
    EasyMock.verify(mockPrintWriter);
  }
}
