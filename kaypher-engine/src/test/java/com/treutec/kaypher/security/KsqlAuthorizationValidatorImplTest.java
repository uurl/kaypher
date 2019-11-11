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
package com.treutec.kaypher.security;

import static org.mockito.Mockito.when;

import com.treutec.kaypher.engine.KaypherEngine;
import com.treutec.kaypher.engine.KaypherEngineTestUtil;
import com.treutec.kaypher.exception.KafkaResponseGetFailedException;
import com.treutec.kaypher.exception.KaypherTopicAuthorizationException;
import com.treutec.kaypher.execution.ddl.commands.KaypherTopic;
import com.treutec.kaypher.function.InternalFunctionRegistry;
import com.treutec.kaypher.metastore.MetaStoreImpl;
import com.treutec.kaypher.metastore.MutableMetaStore;
import com.treutec.kaypher.metastore.model.KeyField;
import com.treutec.kaypher.metastore.model.KaypherStream;
import com.treutec.kaypher.name.ColumnName;
import com.treutec.kaypher.name.SourceName;
import com.treutec.kaypher.parser.tree.Statement;
import com.treutec.kaypher.schema.kaypher.LogicalSchema;
import com.treutec.kaypher.schema.kaypher.types.SqlTypes;
import com.treutec.kaypher.serde.Format;
import com.treutec.kaypher.serde.FormatInfo;
import com.treutec.kaypher.serde.KeyFormat;
import com.treutec.kaypher.serde.SerdeOption;
import com.treutec.kaypher.serde.ValueFormat;
import com.treutec.kaypher.services.KafkaTopicClient;
import com.treutec.kaypher.services.ServiceContext;
import com.treutec.kaypher.util.timestamp.MetadataTimestampExtractionPolicy;
import java.util.Collections;
import java.util.Set;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.acl.AclOperation;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class KaypherAuthorizationValidatorImplTest {

  private static final LogicalSchema SCHEMA = LogicalSchema.builder()
      .valueColumn(ColumnName.of("F1"), SqlTypes.STRING)
      .build();

  private static final String STREAM_TOPIC_1 = "s1";
  private static final String STREAM_TOPIC_2 = "s2";
  private final static String TOPIC_NAME_1 = "topic1";
  private final static String TOPIC_NAME_2 = "topic2";

  @Mock
  private ServiceContext serviceContext;
  @Mock
  private KafkaTopicClient kafkaTopicClient;
  @Mock
  private TopicDescription TOPIC_1;
  @Mock
  private TopicDescription TOPIC_2;

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  private KaypherAuthorizationValidator authorizationValidator;
  private KaypherEngine kaypherEngine;
  private MutableMetaStore metaStore;

  @Before
  public void setUp() {
    metaStore = new MetaStoreImpl(new InternalFunctionRegistry());
    kaypherEngine = KaypherEngineTestUtil.createKaypherEngine(serviceContext, metaStore);

    authorizationValidator = new KaypherAuthorizationValidatorImpl();
    when(serviceContext.getTopicClient()).thenReturn(kafkaTopicClient);

    givenTopic(TOPIC_NAME_1, TOPIC_1);
    givenStreamWithTopic(STREAM_TOPIC_1, TOPIC_1);

    givenTopic(TOPIC_NAME_2, TOPIC_2);
    givenStreamWithTopic(STREAM_TOPIC_2, TOPIC_2);
  }

  @After
  public void closeEngine() {
    kaypherEngine.close();
  }

  private Statement givenStatement(final String sql) {
    return kaypherEngine.prepare(kaypherEngine.parse(sql).get(0)).getStatement();
  }

  @Test
  public void shouldAllowAnyOperationIfPermissionsAreNull() {
    // Given:
    givenTopicPermissions(TOPIC_1, null);
    final Statement statement = givenStatement("SELECT * FROM " + STREAM_TOPIC_1 + ";");

    // When:
    authorizationValidator.checkAuthorization(serviceContext, metaStore, statement);

    // Then:
    // Above command should not throw any exception
  }

  @Test
  public void shouldSingleSelectWithReadPermissionsAllowed() {
    // Given:
    givenTopicPermissions(TOPIC_1, Collections.singleton(AclOperation.READ));
    final Statement statement = givenStatement("SELECT * FROM " + STREAM_TOPIC_1 + ";");

    // When:
    authorizationValidator.checkAuthorization(serviceContext, metaStore, statement);

    // Then:
    // Above command should not throw any exception
  }

  @Test
  public void shouldThrowWhenSingleSelectWithoutReadPermissionsDenied() {
    // Given:
    givenTopicPermissions(TOPIC_1, Collections.emptySet());
    final Statement statement = givenStatement(String.format(
        "SELECT * FROM %s;", STREAM_TOPIC_1)
    );

    // Then:
    expectedException.expect(KaypherTopicAuthorizationException.class);
    expectedException.expectMessage(String.format(
        "Authorization denied to Read on topic(s): [%s]", TOPIC_1.name()
    ));

    // When:
    authorizationValidator.checkAuthorization(serviceContext, metaStore, statement);
  }

  @Test
  public void shouldJoinSelectWithReadPermissionsAllowed() {
    // Given:
    givenTopicPermissions(TOPIC_1, Collections.singleton(AclOperation.READ));
    givenTopicPermissions(TOPIC_2, Collections.singleton(AclOperation.READ));
    final Statement statement = givenStatement(String.format(
        "SELECT * FROM %s A JOIN %s B ON A.F1 = B.F1;", STREAM_TOPIC_1, STREAM_TOPIC_2)
    );

    // When:
    authorizationValidator.checkAuthorization(serviceContext, metaStore, statement);

    // Then:
    // Above command should not throw any exception
  }

  @Test
  public void shouldThrowWhenJoinSelectWithoutReadPermissionsDenied() {
    // Given:
    givenTopicPermissions(TOPIC_1, Collections.singleton(AclOperation.WRITE));
    givenTopicPermissions(TOPIC_2, Collections.singleton(AclOperation.WRITE));
    final Statement statement = givenStatement(String.format(
        "SELECT * FROM %s A JOIN %s B ON A.F1 = B.F1;", STREAM_TOPIC_1, STREAM_TOPIC_2)
    );

    // Then:
    expectedException.expect(KaypherTopicAuthorizationException.class);
    expectedException.expectMessage(String.format(
        "Authorization denied to Read on topic(s): [%s]", TOPIC_1.name()
    ));

    // When:
    authorizationValidator.checkAuthorization(serviceContext, metaStore, statement);
  }

  @Test
  public void shouldThrowWhenJoinWithOneRightTopicWithReadPermissionsDenied() {
    // Given:
    givenTopicPermissions(TOPIC_1, Collections.singleton(AclOperation.READ));
    givenTopicPermissions(TOPIC_2, Collections.singleton(AclOperation.WRITE));
    final Statement statement = givenStatement(String.format(
        "SELECT * FROM %s A JOIN %s B ON A.F1 = B.F1;", STREAM_TOPIC_1, STREAM_TOPIC_2)
    );

    // Then:
    expectedException.expect(KaypherTopicAuthorizationException.class);
    expectedException.expectMessage(String.format(
        "Authorization denied to Read on topic(s): [%s]", TOPIC_2.name()
    ));

    // When:
    authorizationValidator.checkAuthorization(serviceContext, metaStore, statement);
  }

  @Test
  public void shouldThrowWhenJoinWitOneLeftTopicWithReadPermissionsDenied() {
    // Given:
    givenTopicPermissions(TOPIC_1, Collections.singleton(AclOperation.WRITE));
    givenTopicPermissions(TOPIC_2, Collections.singleton(AclOperation.READ));
    final Statement statement = givenStatement(String.format(
        "SELECT * FROM %s A JOIN %s B ON A.F1 = B.F1;", STREAM_TOPIC_1, STREAM_TOPIC_2)
    );

    // Then:
    expectedException.expect(KaypherTopicAuthorizationException.class);
    expectedException.expectMessage(String.format(
        "Authorization denied to Read on topic(s): [%s]", TOPIC_1.name()
    ));

    // When:
    authorizationValidator.checkAuthorization(serviceContext, metaStore, statement);
  }

  @Test
  public void shouldInsertIntoWithAllPermissionsAllowed() {
    // Given:
    givenTopicPermissions(TOPIC_1, Collections.singleton(AclOperation.READ));
    givenTopicPermissions(TOPIC_2, Collections.singleton(AclOperation.WRITE));
    final Statement statement = givenStatement(String.format(
        "INSERT INTO %s SELECT * FROM %s;", STREAM_TOPIC_2, STREAM_TOPIC_1)
    );

    // When:
    authorizationValidator.checkAuthorization(serviceContext, metaStore, statement);

    // Then:
    // Above command should not throw any exception
  }

  @Test
  public void shouldThrowWhenInsertIntoWithOnlyReadPermissionsDenied() {
    // Given:
    givenTopicPermissions(TOPIC_1, Collections.singleton(AclOperation.READ));
    givenTopicPermissions(TOPIC_2, Collections.singleton(AclOperation.READ));
    final Statement statement = givenStatement(String.format(
        "INSERT INTO %s SELECT * FROM %s;", STREAM_TOPIC_2, STREAM_TOPIC_1)
    );

    // Then:
    expectedException.expect(KaypherTopicAuthorizationException.class);
    expectedException.expectMessage(String.format(
        "Authorization denied to Write on topic(s): [%s]", TOPIC_2.name()
    ));

    // When:
    authorizationValidator.checkAuthorization(serviceContext, metaStore, statement);
  }

  @Test
  public void shouldThrowWhenInsertIntoWithOnlyWritePermissionsDenied() {
    // Given:
    givenTopicPermissions(TOPIC_1, Collections.singleton(AclOperation.WRITE));
    givenTopicPermissions(TOPIC_2, Collections.singleton(AclOperation.WRITE));
    final Statement statement = givenStatement(String.format(
        "INSERT INTO %s SELECT * FROM %s;", STREAM_TOPIC_2, STREAM_TOPIC_1)
    );

    // Then:
    expectedException.expect(KaypherTopicAuthorizationException.class);
    expectedException.expectMessage(String.format(
        "Authorization denied to Read on topic(s): [%s]", TOPIC_1.name()
    ));

    // When:
    authorizationValidator.checkAuthorization(serviceContext, metaStore, statement);
  }

  @Test
  public void shouldThrowWhenCreateAsSelectWithoutReadPermissionsDenied() {
    // Given:
    givenTopicPermissions(TOPIC_1, Collections.emptySet());
    final Statement statement = givenStatement(String.format(
        "CREATE STREAM newStream AS SELECT * FROM %s;", STREAM_TOPIC_1)
    );

    // Then:
    expectedException.expect(KaypherTopicAuthorizationException.class);
    expectedException.expectMessage(String.format(
        "Authorization denied to Read on topic(s): [%s]", TOPIC_1.name()
    ));

    // When:
    authorizationValidator.checkAuthorization(serviceContext, metaStore, statement);
  }

  @Test
  public void shouldCreateAsSelectExistingTopicWithWritePermissionsAllowed() {
    // Given:
    givenTopicPermissions(TOPIC_1, Collections.singleton(AclOperation.READ));
    givenTopicPermissions(TOPIC_2, Collections.singleton(AclOperation.WRITE));
    final Statement statement = givenStatement(String.format(
        "CREATE STREAM %s AS SELECT * FROM %s;", STREAM_TOPIC_2, STREAM_TOPIC_1)
    );

    // When:
    authorizationValidator.checkAuthorization(serviceContext, metaStore, statement);

    // Then:
    // Above command should not throw any exception
  }

  @Test
  public void shouldThrowWhenCreateAsSelectExistingStreamWithoutWritePermissionsDenied() {
    // Given:
    givenTopicPermissions(TOPIC_1, Collections.singleton(AclOperation.READ));
    givenTopicPermissions(TOPIC_2, Collections.singleton(AclOperation.READ));
    final Statement statement = givenStatement(String.format(
        "CREATE STREAM %s AS SELECT * FROM %s;", STREAM_TOPIC_2, STREAM_TOPIC_1)
    );

    // Then:
    expectedException.expect(KaypherTopicAuthorizationException.class);
    expectedException.expectMessage(String.format(
        "Authorization denied to Write on topic(s): [%s]", TOPIC_2.name()
    ));


    // When:
    authorizationValidator.checkAuthorization(serviceContext, metaStore, statement);
  }

  @Test
  public void shouldCreateAsSelectWithTopicAndWritePermissionsAllowed() {
    // Given:
    givenTopicPermissions(TOPIC_1, Collections.singleton(AclOperation.READ));
    givenTopicPermissions(TOPIC_2, Collections.singleton(AclOperation.WRITE));
    final Statement statement = givenStatement(String.format(
        "CREATE STREAM newStream WITH (kafka_topic='%s') AS SELECT * FROM %s;",
        TOPIC_2.name(), STREAM_TOPIC_1)
    );

    // When:
    authorizationValidator.checkAuthorization(serviceContext, metaStore, statement);

    // Then:
    // Above command should not throw any exception
  }

  @Test
  public void shouldPrintTopicWithReadPermissionsAllowed() {
    // Given:
    givenTopicPermissions(TOPIC_1, Collections.singleton(AclOperation.READ));
    final Statement statement = givenStatement(String.format("Print '%s';", TOPIC_NAME_1));

    // When:
    authorizationValidator.checkAuthorization(serviceContext, metaStore, statement);

    // Then:
    // Above command should not throw any exception
  }

  @Test
  public void shouldThrowWhenThrowPrintTopicWithoutReadPermissionsDenied() {
    // Given:
    givenTopicPermissions(TOPIC_1, Collections.emptySet());
    final Statement statement = givenStatement(String.format("Print '%s';", TOPIC_NAME_1));

    // Then:
    expectedException.expect(KaypherTopicAuthorizationException.class);
    expectedException.expectMessage(String.format(
        "Authorization denied to Read on topic(s): [%s]", TOPIC_1.name()
    ));

    // When:
    authorizationValidator.checkAuthorization(serviceContext, metaStore, statement);
  }

  @Test
  public void shouldCreateSourceWithReadPermissionsAllowed() {
    // Given:
    givenTopicPermissions(TOPIC_1, Collections.singleton(AclOperation.READ));
    final Statement statement = givenStatement(String.format(
        "CREATE STREAM s1 WITH (kafka_topic='%s', value_format='JSON');", TOPIC_NAME_1)
    );

    // When:
    authorizationValidator.checkAuthorization(serviceContext, metaStore, statement);

    // Then:
    // Above command should not throw any exception
  }

  @Test
  public void shouldThrowWhenCreateSourceWithoutReadPermissionsDenied() {
    // Given:
    givenTopicPermissions(TOPIC_1, Collections.singleton(AclOperation.WRITE));
    final Statement statement = givenStatement(String.format(
        "CREATE STREAM s1 WITH (kafka_topic='%s', value_format='JSON');", TOPIC_NAME_1)
    );

    // Then:
    expectedException.expect(KaypherTopicAuthorizationException.class);
    expectedException.expectMessage(String.format(
        "Authorization denied to Read on topic(s): [%s]", TOPIC_1.name()
    ));

    // When:
    authorizationValidator.checkAuthorization(serviceContext, metaStore, statement);
  }

  @Test
  public void shouldThrowExceptionWhenTopicClientFails() {
    // Given:
    givenTopicPermissions(TOPIC_1, Collections.singleton(AclOperation.READ));
    final Statement statement = givenStatement("SELECT * FROM " + STREAM_TOPIC_1 + ";");
    givenTopicClientError(TOPIC_1);

    // Then:
    expectedException.expect(KafkaResponseGetFailedException.class);

    // When:
    authorizationValidator.checkAuthorization(serviceContext, metaStore, statement);
  }

  private void givenTopic(final String topicName, final TopicDescription topicDescription) {
    when(topicDescription.name()).thenReturn(topicName);
    when(kafkaTopicClient.describeTopic(topicDescription.name())).thenReturn(topicDescription);
  }

  private void givenTopicPermissions(
      final TopicDescription topicDescription,
      final Set<AclOperation> operations
  ) {
    when(topicDescription.authorizedOperations()).thenReturn(operations);
  }

  private void givenStreamWithTopic(
      final String streamName,
      final TopicDescription topicDescription
  ) {
    final KaypherTopic sourceTopic = new KaypherTopic(
        topicDescription.name(),
        KeyFormat.nonWindowed(FormatInfo.of(Format.KAFKA)),
        ValueFormat.of(FormatInfo.of(Format.JSON)),
        false
    );

    final KaypherStream<?> streamSource = new KaypherStream<>(
        "",
        SourceName.of(streamName.toUpperCase()),
        SCHEMA,
        SerdeOption.none(),
        KeyField.none(),
        new MetadataTimestampExtractionPolicy(),
        sourceTopic
    );

    metaStore.putSource(streamSource);
  }

  private void givenTopicClientError(final TopicDescription topic) {
    when(kafkaTopicClient.describeTopic(topic.name()))
        .thenThrow(KafkaResponseGetFailedException.class);
  }
}
