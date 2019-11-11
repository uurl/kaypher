package com.treutec.kaypher.ddl.commands;

import static com.treutec.kaypher.metastore.model.MetaStoreMatchers.KeyFieldMatchers.hasName;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

import com.treutec.kaypher.execution.ddl.commands.CreateStreamCommand;
import com.treutec.kaypher.execution.ddl.commands.CreateTableCommand;
import com.treutec.kaypher.execution.ddl.commands.DdlCommandResult;
import com.treutec.kaypher.execution.ddl.commands.DropSourceCommand;
import com.treutec.kaypher.execution.ddl.commands.DropTypeCommand;
import com.treutec.kaypher.execution.ddl.commands.KaypherTopic;
import com.treutec.kaypher.function.InternalFunctionRegistry;
import com.treutec.kaypher.metastore.MutableMetaStore;
import com.treutec.kaypher.metastore.model.DataSource.DataSourceType;
import com.treutec.kaypher.metastore.model.KaypherStream;
import com.treutec.kaypher.name.ColumnName;
import com.treutec.kaypher.name.SourceName;
import com.treutec.kaypher.schema.kaypher.LogicalSchema;
import com.treutec.kaypher.schema.kaypher.types.SqlPrimitiveType;
import com.treutec.kaypher.schema.kaypher.types.SqlTypes;
import com.treutec.kaypher.serde.Format;
import com.treutec.kaypher.serde.FormatInfo;
import com.treutec.kaypher.serde.KeyFormat;
import com.treutec.kaypher.serde.SerdeOption;
import com.treutec.kaypher.serde.ValueFormat;
import com.treutec.kaypher.util.MetaStoreFixture;
import com.treutec.kaypher.util.timestamp.TimestampExtractionPolicy;
import java.util.Optional;
import java.util.Set;
import org.hamcrest.MatcherAssert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

public class DdlCommandExecTest {
  private static final SourceName STREAM_NAME = SourceName.of("s1");
  private static final SourceName TABLE_NAME = SourceName.of("t1");
  private static final String TOPIC_NAME = "topic";
  private static final LogicalSchema SCHEMA = new LogicalSchema.Builder()
      .valueColumn(ColumnName.of("F1"), SqlPrimitiveType.of("INTEGER"))
      .valueColumn(ColumnName.of("F2"), SqlPrimitiveType.of("VARCHAR"))
      .build();
  private Set<SerdeOption> serdeOptions = SerdeOption.none();

  private CreateStreamCommand createStream;
  private CreateTableCommand createTable;
  private DropSourceCommand dropSource;
  private DropTypeCommand dropType;

  private MutableMetaStore metaStore
      = MetaStoreFixture.getNewMetaStore(new InternalFunctionRegistry());

  @Mock
  private TimestampExtractionPolicy timestampExtractionPolicy;
  @Mock
  private KaypherStream source;

  private DdlCommandExec cmdExec;

  @Rule
  public final MockitoRule mockitoRule = MockitoJUnit.rule();

  @Rule
  public final ExpectedException expectedException = ExpectedException.none();

  @Before
  public void setup() {
    //when(metaStore.getSource(STREAM_NAME)).thenReturn(source);
    when(source.getName()).thenReturn(STREAM_NAME);
    when(source.getDataSourceType()).thenReturn(DataSourceType.KSTREAM);
    when(source.getKafkaTopicName()).thenReturn(TOPIC_NAME);

    cmdExec = new DdlCommandExec(metaStore);
    dropType = new DropTypeCommand("type");
  }

  @Test
  public void shouldAddStreamWithKeyField() {
    // Given:
    givenCreateStreamWithKey(Optional.of("F1"));

    // When:
    cmdExec.execute(createStream);

    // Then:
    MatcherAssert.assertThat(metaStore.getSource(STREAM_NAME).getKeyField(), hasName("F1"));
  }

  @Test
  public void shouldAddStreamWithNoKeyField() {
    // Given:
    givenCreateStreamWithKey(Optional.empty());

    // When:
    cmdExec.execute(createStream);

    // Then:
    MatcherAssert.assertThat(metaStore.getSource(STREAM_NAME).getKeyField(), hasName(Optional.empty()));
  }

  @Test
  public void shouldAddTableWithKeyField() {
    // Given:
    givenCreateTableWithKey(Optional.of("F1"));

    // When:
    cmdExec.execute(createTable);

    // Then:
    MatcherAssert.assertThat(metaStore.getSource(TABLE_NAME).getKeyField(), hasName("F1"));
  }

  @Test
  public void shouldAddTableWithNoKeyField() {
    // Given:
    givenCreateTableWithKey(Optional.empty());

    // When:
    cmdExec.execute(createTable);

    // Then:
    MatcherAssert.assertThat(metaStore.getSource(TABLE_NAME).getKeyField(), hasName(Optional.empty()));
  }

  @Test
  public void shouldDropMissingSource() {
    // Given:
    givenDropSourceCommand(STREAM_NAME);

    // When:
    final DdlCommandResult result = cmdExec.execute(dropSource);

    // Then:
    assertThat(result.isSuccess(), is(true));
    assertThat(result.getMessage(), equalTo("Source " + STREAM_NAME + " does not exist."));
  }

  @Test
  public void shouldDropSource() {
    // Given:
    metaStore.putSource(source);
    givenDropSourceCommand(STREAM_NAME);

    // When:
    final DdlCommandResult result = cmdExec.execute(dropSource);

    // Then
    assertThat(result.isSuccess(), is(true));
    assertThat(
        result.getMessage(),
        equalTo(String.format("Source %s (topic: %s) was dropped.",  STREAM_NAME, TOPIC_NAME))
    );
  }

  @Test
  public void shouldDropExistingType() {
    // Given:
    metaStore.registerType("type", SqlTypes.STRING);

    // When:
    final DdlCommandResult result  = cmdExec.execute(dropType);

    // Then:
    assertThat(metaStore.resolveType("type").isPresent(), is(false));
    MatcherAssert.assertThat("Expected successful execution", result.isSuccess());
    MatcherAssert.assertThat(result.getMessage(), is("Dropped type 'type'"));
  }

  @Test
  public void shouldDropMissingType() {
    // Given:
    metaStore.deleteType("type");

    // When:
    final DdlCommandResult result = cmdExec.execute(dropType);

    // Then:
    MatcherAssert.assertThat("Expected successful execution", result.isSuccess());
    MatcherAssert.assertThat(result.getMessage(), is("Type 'type' does not exist"));
  }

  private void givenDropSourceCommand(final SourceName name) {
    dropSource = new DropSourceCommand(name);
  }

  private void givenCreateStreamWithKey(final Optional<String> keyField) {
    createStream = new CreateStreamCommand(
        "some sql",
        STREAM_NAME,
        SCHEMA,
        keyField.map(ColumnName::of),
        timestampExtractionPolicy,
        serdeOptions,
        new KaypherTopic(
            "topic",
            KeyFormat.nonWindowed(FormatInfo.of(Format.KAFKA)),
            ValueFormat.of(FormatInfo.of(Format.JSON)),
            false
        )
    );
  }

  private void givenCreateTableWithKey(final Optional<String> keyField) {
    createTable = new CreateTableCommand(
        "some sql",
        TABLE_NAME,
        SCHEMA,
        keyField.map(ColumnName::of),
        timestampExtractionPolicy,
        serdeOptions,
        new KaypherTopic(
            TOPIC_NAME,
            KeyFormat.nonWindowed(FormatInfo.of(Format.KAFKA)),
            ValueFormat.of(FormatInfo.of(Format.JSON)),
            false
        )
    );
  }
}