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

package com.treutec.kaypher.topic;

import static com.treutec.kaypher.topic.TopicPropertiesTest.Inject.KAYPHER_CONFIG;
import static com.treutec.kaypher.topic.TopicPropertiesTest.Inject.KAYPHER_CONFIG_P;
import static com.treutec.kaypher.topic.TopicPropertiesTest.Inject.KAYPHER_CONFIG_R;
import static com.treutec.kaypher.topic.TopicPropertiesTest.Inject.NO_CONFIG;
import static com.treutec.kaypher.topic.TopicPropertiesTest.Inject.NO_OVERRIDES;
import static com.treutec.kaypher.topic.TopicPropertiesTest.Inject.NO_WITH;
import static com.treutec.kaypher.topic.TopicPropertiesTest.Inject.OVERRIDES;
import static com.treutec.kaypher.topic.TopicPropertiesTest.Inject.OVERRIDES_P;
import static com.treutec.kaypher.topic.TopicPropertiesTest.Inject.OVERRIDES_R;
import static com.treutec.kaypher.topic.TopicPropertiesTest.Inject.SOURCE;
import static com.treutec.kaypher.topic.TopicPropertiesTest.Inject.WITH;
import static com.treutec.kaypher.topic.TopicPropertiesTest.Inject.WITH_P;
import static com.treutec.kaypher.topic.TopicPropertiesTest.Inject.WITH_R;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.treutec.kaypher.topic.TopicProperties.Builder;
import com.treutec.kaypher.util.KaypherConfig;
import com.treutec.kaypher.util.KaypherException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartitionInfo;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

@RunWith(Enclosed.class)
public class TopicPropertiesTest {

  public static class Tests {

    public @Rule ExpectedException expectedException = ExpectedException.none();

    private final KaypherConfig config = new KaypherConfig(ImmutableMap.of(
        KaypherConfig.SINK_NUMBER_OF_PARTITIONS_PROPERTY, 1,
        KaypherConfig.SINK_NUMBER_OF_REPLICAS_PROPERTY, (short) 1
    ));

    @Test
    public void shouldUseNameFromWithClause() {
      // When:
      final TopicProperties properties = new TopicProperties.Builder()
          .withWithClause(
              Optional.of("name"),
              Optional.empty(),
              Optional.empty()
          )
          .withKaypherConfig(config)
          .build();

      // Then:
      assertThat(properties.getTopicName(), equalTo("name"));
    }

    @Test
    public void shouldUseNameFromWithClauseWhenNameIsAlsoPresent() {
      // When:
      final TopicProperties properties = new TopicProperties.Builder()
          .withName("oh no!")
          .withWithClause(
              Optional.of("name"),
              Optional.empty(),
              Optional.empty()
          )
          .withKaypherConfig(config)
          .build();

      // Then:
      assertThat(properties.getTopicName(), equalTo("name"));
    }

    @Test
    public void shouldUseNameIfNoWIthClause() {
      // When:
      final TopicProperties properties = new TopicProperties.Builder()
          .withName("name")
          .withKaypherConfig(config)
          .build();

      // Then:
      assertThat(properties.getTopicName(), equalTo("name"));
    }

    @Test
    public void shouldFailIfNoNameSupplied() {
      // Expect:
      expectedException.expect(NullPointerException.class);
      expectedException.expectMessage("Was not supplied with any valid source for topic name!");

      // When:
      new TopicProperties.Builder()
          .withKaypherConfig(config)
          .build();
    }

    @Test
    public void shouldFailIfEmptyNameSupplied() {
      // Expect:
      expectedException.expect(KaypherException.class);
      expectedException.expectMessage("Must have non-empty topic name.");

      // When:
      new TopicProperties.Builder()
          .withName("")
          .withKaypherConfig(config)
          .build();
    }

    @Test
    public void shouldFailIfNoPartitionsSupplied() {
      // Given:
      final KaypherConfig config = new KaypherConfig(ImmutableMap.of(
          KaypherConfig.SINK_NUMBER_OF_REPLICAS_PROPERTY, (short) 1
      ));

      // Expect:
      expectedException.expect(KaypherException.class);
      expectedException.expectMessage("Cannot determine partitions for creating topic");

      // When:
      new TopicProperties.Builder()
          .withName("name")
          .withKaypherConfig(config)
          .build();
    }

    @Test
    public void shouldDefaultIfNoReplicasSupplied() {
      // Given:
      final KaypherConfig config = new KaypherConfig(ImmutableMap.of(
          KaypherConfig.SINK_NUMBER_OF_PARTITIONS_PROPERTY, 1
      ));

      // When:
      final TopicProperties properties = new Builder()
          .withName("name")
          .withKaypherConfig(config)
          .build();

      // Then:
      assertThat(properties.getReplicas(), is(TopicProperties.DEFAULT_REPLICAS));
    }

    @Test
    public void shouldNotMakeRemoteCallIfUnnecessary() {
      // When:
      final TopicProperties properties = new TopicProperties.Builder()
          .withWithClause(
              Optional.of("name"),
              Optional.of(1),
              Optional.of((short) 1)
          )
          .withKaypherConfig(config)
          .withSource(() -> {
            throw new RuntimeException();
          })
          .build();

      // Then:
      assertThat(properties.getPartitions(), equalTo(1));
      assertThat(properties.getReplicas(), equalTo((short) 1));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldNotMakeMultipleRemoteCalls() {
      // Given:
      final Supplier<TopicDescription> source = mock(Supplier.class);
      when(source.get())
          .thenReturn(
              new TopicDescription(
                  "",
                  false,
                  ImmutableList.of(
                      new TopicPartitionInfo(
                          0,
                          null,
                          ImmutableList.of(new Node(1, "", 1)),
                          ImmutableList.of()))))
          .thenThrow();

      // When:
      final TopicProperties properties = new TopicProperties.Builder()
          .withName("name")
          .withSource(source)
          .build();

      // Then:
      assertThat(properties.getPartitions(), equalTo(1));
      assertThat(properties.getReplicas(), equalTo((short) 1));
    }

    @Test
    public void shouldHandleStringOverrides() {
      // Given:
      final Map<String, Object> propertyOverrides = ImmutableMap.of(
          KaypherConfig.SINK_NUMBER_OF_PARTITIONS_PROPERTY, "1",
          KaypherConfig.SINK_NUMBER_OF_REPLICAS_PROPERTY, "2"
      );

      // When:
      final TopicProperties properties = new TopicProperties.Builder()
          .withName("name")
          .withOverrides(propertyOverrides)
          .build();

      // Then:
      assertThat(properties.getPartitions(), is(1));
      assertThat(properties.getReplicas(), is((short)2));
    }

    @Test
    public void shouldHandleNumberOverrides() {
      // Given:
      final Map<String, Object> propertyOverrides = ImmutableMap.of(
          KaypherConfig.SINK_NUMBER_OF_PARTITIONS_PROPERTY, 1,
          KaypherConfig.SINK_NUMBER_OF_REPLICAS_PROPERTY, 2
      );

      // When:
      final TopicProperties properties = new TopicProperties.Builder()
          .withName("name")
          .withOverrides(propertyOverrides)
          .build();

      // Then:
      assertThat(properties.getPartitions(), is(1));
      assertThat(properties.getReplicas(), is((short)2));
    }

    @Test
    public void shouldThrowOnInvalidPartitionsOverride() {
      // Given:
      final Map<String, Object> propertyOverrides = ImmutableMap.of(
          KaypherConfig.SINK_NUMBER_OF_PARTITIONS_PROPERTY, "I ain't no number",
          KaypherConfig.SINK_NUMBER_OF_REPLICAS_PROPERTY, "2"
      );

      // Then:
      expectedException.expect(KaypherException.class);
      expectedException.expectMessage(
          "Failed to parse property override 'kaypher.sink.partitions': "
              + "For input string: \"I ain't no number\"");

      // When:
      new TopicProperties.Builder()
          .withName("name")
          .withOverrides(propertyOverrides)
          .build();
    }

    @Test
    public void shouldThrowOnInvalidReplicasOverride() {
      // Given:
      final Map<String, Object> propertyOverrides = ImmutableMap.of(
          KaypherConfig.SINK_NUMBER_OF_PARTITIONS_PROPERTY, "1",
          KaypherConfig.SINK_NUMBER_OF_REPLICAS_PROPERTY, "I ain't no number"
      );

      // Then:
      expectedException.expect(KaypherException.class);
      expectedException.expectMessage(
          "Failed to parse property override 'kaypher.sink.replicas': "
              + "For input string: \"I ain't no number\"");

      // When:
      new TopicProperties.Builder()
          .withName("name")
          .withOverrides(propertyOverrides)
          .build();
    }
  }

  @RunWith(Parameterized.class)
  public static class PartitionsAndReplicasPrecedence {

    @Parameters(name = "given {0} -> expect({2} partitions, {3} replicas)")
    public static Iterable<Object[]> data() {
      final Object[][] data = new Object[][]{
          // THIS LIST WAS GENERATED BY RUNNING Inject#main
          //
          // Given:     Overrides                        Expect: [Partitions,     Replicas       ]
          {new Inject[]{WITH, OVERRIDES, KAYPHER_CONFIG          }, WITH           , WITH           },
          {new Inject[]{WITH, OVERRIDES, KAYPHER_CONFIG_P        }, WITH           , WITH           },
          {new Inject[]{WITH, OVERRIDES, KAYPHER_CONFIG_R        }, WITH           , WITH           },
          {new Inject[]{WITH, OVERRIDES, NO_CONFIG            }, WITH           , WITH           },
          {new Inject[]{WITH, OVERRIDES_P, KAYPHER_CONFIG        }, WITH           , WITH           },
          {new Inject[]{WITH, OVERRIDES_P, KAYPHER_CONFIG_P      }, WITH           , WITH           },
          {new Inject[]{WITH, OVERRIDES_P, KAYPHER_CONFIG_R      }, WITH           , WITH           },
          {new Inject[]{WITH, OVERRIDES_P, NO_CONFIG          }, WITH           , WITH           },
          {new Inject[]{WITH, OVERRIDES_R, KAYPHER_CONFIG        }, WITH           , WITH           },
          {new Inject[]{WITH, OVERRIDES_R, KAYPHER_CONFIG_P      }, WITH           , WITH           },
          {new Inject[]{WITH, OVERRIDES_R, KAYPHER_CONFIG_R      }, WITH           , WITH           },
          {new Inject[]{WITH, OVERRIDES_R, NO_CONFIG          }, WITH           , WITH           },
          {new Inject[]{WITH, NO_OVERRIDES, KAYPHER_CONFIG       }, WITH           , WITH           },
          {new Inject[]{WITH, NO_OVERRIDES, KAYPHER_CONFIG_P     }, WITH           , WITH           },
          {new Inject[]{WITH, NO_OVERRIDES, KAYPHER_CONFIG_R     }, WITH           , WITH           },
          {new Inject[]{WITH, NO_OVERRIDES, NO_CONFIG         }, WITH           , WITH           },
          {new Inject[]{WITH_P, OVERRIDES, KAYPHER_CONFIG        }, WITH_P         , OVERRIDES      },
          {new Inject[]{WITH_P, OVERRIDES, KAYPHER_CONFIG_P      }, WITH_P         , OVERRIDES      },
          {new Inject[]{WITH_P, OVERRIDES, KAYPHER_CONFIG_R      }, WITH_P         , OVERRIDES      },
          {new Inject[]{WITH_P, OVERRIDES, NO_CONFIG          }, WITH_P         , OVERRIDES      },
          {new Inject[]{WITH_P, OVERRIDES_P, KAYPHER_CONFIG      }, WITH_P         , KAYPHER_CONFIG    },
          {new Inject[]{WITH_P, OVERRIDES_P, KAYPHER_CONFIG_P    }, WITH_P         , SOURCE         },
          {new Inject[]{WITH_P, OVERRIDES_P, KAYPHER_CONFIG_R    }, WITH_P         , KAYPHER_CONFIG_R  },
          {new Inject[]{WITH_P, OVERRIDES_P, NO_CONFIG        }, WITH_P         , SOURCE         },
          {new Inject[]{WITH_P, OVERRIDES_R, KAYPHER_CONFIG      }, WITH_P         , OVERRIDES_R    },
          {new Inject[]{WITH_P, OVERRIDES_R, KAYPHER_CONFIG_P    }, WITH_P         , OVERRIDES_R    },
          {new Inject[]{WITH_P, OVERRIDES_R, KAYPHER_CONFIG_R    }, WITH_P         , OVERRIDES_R    },
          {new Inject[]{WITH_P, OVERRIDES_R, NO_CONFIG        }, WITH_P         , OVERRIDES_R    },
          {new Inject[]{WITH_P, NO_OVERRIDES, KAYPHER_CONFIG     }, WITH_P         , KAYPHER_CONFIG    },
          {new Inject[]{WITH_P, NO_OVERRIDES, KAYPHER_CONFIG_P   }, WITH_P         , SOURCE         },
          {new Inject[]{WITH_P, NO_OVERRIDES, KAYPHER_CONFIG_R   }, WITH_P         , KAYPHER_CONFIG_R  },
          {new Inject[]{WITH_P, NO_OVERRIDES, NO_CONFIG       }, WITH_P         , SOURCE         },
          {new Inject[]{WITH_R, OVERRIDES, KAYPHER_CONFIG        }, OVERRIDES      , WITH_R         },
          {new Inject[]{WITH_R, OVERRIDES, KAYPHER_CONFIG_P      }, OVERRIDES      , WITH_R         },
          {new Inject[]{WITH_R, OVERRIDES, KAYPHER_CONFIG_R      }, OVERRIDES      , WITH_R         },
          {new Inject[]{WITH_R, OVERRIDES, NO_CONFIG          }, OVERRIDES      , WITH_R         },
          {new Inject[]{WITH_R, OVERRIDES_P, KAYPHER_CONFIG      }, OVERRIDES_P    , WITH_R         },
          {new Inject[]{WITH_R, OVERRIDES_P, KAYPHER_CONFIG_P    }, OVERRIDES_P    , WITH_R         },
          {new Inject[]{WITH_R, OVERRIDES_P, KAYPHER_CONFIG_R    }, OVERRIDES_P    , WITH_R         },
          {new Inject[]{WITH_R, OVERRIDES_P, NO_CONFIG        }, OVERRIDES_P    , WITH_R         },
          {new Inject[]{WITH_R, OVERRIDES_R, KAYPHER_CONFIG      }, KAYPHER_CONFIG    , WITH_R         },
          {new Inject[]{WITH_R, OVERRIDES_R, KAYPHER_CONFIG_P    }, KAYPHER_CONFIG_P  , WITH_R         },
          {new Inject[]{WITH_R, OVERRIDES_R, KAYPHER_CONFIG_R    }, SOURCE         , WITH_R         },
          {new Inject[]{WITH_R, OVERRIDES_R, NO_CONFIG        }, SOURCE         , WITH_R         },
          {new Inject[]{WITH_R, NO_OVERRIDES, KAYPHER_CONFIG     }, KAYPHER_CONFIG    , WITH_R         },
          {new Inject[]{WITH_R, NO_OVERRIDES, KAYPHER_CONFIG_P   }, KAYPHER_CONFIG_P  , WITH_R         },
          {new Inject[]{WITH_R, NO_OVERRIDES, KAYPHER_CONFIG_R   }, SOURCE         , WITH_R         },
          {new Inject[]{WITH_R, NO_OVERRIDES, NO_CONFIG       }, SOURCE         , WITH_R         },
          {new Inject[]{NO_WITH, OVERRIDES, KAYPHER_CONFIG       }, OVERRIDES      , OVERRIDES      },
          {new Inject[]{NO_WITH, OVERRIDES, KAYPHER_CONFIG_P     }, OVERRIDES      , OVERRIDES      },
          {new Inject[]{NO_WITH, OVERRIDES, KAYPHER_CONFIG_R     }, OVERRIDES      , OVERRIDES      },
          {new Inject[]{NO_WITH, OVERRIDES, NO_CONFIG         }, OVERRIDES      , OVERRIDES      },
          {new Inject[]{NO_WITH, OVERRIDES_P, KAYPHER_CONFIG     }, OVERRIDES_P    , KAYPHER_CONFIG    },
          {new Inject[]{NO_WITH, OVERRIDES_P, KAYPHER_CONFIG_P   }, OVERRIDES_P    , SOURCE         },
          {new Inject[]{NO_WITH, OVERRIDES_P, KAYPHER_CONFIG_R   }, OVERRIDES_P    , KAYPHER_CONFIG_R  },
          {new Inject[]{NO_WITH, OVERRIDES_P, NO_CONFIG       }, OVERRIDES_P    , SOURCE         },
          {new Inject[]{NO_WITH, OVERRIDES_R, KAYPHER_CONFIG     }, KAYPHER_CONFIG    , OVERRIDES_R    },
          {new Inject[]{NO_WITH, OVERRIDES_R, KAYPHER_CONFIG_P   }, KAYPHER_CONFIG_P  , OVERRIDES_R    },
          {new Inject[]{NO_WITH, OVERRIDES_R, KAYPHER_CONFIG_R   }, SOURCE         , OVERRIDES_R    },
          {new Inject[]{NO_WITH, OVERRIDES_R, NO_CONFIG       }, SOURCE         , OVERRIDES_R    },
          {new Inject[]{NO_WITH, NO_OVERRIDES, KAYPHER_CONFIG    }, KAYPHER_CONFIG    , KAYPHER_CONFIG    },
          {new Inject[]{NO_WITH, NO_OVERRIDES, KAYPHER_CONFIG_P  }, KAYPHER_CONFIG_P  , SOURCE         },
          {new Inject[]{NO_WITH, NO_OVERRIDES, KAYPHER_CONFIG_R  }, SOURCE         , KAYPHER_CONFIG_R  },
          {new Inject[]{NO_WITH, NO_OVERRIDES, NO_CONFIG      }, SOURCE         , SOURCE         },
      };

      // generate the description from the given injections and put it at the beginning
      return Lists.newArrayList(data)
          .stream()
          .map(params -> Lists.asList(
              Arrays.stream((Inject[]) params[0])
                  .map(Objects::toString)
                  .collect(Collectors.joining(", ")),
              params))
          .map(List::toArray)
          .collect(Collectors.toList());
    }

    @Parameter
    public String description;

    @Parameter(1)
    public Inject[] injects;

    @Parameter(2)
    public Inject expectedPartitions;

    @Parameter(3)
    public Inject expectedReplicas;

    private KaypherConfig kaypherConfig = new KaypherConfig(new HashMap<>());
    private final Map<String, Object> propertyOverrides = new HashMap<>();
    private Optional<Integer> withClausePartitionCount = Optional.empty();
    private Optional<Short> withClauseReplicationFactor = Optional.empty();

    @Test
    public void shouldInferCorrectPartitionsAndReplicas() {
      // Given:
      Arrays.stream(injects).forEach(this::givenInject);

      // When:
      final TopicProperties properties = new TopicProperties.Builder()
          .withName("name")
          .withWithClause(
              Optional.empty(),
              withClausePartitionCount,
              withClauseReplicationFactor
          )
          .withOverrides(propertyOverrides)
          .withKaypherConfig(kaypherConfig)
          .withSource(() -> source(SOURCE))
          .build();

      // Then:
      assertThat(properties.getPartitions(), equalTo(expectedPartitions.partitions.orElse(null)));
      assertThat(properties.getReplicas(), equalTo(expectedReplicas.replicas.orElse(null)));
    }

    private void givenInject(final Inject inject) {
      switch (inject.type) {
        case WITH:
          withClausePartitionCount = inject.partitions;
          withClauseReplicationFactor = inject.replicas;
          break;
        case OVERRIDES:
          inject.partitions.ifPresent(partitions ->
              propertyOverrides.put(KaypherConfig.SINK_NUMBER_OF_PARTITIONS_PROPERTY, partitions)
          );

          inject.replicas.ifPresent(replicas ->
              propertyOverrides.put(KaypherConfig.SINK_NUMBER_OF_REPLICAS_PROPERTY, replicas)
          );
          break;
        case KAYPHER_CONFIG:
          final Map<String, Object> cfg = new HashMap<>();
          inject.partitions.ifPresent(partitions ->
              cfg.put(KaypherConfig.SINK_NUMBER_OF_PARTITIONS_PROPERTY, partitions)
          );

          inject.replicas.ifPresent(replicas ->
              cfg.put(KaypherConfig.SINK_NUMBER_OF_REPLICAS_PROPERTY, replicas)
          );
          kaypherConfig = new KaypherConfig(cfg);
          break;
        case SOURCE:
        default:
          throw new IllegalArgumentException(inject.type.toString());
      }
    }

    public static TopicDescription source(final Inject source) {
      return new TopicDescription(
          "source",
          false,
          Collections.nCopies(source.partitions.get(),
              new TopicPartitionInfo(
                  0,
                  null,
                  Collections.nCopies(source.replicas.get(), new Node(0, "", 0)),
                  ImmutableList.of())
          )
      );
    }
  }

  enum Inject {
    SOURCE(Type.SOURCE, 1, (short) 1),
    SOURCE2(Type.SOURCE, 12, (short) 12),

    WITH(Type.WITH, 2, (short) 2),
    OVERRIDES(Type.OVERRIDES, 3, (short) 3),
    KAYPHER_CONFIG(Type.KAYPHER_CONFIG, 4, (short) 4),

    WITH_P(Type.WITH, 5, null),
    OVERRIDES_P(Type.OVERRIDES, 6, null),
    KAYPHER_CONFIG_P(Type.KAYPHER_CONFIG, 7, null),

    WITH_R(Type.WITH, null, (short) 8),
    OVERRIDES_R(Type.OVERRIDES, null, (short) 9),
    KAYPHER_CONFIG_R(Type.KAYPHER_CONFIG, null, (short) 10),

    NO_WITH(Type.WITH, null, null),
    NO_OVERRIDES(Type.OVERRIDES, null, null),
    NO_CONFIG(Type.KAYPHER_CONFIG, null, null)
    ;

    final Type type;
    final Optional<Integer> partitions;
    final Optional<Short> replicas;

    Inject(final Type type, final Integer partitions, final Short replicas) {
      this.type = type;
      this.partitions = Optional.ofNullable(partitions);
      this.replicas = Optional.ofNullable(replicas);
    }

    enum Type {
      WITH,
      OVERRIDES,
      KAYPHER_CONFIG,
      SOURCE
    }

    /**
     * Generates code for all combinations of Injects
     */
    public static void main(String[] args) {
      final List<Inject> withs = EnumSet.allOf(Inject.class)
          .stream().filter(i -> i.type == Type.WITH).collect(Collectors.toList());
      final List<Inject> overrides = EnumSet.allOf(Inject.class)
          .stream().filter(i -> i.type == Type.OVERRIDES).collect(Collectors.toList());
      final List<Inject> kaypherConfigs = EnumSet.allOf(Inject.class)
          .stream().filter(i -> i.type == Type.KAYPHER_CONFIG).collect(Collectors.toList());

      for (List<Inject> injects : Lists.cartesianProduct(withs, overrides, kaypherConfigs)) {
        // sort by precedence order
        injects = new ArrayList<>(injects);
        injects.sort(Comparator.comparing(i -> i.type));

        final Inject expectedPartitions = injects.stream()
            .filter(i -> i.partitions.isPresent())
            .findFirst()
            .orElse(Inject.SOURCE);

        final Inject expectedReplicas = injects.stream()
            .filter(i -> i.replicas.isPresent())
            .findFirst()
            .orElse(Inject.SOURCE);

        System.out.println(String.format("{new Inject[]{%-38s}, %-15s, %-15s},",
            injects.stream().map(Objects::toString).collect(Collectors.joining(", ")),
            expectedPartitions,
            expectedReplicas)
        );
      }
    }
  }
}
