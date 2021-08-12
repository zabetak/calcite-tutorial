/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.zabetak.calcite.tutorial;

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeName;

import com.github.zabetak.calcite.tutorial.indexer.DatasetIndexer;
import com.github.zabetak.calcite.tutorial.indexer.TpchTable;
import com.google.common.collect.ImmutableSet;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.Date;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

/**
 * Tests for {@link LuceneEnumerable}.
 */
public class LuceneEnumerableTest {

  @BeforeAll
  static void indexTpchDataset() throws IOException, URISyntaxException {
    // The dataset may already be there but doesn't hurt much to re-index it
    DatasetIndexer.main(new String[]{});
  }

  @ParameterizedTest(name = "table={0}, projection={1}, filter={2}")
  @MethodSource("queriesWithExpectedResults")
  void testSelectFilterQueryReturnsCorrectRow(String table, Set<String> fields, String query,
      Object[] expectedRow) {
    LuceneEnumerable enumerable = new LuceneEnumerable(
        "target/tpch/" + table, typedFields(table, fields), query);
    List<Object[]> expected = Collections.singletonList(expectedRow);
    assertContentEquals(expected, enumerable.toList());
  }

  private static void assertContentEquals(List<Object[]> expected, List<Object[]> actual) {
    Assertions.assertEquals(expected.size(), actual.size());
    for (int i = 0; i < expected.size(); i++) {
      Assertions.assertArrayEquals(expected.get(i), actual.get(i));
    }
  }

  private static LinkedHashMap<String, SqlTypeName> typedFields(String table, Set<String> fields) {
    LinkedHashMap<String, SqlTypeName> fieldToType = new LinkedHashMap<>();
    TpchTable t = TpchTable.valueOf(table.toUpperCase());
    JavaTypeFactory typeFactory = new JavaTypeFactoryImpl();
    for (TpchTable.Column c : t.columns) {
      if (fields.contains(c.name)) {
        fieldToType.put(c.name, typeFactory.createType(c.type).getSqlTypeName());
      }
    }
    return fieldToType;
  }

  private static Stream<Arguments> queriesWithExpectedResults() {
    return Stream.of(
        // Equality filters on different data types
        Arguments.of("CUSTOMER",
            ImmutableSet.of("c_custkey", "c_name"),
            "+c_custkey:[32 TO 32]",
            new Object[]{32, "Customer#000000032"}),
        Arguments.of("CUSTOMER",
            ImmutableSet.of("c_custkey", "c_name"),
            "+c_name:Customer#000000032",
            new Object[]{32, "Customer#000000032"}),
        Arguments.of("CUSTOMER",
            ImmutableSet.of("c_custkey", "c_name", "c_acctbal"),
            "+c_acctbal:[3471.53 TO 3471.53]",
            new Object[]{32, "Customer#000000032", 3471.53}),
        Arguments.of("ORDERS",
            ImmutableSet.of("o_orderkey", "o_orderdate"),
            "+o_orderdate:[8872 TO 8872]",
            new Object[]{96, Date.valueOf("1994-04-17")}));
  }
}
