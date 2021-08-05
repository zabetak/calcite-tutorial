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

import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.sql.parser.SqlParseException;

import com.github.zabetak.calcite.tutorial.indexer.DatasetIndexer;
import com.github.zabetak.calcite.tutorial.indexer.TpchTable;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.sql.Date;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests for end to end tests over Apache Lucene.
 */
public class LuceneQueryProcessorTest {

  @BeforeAll
  static void indexTpchDataset() throws IOException, URISyntaxException {
    // The dataset may already be there but doesn't hurt much to re-index it
    DatasetIndexer.main(new String[]{});
  }

  @ParameterizedTest
  @ValueSource(strings = {"SIMPLE", "ADVANCED", "PUSHDOWN"})
  void testPredefinedTpchQueriesRun(String processor) throws Exception {
    Files.walkFileTree(Paths.get("..","queries", "tpch"), new SimpleFileVisitor<Path>() {
      @Override public FileVisitResult visitFile(final Path file, final BasicFileAttributes attrs) throws IOException {
        try {
          LuceneQueryProcessor.main(new String[]{processor, file.toString()});
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
        return super.visitFile(file, attrs);
      }
    });
  }

  @ParameterizedTest
  @EnumSource(LuceneQueryProcessor.Type.class)
  void testFullScanTpchTablesRuns(LuceneQueryProcessor.Type processor) throws SqlParseException {
    for (TpchTable t : TpchTable.values()) {
      LuceneQueryProcessor.execute("SELECT * FROM " + t.name(), processor);
    }
  }

  @ParameterizedTest
  @EnumSource(LuceneQueryProcessor.Type.class)
  void testCountTpchTables(LuceneQueryProcessor.Type processor) throws SqlParseException {
    Map<TpchTable, Long> expectedCounts = new HashMap<>();
    expectedCounts.put(TpchTable.CUSTOMER, 150L);
    expectedCounts.put(TpchTable.LINEITEM, 6005L);
    expectedCounts.put(TpchTable.NATION, 25L);
    expectedCounts.put(TpchTable.ORDERS, 1500L);
    expectedCounts.put(TpchTable.PARTSUPP, 800L);
    expectedCounts.put(TpchTable.PART, 200L);
    expectedCounts.put(TpchTable.REGION, 5L);
    expectedCounts.put(TpchTable.SUPPLIER, 10L);
    for (TpchTable t : TpchTable.values()) {
      Enumerable<Long> result = LuceneQueryProcessor.execute(
          "SELECT COUNT(1) FROM " + t.name(), processor);
      Long actualCnt = result.iterator().next();
      assertEquals(expectedCounts.get(t), actualCnt);
    }
  }

  @ParameterizedTest
  @EnumSource(LuceneQueryProcessor.Type.class)
  void testFilterOnPKAllColumnsValueTypesMatch(LuceneQueryProcessor.Type processor) throws SqlParseException {
    Map<String, Object[]> queryToRow = new HashMap<>();
    queryToRow.put("CUSTOMER WHERE c_custkey = 32", new Object[]{
        32,
        "Customer#000000032",
        "jD2xZzi UmId,DCtNBLXKj9q0Tlp2iQ6ZcO3J",
        15,
        "25-430-914-2194",
        3471.53,
        "BUILDING",
        "cial ideas. final, furious requests across the e"
    });
    queryToRow.put("LINEITEM WHERE l_orderkey = 6 AND l_partkey=140 AND l_suppkey=6", new Object[]{
        6,
        140,
        6,
        1,
        37,
        38485.18,
        0.08,
        0.03,
        "A",
        "F",
        Date.valueOf("1992-04-27"),
        Date.valueOf("1992-05-15"),
        Date.valueOf("1992-05-02"),
        "TAKE BACK RETURN",
        "TRUCK",
        "p furiously special foxes"
    });
    queryToRow.put("NATION WHERE n_nationkey = 6", new Object[]{
        6, "FRANCE", 3, "refully final requests. regular, ironi"
    });
    queryToRow.put("ORDERS WHERE o_orderkey = 96", new Object[]{
        96, 109, "F", 55090.67, Date.valueOf("1994-04-17"), "2-HIGH", "Clerk#000000395", 0, "oost furiously. pinto"
    });
    queryToRow.put("PART WHERE p_partkey = 10", new Object[]{
        10,
        "linen pink saddle puff powder",
        "Manufacturer#5",
        "Brand#54",
        "LARGE BURNISHED STEEL",
        44,
        "LG CAN",
        910.01,
        "ithely final deposit"
    });
    queryToRow.put("PARTSUPP WHERE ps_partkey = 5 AND ps_suppkey = 6", new Object[]{
        5, 6, 3735, 255.88, "arefully even requests. ironic requests cajole carefully even dolphin"
    });
    queryToRow.put("REGION WHERE r_regionkey = 3", new Object[]{
        3, "EUROPE", "ly final courts cajole furiously final excuse"
    });

    queryToRow.put("SUPPLIER WHERE s_suppkey = 5", new Object[]{
        5, "Supplier#000000005", "Gcdm2rJRzl5qlTVzc", 11, "21-151-690-3663", -283.84, ". slyly regular pinto bea"
    });
    for (Map.Entry<String, Object[]> q2r : queryToRow.entrySet()) {
      String query = "SELECT * FROM " + q2r.getKey();
      Object[] expectedRow = q2r.getValue();
      assertArrayEquals(expectedRow, (Object[]) LuceneQueryProcessor.execute(query, processor).single());
    }
  }
}
