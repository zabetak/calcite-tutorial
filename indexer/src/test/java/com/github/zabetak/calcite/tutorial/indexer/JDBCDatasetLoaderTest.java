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
package com.github.zabetak.calcite.tutorial.indexer;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests for {@link JDBCDatasetLoader}.
 */
public class JDBCDatasetLoaderTest {

  private static final String JDBC_URL = "jdbc:hsqldb:mem:tpch";
  private static final String JDBC_USER = "SA";
  private static final String JDBC_PWD = "";

  @BeforeAll
  static void loadTpchDataset() throws SQLException, IOException {
    new JDBCDatasetLoader(JDBC_URL, JDBC_USER, JDBC_PWD).load();
  }

  @Test
  void testTpchDatasetRowCounts() throws SQLException {
    Map<TpchTable, Integer> expectedCounts = new HashMap<>();
    expectedCounts.put(TpchTable.CUSTOMER, 150);
    expectedCounts.put(TpchTable.LINEITEM, 6005);
    expectedCounts.put(TpchTable.NATION, 25);
    expectedCounts.put(TpchTable.ORDERS, 1500);
    expectedCounts.put(TpchTable.PARTSUPP, 800);
    expectedCounts.put(TpchTable.PART, 200);
    expectedCounts.put(TpchTable.REGION, 5);
    expectedCounts.put(TpchTable.SUPPLIER, 10);
    try (Connection conn = DriverManager.getConnection(JDBC_URL, JDBC_USER, JDBC_PWD)) {
      for (TpchTable table : TpchTable.values()) {
        try (Statement stmt = conn.createStatement()) {
          try (ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + table.name())) {
            int count = -1;
            while (rs.next()) {
              count = rs.getInt(1);
            }
            assertEquals(expectedCounts.get(table), count, table.name());
          }
        }
      }
    }
  }
}
