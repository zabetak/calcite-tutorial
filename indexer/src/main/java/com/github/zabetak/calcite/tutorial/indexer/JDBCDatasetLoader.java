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

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Objects;

/**
 * A class for loading predefined datasets in a JDBC compliant database.
 *
 * The following datasets are available:
 * <ul>
 *   <li>TPC-H with scale factor 0.001</li>
 * </ul>
 *
 * Currently, only <a href="http://hsqldb.org/">HyperSQL</a> is tested.
 *
 * The loader reads data from CSV files and populates the database by first executing CREATE DDL
 * statements and then inserting the data via series of INSERT INTO statements.
 */
public class JDBCDatasetLoader {
  private final String url;
  private final String user;
  private final String pwd;

  public JDBCDatasetLoader(String url, String user, String pwd) {
    this.url = Objects.requireNonNull(url, "JDBC URL cannot be null");
    this.user = user;
    this.pwd = pwd;
  }

  public void load() throws SQLException, IOException {
    try (Connection conn = DriverManager.getConnection(url, user, pwd)) {
      for (TpchTable table : TpchTable.values()) {
        try (Statement stmt = conn.createStatement()) {
          stmt.execute(table.getCreateDDL());
        }
        TpchCSVReader.of(table).forEach(values -> {
          try (Statement stmt = conn.createStatement()) {
            stmt.executeUpdate(table.getInsertDML(values));
          } catch (SQLException e) {
            throw new RuntimeException("Updating database failed. Check cause for details", e);
          }
        });
      }
    }
  }

}
