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

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.LinkedHashMap;

/**
 * Table representing an Apache Lucene index.
 *
 * The table implements the {@link ScannableTable} interface and knows how to extract rows
 * from Lucene and map them to Calcite's internal representation. Implementing this interface
 * makes it easy to run queries over Lucene without introducing custom rules and operators.
 *
 * The {@link LuceneQueryProcessor.Type#SIMPLE} processor relies on the {@link ScannableTable}
 * interface in order to work. The {@link LuceneQueryProcessor.Type#ADVANCED} and
 * {@link LuceneQueryProcessor.Type#PUSHDOWN} variants do not need this interface so the respective
 * methods can be removed.
 */
public final class LuceneTable extends AbstractTable implements ScannableTable {
  private final String indexPath;
  private final RelDataType dataType;

  public LuceneTable(String indexPath, RelDataType dataType) {
    this.indexPath = indexPath;
    this.dataType = dataType;
  }

  @Override public Enumerable<Object[]> scan(final DataContext root) {
    LinkedHashMap<String, SqlTypeName> fields = new LinkedHashMap<>();
    for (RelDataTypeField f : dataType.getFieldList()) {
      fields.put(f.getName(), f.getType().getSqlTypeName());
    }
    return new LuceneEnumerable(indexPath, fields, "*:*");
  }

  @Override public RelDataType getRowType(final RelDataTypeFactory typeFactory) {
    return typeFactory.copyType(dataType);
  }

  /**
   * Returns the path to the index in the filesystem.
   */
  public String indexPath() {
    return indexPath;
  }
}
