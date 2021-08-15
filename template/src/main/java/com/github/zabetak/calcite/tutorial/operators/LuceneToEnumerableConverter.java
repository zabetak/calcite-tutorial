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
package com.github.zabetak.calcite.tutorial.operators;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableRel;

/**
 * Relational expression that converts an lucene input to enumerable calling convention.
 *
 * The {@link EnumerableRel} operators are used to generate Java code. The code is compiled
 * by an embedded Java compiler (Janino) in order to process the input data and generate the
 * results. In order to "glue" together the {@link LuceneRel} operators with {@link EnumerableRel}
 * operators this converter needs to generate java code calling the Lucene APIs.
 *
 * @see LuceneRel#LUCENE
 * @see EnumerableConvention
 */
public final class LuceneToEnumerableConverter {
  // TODO 1. Extend ConverterImpl
  // TODO 2. Implement RelNode.copy
  // TODO 3. Implement EnumerableRel interface
  // TODO 4. Implement EnumerableRel#implement method
  //  The generated java code should resemble the snippet below:
  //  java.util.LinkedHashMap fields = new java.util.LinkedHashMap();
  //  fields.put("ps_partkey", org.apache.calcite.sql.type.SqlTypeName.INTEGER);
  //  fields.put("ps_suppkey", org.apache.calcite.sql.type.SqlTypeName.INTEGER);
  //  fields.put("ps_availqty", org.apache.calcite.sql.type.SqlTypeName.INTEGER);
  //  fields.put("ps_supplycost", org.apache.calcite.sql.type.SqlTypeName.DOUBLE);
  //  fields.put("ps_comment", org.apache.calcite.sql.type.SqlTypeName.VARCHAR);
  //  return new LuceneEnumerable("target/tpch/PARTSUPP", fields, "*:*");
}
