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
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.adapter.enumerable.JavaRowFormat;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.adapter.enumerable.PhysTypeImpl;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.ConstantExpression;
import org.apache.calcite.linq4j.tree.DeclarationStatement;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.MethodCallExpression;
import org.apache.calcite.linq4j.tree.NewExpression;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterImpl;
import org.apache.calcite.rel.type.RelDataTypeField;

import com.github.zabetak.calcite.tutorial.LuceneEnumerable;

import java.lang.reflect.Method;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Relational expression that converts an lucene input to enumerable calling convention.
 *
 * The {@link EnumerableRel} operators are used to generate Java code. The code is compiled
 * by an embedded Java compiler (Janino) in order to process the input data and generate the
 * results. In order to "glue" together the {@link LuceneRel} operators with {@link EnumerableRel}
 * operators this converter needs to generate java code calling the Lucene APIs.
 *
 * @see LuceneRel#LUCENE
 * @see org.apache.calcite.adapter.enumerable.EnumerableConvention
 */
public final class LuceneToEnumerableConverter extends ConverterImpl implements EnumerableRel {
  public LuceneToEnumerableConverter(RelNode child) {
    super(child.getCluster(),
        ConventionTraitDef.INSTANCE,
        child.getCluster().traitSetOf(EnumerableConvention.INSTANCE),
        child);
  }

  @Override public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new LuceneToEnumerableConverter(inputs.get(0));
  }

  @Override public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
    //  The method generates java code which resembles the snippet below.
    //  java.util.LinkedHashMap fields = new java.util.LinkedHashMap();
    //  fields.put("ps_partkey", org.apache.calcite.sql.type.SqlTypeName.INTEGER);
    //  fields.put("ps_suppkey", org.apache.calcite.sql.type.SqlTypeName.INTEGER);
    //  fields.put("ps_availqty", org.apache.calcite.sql.type.SqlTypeName.INTEGER);
    //  fields.put("ps_supplycost", org.apache.calcite.sql.type.SqlTypeName.DOUBLE);
    //  fields.put("ps_comment", org.apache.calcite.sql.type.SqlTypeName.VARCHAR);
    //  return new LuceneEnumerable("target/tpch/PARTSUPP", fields, "*:*");
    try {
      BlockBuilder codeBlock = new BlockBuilder();
      DeclarationStatement fieldStmt =
          Expressions.declare(0, "fields", Expressions.new_(LinkedHashMap.class));
      codeBlock.add(fieldStmt);
      Method putMethod = Map.class.getMethod("put", Object.class, Object.class);
      for (RelDataTypeField f : getRowType().getFieldList()) {
        ConstantExpression fieldName = Expressions.constant(f.getName());
        ConstantExpression fieldType = Expressions.constant(f.getType().getSqlTypeName());
        MethodCallExpression callPut =
            Expressions.call(fieldStmt.parameter, putMethod, fieldName, fieldType);
        codeBlock.add(Expressions.statement(callPut));
      }
      ConstantExpression indexPath =
          Expressions.constant(((LuceneRel) input).implement().indexPath);
      ConstantExpression luceneQuery =
          Expressions.constant(((LuceneRel) input).implement().query.toString());
      NewExpression luceneEnumerable =
          Expressions.new_(LuceneEnumerable.class, indexPath, fieldStmt.parameter, luceneQuery);
      codeBlock.add(Expressions.return_(null, luceneEnumerable));
      PhysType physType = PhysTypeImpl.of(implementor.getTypeFactory(), getRowType(),
          pref.prefer(JavaRowFormat.ARRAY));
      return implementor.result(physType, codeBlock.toBlock());
    } catch (NoSuchMethodException e) {
      throw new RuntimeException(e);
    }
  }
}
