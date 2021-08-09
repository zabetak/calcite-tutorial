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
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableInterpretable;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.runtime.Bindable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlExplainFormat;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.sql.dialect.AnsiSqlDialect;
import org.apache.calcite.tools.RelBuilder;

import com.github.zabetak.calcite.tutorial.indexer.DatasetIndexer;
import com.github.zabetak.calcite.tutorial.indexer.TpchTable;

import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Properties;

/**
 * Query processor for running TPC-H queries over Apache Lucene using the {@link RelBuilder} API.
 */
public class LuceneBuilderProcessor {

  /**
   * Plans and executes a query constructed using the {@link RelBuilder}.
   */
  public static void main(String[] args) {
    CalciteSchema schema = CalciteSchema.createRootSchema(false);
    RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
    for (TpchTable tpchTable : TpchTable.values()) {
      RelDataTypeFactory.Builder typeBuilder = new RelDataTypeFactory.Builder(typeFactory);
      for (TpchTable.Column c : tpchTable.columns) {
        typeBuilder.add(c.name, typeFactory.createJavaType(c.type).getSqlTypeName());
      }
      String indexPath =
          Paths.get(DatasetIndexer.INDEX_LOCATION, "tpch", tpchTable.name()).toString();
      // TODO 1. Uncomment the following line to add table to the schema
      // schema.add(tpchTable.name(), new LuceneScannableTable(indexPath, typeBuilder.build()));
    }

    RelOptCluster cluster = newCluster(typeFactory);
    Properties props = new Properties();
    props.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), "false");
    CalciteConnectionConfig config = new CalciteConnectionConfigImpl(props);
    CalciteCatalogReader catalogReader = new CalciteCatalogReader(schema,
        Collections.singletonList(""),
        typeFactory, config);

    RelBuilder builder = new RelBuilder(Contexts.EMPTY_CONTEXT, cluster, catalogReader) {
    };
    // TODO 2. Use the RelBuilder to create directly a logical plan for execution
    RelNode logPlan = builder.build();
    // Display the logical plan
    System.out.println(
        RelOptUtil.dumpPlan("[Logical plan]", logPlan, SqlExplainFormat.TEXT,
            SqlExplainLevel.NON_COST_ATTRIBUTES));

    RelToSqlConverter relToSqlConverter = new RelToSqlConverter(AnsiSqlDialect.DEFAULT);
    System.out.println(relToSqlConverter.visitRoot(logPlan).asStatement().toString());

    // Initialize optimizer/planner with the necessary rules
    RelOptPlanner planner = cluster.getPlanner();
    planner.addRule(CoreRules.PROJECT_TO_CALC);
    planner.addRule(CoreRules.FILTER_TO_CALC);
    planner.addRule(EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE);
    planner.addRule(EnumerableRules.ENUMERABLE_CALC_RULE);
    planner.addRule(EnumerableRules.ENUMERABLE_JOIN_RULE);
    planner.addRule(EnumerableRules.ENUMERABLE_SORT_RULE);
    planner.addRule(EnumerableRules.ENUMERABLE_LIMIT_RULE);
    planner.addRule(EnumerableRules.ENUMERABLE_AGGREGATE_RULE);
    planner.addRule(EnumerableRules.ENUMERABLE_VALUES_RULE);
    planner.addRule(EnumerableRules.ENUMERABLE_UNION_RULE);
    planner.addRule(EnumerableRules.ENUMERABLE_MINUS_RULE);
    planner.addRule(EnumerableRules.ENUMERABLE_INTERSECT_RULE);
    planner.addRule(EnumerableRules.ENUMERABLE_MATCH_RULE);
    planner.addRule(EnumerableRules.ENUMERABLE_WINDOW_RULE);

    // Define the type of the output plan (in this case we want a physical plan in
    // EnumerableContention)
    logPlan = planner.changeTraits(logPlan,
        cluster.traitSet().replace(EnumerableConvention.INSTANCE));
    planner.setRoot(logPlan);
    // Start the optimization process to obtain the most efficient physical plan based on the
    // provided rule set.
    EnumerableRel phyPlan = (EnumerableRel) planner.findBestExp();

    // Display the physical plan
    System.out.println(
        RelOptUtil.dumpPlan("[Physical plan]", phyPlan, SqlExplainFormat.TEXT,
            SqlExplainLevel.NON_COST_ATTRIBUTES));

    // Obtain the executable plan
    Bindable<Object[]> executablePlan = EnumerableInterpretable.toBindable(
        new HashMap<>(),
        null,
        phyPlan,
        EnumerableRel.Prefer.ARRAY);
    long start = System.currentTimeMillis();
    for (Object[] row : executablePlan.bind(new SchemaOnlyDataContext(schema))) {
      System.out.println(Arrays.toString(row));
    }
    long finish = System.currentTimeMillis();
    System.out.println("Elapsed time " + (finish - start) + "ms");
  }

  private static RelOptCluster newCluster(RelDataTypeFactory factory) {
    RelOptPlanner planner = new VolcanoPlanner();
    planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
    return RelOptCluster.create(planner, new RexBuilder(factory));
  }

  /**
   * A simple data context only with schema information.
   */
  private static final class SchemaOnlyDataContext implements DataContext {
    private final SchemaPlus schema;

    SchemaOnlyDataContext(CalciteSchema calciteSchema) {
      this.schema = calciteSchema.plus();
    }

    @Override public SchemaPlus getRootSchema() {
      return schema;
    }

    @Override public JavaTypeFactory getTypeFactory() {
      return new JavaTypeFactoryImpl();
    }

    @Override public QueryProvider getQueryProvider() {
      return null;
    }

    @Override public Object get(final String name) {
      return null;
    }
  }
}
