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

import java.sql.Date;
import java.util.Arrays;
import java.util.List;

/**
 * A table from the TPC-H benchmark.
 */
public enum TpchTable {
  /**
   * <pre>{@code
   * CREATE TABLE CUSTOMER (
   *   C_CUSTKEY    INT NOT NULL,
   *   C_NAME       VARCHAR(25),
   *   C_ADDRESS    VARCHAR(40),
   *   C_NATIONKEY  INT NOT NULL,
   *   C_PHONE      CHAR(15),
   *   C_ACCTBAL    DECIMAL,
   *   C_MKTSEGMENT CHAR(10),
   *   C_COMMENT    VARCHAR(117))
   * }</pre>
   */
  CUSTOMER(Arrays.asList(
      new Column("c_custkey", Integer.class),
      new Column("c_name", String.class),
      new Column("c_address", String.class),
      new Column("c_nationkey", Integer.class),
      new Column("c_phone", String.class),
      new Column("c_acctbal", Double.class),
      new Column("c_mktsegment", String.class),
      new Column("c_comment", String.class))),
  /**
   * <pre>{@code
   * CREATE TABLE LINEITEM (
   * 	L_ORDERKEY		INT NOT NULL,
   * 	L_PARTKEY		INT NOT NULL,
   * 	L_SUPPKEY		INT NOT NULL,
   * 	L_LINENUMBER	INT,
   * 	L_QUANTITY		DECIMAL,
   * 	L_EXTENDEDPRICE	DECIMAL,
   * 	L_DISCOUNT		DECIMAL,
   * 	L_TAX			DECIMAL,
   * 	L_RETURNFLAG	CHAR(1),
   * 	L_LINESTATUS	CHAR(1),
   * 	L_SHIPDATE		DATE,
   * 	L_COMMITDATE	DATE,
   * 	L_RECEIPTDATE	DATE,
   * 	L_SHIPINSTRUCT	CHAR(25),
   * 	L_SHIPMODE		CHAR(10),
   * 	L_COMMENT		VARCHAR(44))
   * }</pre>
   */
  LINEITEM(Arrays.asList(
      new Column("l_orderkey", Integer.class),
      new Column("l_partkey", Integer.class),
      new Column("l_suppkey", Integer.class),
      new Column("l_linenumber", Integer.class),
      new Column("l_quantity", Integer.class),
      new Column("l_extendedprice", Double.class),
      new Column("l_discount", Double.class),
      new Column("l_tax", Double.class),
      new Column("l_returnflag", String.class),
      new Column("l_linestatus", String.class),
      new Column("l_shipdate", Date.class),
      new Column("l_commitdate", Date.class),
      new Column("l_receiptdate", Date.class),
      new Column("l_shipinstruct", String.class),
      new Column("l_shipmode", String.class),
      new Column("l_comment", String.class))),
  /**
   * <pre>{@code
   * CREATE TABLE ORDERS (
   * 	O_ORDERKEY      INT NOT NULL,
   * 	O_CUSTKEY       INT NOT NULL,
   * 	O_ORDERSTATUS   CHAR(1),
   * 	O_TOTALPRICE    DECIMAL,
   * 	O_ORDERDATE     DATE,
   * 	O_ORDERPRIORITY CHAR(15),
   * 	O_CLERK         CHAR(15),
   * 	O_SHIPPRIORITY  INT,
   * 	O_COMMENT       VARCHAR(79))
   * 	}</pre>
   */
  ORDERS(Arrays.asList(
      new Column("o_orderkey", Integer.class),
      new Column("o_custkey", Integer.class),
      new Column("o_orderstatus", String.class),
      new Column("o_totalprice", Double.class),
      new Column("o_orderdate", Date.class),
      new Column("o_orderpriority", String.class),
      new Column("o_clerk", String.class),
      new Column("o_shippriority", Integer.class),
      new Column("o_comment", String.class))),
  /**
   * <pre>{@code
   * CREATE TABLE NATION (
   *   N_NATIONKEY INT NOT NULL,
   *   N_NAME      CHAR(25),
   *   N_REGIONKEY INT NOT NULL,
   *   N_COMMENT   VARCHAR(152))
   * }</pre>
   */
  NATION(Arrays.asList(
      new Column("n_nationkey", Integer.class),
      new Column("n_name", String.class),
      new Column("n_regionkey", Integer.class),
      new Column("n_comment", String.class))),
  /**
   * <pre>{@code
   * CREATE TABLE part(
   *   p_partkey INT NOT NULL,
   *   p_name STRING,
   *   p_mfgr STRING,
   *   p_brand STRING,
   *   p_type STRING,
   *   p_size INT,
   *   p_container STRING,
   *   p_retailprice DOUBLE,
   *   p_comment STRING)
   * }</pre>
   */
  PART(Arrays.asList(
      new Column("p_partkey", Integer.class),
      new Column("p_name", String.class),
      new Column("p_mfgr", String.class),
      new Column("p_brand", String.class),
      new Column("p_type", String.class),
      new Column("p_size", Integer.class),
      new Column("p_container", String.class),
      new Column("p_retailprice", Double.class),
      new Column("p_comment", String.class))),
  /**
   * <pre>{@code
   *   CREATE TABLE PARTSUPP(
   *   PS_PARTKEY    INT NOT NULL,
   *   PS_SUPPKEY    INT NOT NULL,
   *   PS_AVAILQTY   INT,
   *   PS_SUPPLYCOST DECIMAL,
   *   PS_COMMENT    VARCHAR(199))
   * }</pre>
   */
  PARTSUPP(Arrays.asList(
      new Column("ps_partkey", Integer.class),
      new Column("ps_suppkey", Integer.class),
      new Column("ps_availqty", Integer.class),
      new Column("ps_supplycost", Double.class),
      new Column("ps_comment", String.class))),
  /**
   * <pre>{@code
   * CREATE TABLE REGION (
   *   R_REGIONKEY INT NOT NULL,
   *   R_NAME      CHAR(25),
   *   R_COMMENT   VARCHAR(152))
   * }</pre>
   */
  REGION(Arrays.asList(
      new Column("r_regionkey", Integer.class),
      new Column("r_name", String.class),
      new Column("r_comment", String.class))),
  /**
   * <pre>{@code
   * CREATE TABLE SUPPLIER (
   * 	S_SUPPKEY   INT NOT NULL,
   * 	S_NAME      CHAR(25),
   * 	S_ADDRESS   VARCHAR(40),
   * 	S_NATIONKEY INT NOT NULL,
   * 	S_PHONE     CHAR(15),
   * 	S_ACCTBAL   DECIMAL,
   * 	S_COMMENT   VARCHAR(101))
   * }</pre>
   */
  SUPPLIER(Arrays.asList(
      new Column("s_suppkey", Integer.class),
      new Column("s_name", String.class),
      new Column("s_address", String.class),
      new Column("s_nationkey", Integer.class),
      new Column("s_phone", String.class),
      new Column("s_acctbal", Double.class),
      new Column("s_comment", String.class)));

  public final List<Column> columns;

  TpchTable(final List<Column> columns) {
    this.columns = columns;
  }

  /**
   * A table column defined by a name and type.
   */
  public static final class Column {
    public final String name;
    public final Class<?> type;

    private Column(final String name, final Class<?> type) {
      this.name = name;
      this.type = type;
    }
  }
}
