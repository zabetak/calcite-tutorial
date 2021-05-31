<!--
{% comment %}
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to you under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
{% endcomment %}
-->
# Notes

A file containing notes, ideas, TODOs, about the tutorial.

The following list is not any particular order, just rough thoughts.

* Possibly instead of in-memory data use an end to end example over Apache Lucene
* Turn the focus of the tutorial(or small example) of Calcite as a data integration framework
* Addition of custom UDFs
* Usages of RexShuttle to find interesting patterns, rewrite/split conditions
* Generate SQL in different dialects
* Create custom rules / exercise(s) push condition to underlying engine 
* Use multiple conventions (e.g., JDBC + Lucene, or existing adapters) showing the data integration
capabilities of Calcite
* XxxToEnumerableConverter and code generation examples
* Custom metadata provider
* Usage of RelBuilder / exercise(s) to from SQL to RelNode
* Extract information from SqlNode tree (gather identifiers, tables, etc.)
