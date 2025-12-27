/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.catalyst.optimizer

import scala.collection.mutable

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.{CREATE_NAMED_STRUCT, EXTRACT_VALUE, JSON_TO_STRUCT}
import org.apache.spark.sql.types.{ArrayType, StructType}

/**
 * Simplify redundant csv/json related expressions.
 *
 * The optimization includes:
 * 1. JsonToStructs(StructsToJson(child)) => child.
 * 2. Prune unnecessary columns from GetStructField/GetArrayStructFields + JsonToStructs.
 * 3. CreateNamedStruct(JsonToStructs(json).col1, JsonToStructs(json).col2, ...) =>
 *      If(IsNull(json), nullStruct, KnownNotNull(JsonToStructs(prunedSchema, ..., json)))
 *      if JsonToStructs(json) is shared among all fields of CreateNamedStruct. `prunedSchema`
 *      contains all accessed fields in original CreateNamedStruct.
 * 4. Prune unnecessary columns from GetStructField + CsvToStructs.
 */
object OptimizeCsvJsonExprs extends Rule[LogicalPlan] {
  private def nameOfCorruptRecord = conf.columnNameOfCorruptRecord

  override def apply(plan: LogicalPlan): LogicalPlan = plan.transformWithPruning(
    _.containsAnyPattern(CREATE_NAMED_STRUCT, EXTRACT_VALUE, JSON_TO_STRUCT), ruleId) {
    case p =>
      // First run the existing optimizations, but without the eager pruning.
      val optimized = if (conf.jsonExpressionOptimization) {
        p.transformExpressionsWithPruning(
          _.containsAnyPattern(CREATE_NAMED_STRUCT, EXTRACT_VALUE, JSON_TO_STRUCT)
        )(jsonOptimization)
      } else {
        p
      }

      // Then apply coalesced pruning to the entire plan node.
      val coalesced = if (conf.jsonExpressionOptimization) {
        coalesceJsonPruning(optimized)
      } else {
        optimized
      }

      if (conf.csvExpressionOptimization) {
        coalesced.transformExpressionsWithPruning(
          _.containsAnyPattern(EXTRACT_VALUE))(csvOptimization)
      } else {
        coalesced
      }
  }

  /**
   * This method identifies multiple `GetStructField` or `GetArrayStructFields` on the same
   * `JsonToStructs` expression and prunes the schema to the union of all accessed fields.
   * This allows Common Subexpression Elimination (CSE) to merge the `JsonToStructs` expressions,
   * significantly reducing parsing overhead when multiple fields are accessed.
   */
  private def coalesceJsonPruning(plan: LogicalPlan): LogicalPlan = {
    // 1. Collect all interesting expressions (GetStructField/GetArrayStructFields on JsonToStructs)
    val usages = mutable.ArrayBuffer[(Expression, JsonToStructs, Int)]()

    // We only traverse the top-level expressions of the plan node.
    // Deeply nested expressions will be handled when the rule visits their parent nodes,
    // assuming they are part of a Plan node.
    // However, expressions can be nested within expressions (e.g. `UPPER(from_json(...).a)`).
    // So we need to traverse the expression trees.
    plan.expressions.foreach { expr =>
      expr.foreach {
        case g @ GetStructField(j: JsonToStructs, ordinal, _) =>
          // j.schema needs to be cast to StructType to access length
          val schema = j.schema.asInstanceOf[StructType]
          if (j.options.isEmpty && schema.length > 1) {
            usages += ((g, j, ordinal))
          }
        case g @ GetArrayStructFields(j: JsonToStructs, _, ordinal, _, _) =>
          // j.schema is ArrayType(StructType)
          val schema = j.schema.asInstanceOf[ArrayType].elementType.asInstanceOf[StructType]
          if (j.options.isEmpty && schema.length > 1) {
            usages += ((g, j, ordinal))
          }
        case _ =>
      }
    }

    if (usages.isEmpty) return plan

    // 2. Group by the `JsonToStructs` expression itself.
    // Identical `JsonToStructs` (same child, same options, same schema) will be grouped together.
    val grouped = usages.groupBy(_._2)

    val replacements = mutable.Map.empty[Expression, Expression]

    grouped.foreach { case (jsonExpr, usageList) =>
      val usedOrdinals = usageList.map(_._3).distinct.sorted
      val originalSchema = jsonExpr.schema match {
        case s: StructType => s
        case ArrayType(s: StructType, _) => s
      }

      // 3. If we use fewer fields than the original schema, prune it.
      if (usedOrdinals.length < originalSchema.length) {
        val prunedFields = usedOrdinals.map(originalSchema(_))
        val prunedStructSchema = StructType(prunedFields.toSeq)

        val newSchema = jsonExpr.schema match {
          case _: StructType => prunedStructSchema
          case ArrayType(_, containsNull) => ArrayType(prunedStructSchema, containsNull)
        }

        val newJsonExpr = jsonExpr.copy(schema = newSchema)
        val ordinalMap = usedOrdinals.zipWithIndex.toMap

        usageList.foreach { case (expr, _, oldOrdinal) =>
          val newOrdinal = ordinalMap(oldOrdinal)
          val newExpr = expr match {
            case g: GetStructField => g.copy(child = newJsonExpr, ordinal = newOrdinal)
            case g: GetArrayStructFields =>
              g.copy(child = newJsonExpr, ordinal = newOrdinal,
                numFields = prunedStructSchema.length)
          }
          replacements += (expr -> newExpr)
        }
      }
    }

    if (replacements.isEmpty) {
      plan
    } else {
      plan.transformExpressions { case e => replacements.getOrElse(e, e) }
    }
  }

  private val jsonOptimization: PartialFunction[Expression, Expression] = {
    case c: CreateNamedStruct
        // If we create struct from various fields of the same `JsonToStructs`.
        if c.valExprs.forall { v =>
          v.isInstanceOf[GetStructField] &&
            v.asInstanceOf[GetStructField].child.isInstanceOf[JsonToStructs] &&
            v.children.head.semanticEquals(c.valExprs.head.children.head)
        } =>
      val jsonToStructs = c.valExprs.map(_.children.head)
      val sameFieldName = c.names.zip(c.valExprs).forall {
        case (name, valExpr: GetStructField) =>
          name.toString == valExpr.childSchema(valExpr.ordinal).name
        case _ => false
      }

      // Although `CreateNamedStruct` allows duplicated field names, e.g. "a int, a int",
      // `JsonToStructs` does not support parsing json with duplicated field names.
      val duplicateFields = c.names.map(_.toString).distinct.length != c.names.length

      // If we create struct from various fields of the same `JsonToStructs` and we don't
      // alias field names and there is no duplicated field in the struct.
      if (sameFieldName && !duplicateFields) {
        val fromJson = jsonToStructs.head.asInstanceOf[JsonToStructs].copy(schema = c.dataType)
        val nullFields = c.children.grouped(2).flatMap {
          case Seq(name, value) => Seq(name, Literal(null, value.dataType))
        }.toSeq

        If(IsNull(fromJson.child), c.copy(children = nullFields), KnownNotNull(fromJson))
      } else {
        c
      }

    case jsonToStructs @ JsonToStructs(_, options1,
      StructsToJson(options2, child, timeZoneId2), timeZoneId1)
        if options1.isEmpty && options2.isEmpty && timeZoneId1 == timeZoneId2 &&
          jsonToStructs.dataType == child.dataType =>
      // `StructsToJson` only fails when `JacksonGenerator` encounters data types it
      // cannot convert to JSON. But `StructsToJson.checkInputDataTypes` already
      // verifies its child's data types is convertible to JSON. But in
      // `StructsToJson(JsonToStructs(...))` case, we cannot verify input json string
      // so `JsonToStructs` might throw error in runtime. Thus we cannot optimize
      // this case similarly.
      child

    // Eager pruning cases removed to allow coalescing in coalesceJsonPruning
  }

  private val csvOptimization: PartialFunction[Expression, Expression] = {
    case g @ GetStructField(c @ CsvToStructs(schema: StructType, _, _, _, None), ordinal, _)
        if schema.length > 1 && c.options.isEmpty && schema(ordinal).name != nameOfCorruptRecord =>
        // When the parse mode is permissive, and corrupt column is not selected, we can prune here
        // from `GetStructField`. To be more conservative, it does not optimize when any option
        // is set.
      val prunedSchema = StructType(Array(schema(ordinal)))
      g.copy(child = c.copy(requiredSchema = Some(prunedSchema)), ordinal = 0)
  }
}
