/**
 * Copyright (c) 2013-present Snowplow Analytics Ltd. All rights reserved.
 *
 * This software is made available by Snowplow Analytics, Ltd., under the terms of the Snowplow
 * Limited Use License Agreement, Version 1.1 located at
 * https://docs.snowplow.io/limited-use-license-1.1 BY INSTALLING, DOWNLOADING, ACCESSING, USING OR
 * DISTRIBUTING ANY PORTION OF THE SOFTWARE, YOU AGREE TO THE TERMS OF SUCH LICENSE AGREEMENT.
 */
package com.snowplowanalytics.snowplow.bigquery.processing

import cats.Eq
import cats.implicits._

import com.snowplowanalytics.iglu.schemaddl.parquet.{Field, Type}
import com.google.protobuf.Descriptors
import com.google.cloud.bigquery.{Field => BQField, FieldList, StandardSQLTypeName}

import scala.jdk.CollectionConverters._

object BigQuerySchemaUtils {

  def fieldsMissingFromDescriptor(tableDescriptor: Descriptors.Descriptor, bqFields: FieldList): Boolean =
    bqFields.asScala.exists { field =>
      Option(tableDescriptor.findFieldByName(field.getName)) match {
        case Some(fieldDescriptor) =>
          val nullableMismatch = fieldDescriptor.isRequired && (field.getMode === BQField.Mode.NULLABLE)
          nullableMismatch || nestedFieldMissingFromDescriptor(fieldDescriptor, field)
        case None =>
          true
      }
    }

  private def nestedFieldMissingFromDescriptor(tableField: Descriptors.FieldDescriptor, bqField: BQField): Boolean =
    tableField.getType match {
      case Descriptors.FieldDescriptor.Type.MESSAGE =>
        Option(bqField.getSubFields) match {
          case Some(nestedFields) =>
            fieldsMissingFromDescriptor(tableField.getMessageType, nestedFields)
          case _ =>
            false
        }
      case _ =>
        false
    }

  def alterTableRequired(tableDescriptor: Descriptors.Descriptor, ddlFields: Vector[Field]): Vector[Field] =
    ddlFields.filter { field =>
      Option(tableDescriptor.findFieldByName(field.name)) match {
        case Some(fieldDescriptor) =>
          val nullableMismatch = fieldDescriptor.isRequired && field.nullability.nullable
          nullableMismatch || alterTableRequiredForNestedFields(fieldDescriptor, field)
        case None =>
          true
      }
    }

  private def alterTableRequiredForNestedFields(tableField: Descriptors.FieldDescriptor, ddlField: Field): Boolean =
    tableField.getType match {
      case Descriptors.FieldDescriptor.Type.MESSAGE =>
        ddlField.fieldType match {
          case Type.Struct(nestedFields) =>
            alterTableRequired(tableField.getMessageType, nestedFields.toVector).nonEmpty
          case Type.Array(Type.Struct(nestedFields), _) =>
            alterTableRequired(tableField.getMessageType, nestedFields.toVector).nonEmpty
          case _ =>
            false
        }
      case _ =>
        false
    }

  def mergeInColumns(bqFields: FieldList, ddlFields: Vector[Field]): FieldList = {
    val ddlFieldsByName = ddlFields.map(f => f.name -> f).toMap
    val bqFieldNames    = bqFields.asScala.map(f => f.getName).toSet
    val alteredExisting = bqFields.asScala.map { bqField =>
      ddlFieldsByName.get(bqField.getName) match {
        case Some(ddlField) => mergeField(bqField, ddlField)
        case None           => bqField
      }
    }
    val newColumns = ddlFields
      .filterNot(ddlField => bqFieldNames.contains(ddlField.name))
      .map { ddlField =>
        bqFieldOf(ddlField.copy(nullability = Type.Nullability.Nullable))
      }
    FieldList.of((alteredExisting ++ newColumns).asJava)
  }

  private def mergeField(bqField: BQField, ddlField: Field): BQField = {
    val fixedType: BQField = ddlField.fieldType match {
      case Type.Array(elementType, _) =>
        mergeField(bqField, ddlField.copy(fieldType = elementType))
      case Type.Struct(ddlNestedFields) =>
        Option(bqField.getSubFields) match {
          case Some(bqNestedFields) =>
            bqField.toBuilder
              .setType(StandardSQLTypeName.STRUCT, mergeInColumns(bqNestedFields, ddlNestedFields.toVector))
              .build
          case None =>
            bqField
        }
      case _ => bqField
    }

    if (bqField.getMode == BQField.Mode.REQUIRED && ddlField.nullability.nullable)
      fixedType.toBuilder.setMode(BQField.Mode.NULLABLE).build
    else
      fixedType
  }

  def bqFieldOf(ddlField: Field): BQField =
    ddlField.fieldType match {
      case Type.Array(elementType, _) =>
        bqFieldOf(ddlField.copy(fieldType = elementType)).toBuilder
          .setMode(BQField.Mode.REPEATED)
          .build
      case Type.Struct(nestedFields) =>
        val nested = FieldList.of(nestedFields.map(bqFieldOf).toVector.asJava)
        BQField
          .newBuilder(ddlField.name, StandardSQLTypeName.STRUCT, nested)
          .setMode(bqModeOf(ddlField.nullability))
          .build
      case Type.Decimal(_, scale) =>
        val bqType = if (scale > 9) StandardSQLTypeName.BIGNUMERIC else StandardSQLTypeName.NUMERIC
        simpleField(ddlField, bqType)
      case Type.String    => simpleField(ddlField, StandardSQLTypeName.STRING)
      case Type.Boolean   => simpleField(ddlField, StandardSQLTypeName.BOOL)
      case Type.Integer   => simpleField(ddlField, StandardSQLTypeName.INT64)
      case Type.Long      => simpleField(ddlField, StandardSQLTypeName.INT64)
      case Type.Double    => simpleField(ddlField, StandardSQLTypeName.FLOAT64)
      case Type.Date      => simpleField(ddlField, StandardSQLTypeName.DATE)
      case Type.Timestamp => simpleField(ddlField, StandardSQLTypeName.TIMESTAMP)
      case Type.Json      => simpleField(ddlField, StandardSQLTypeName.JSON)
    }

  private def simpleField(ddlField: Field, bqType: StandardSQLTypeName): BQField =
    BQField
      .newBuilder(ddlField.name, bqType)
      .setMode(bqModeOf(ddlField.nullability))
      .build

  private def bqModeOf(nullability: Type.Nullability): BQField.Mode =
    if (nullability.nullable) BQField.Mode.NULLABLE else BQField.Mode.REQUIRED

  private implicit val modeEq: Eq[BQField.Mode] = Eq.fromUniversalEquals

  def showDescriptor(descriptor: Descriptors.Descriptor): String =
    descriptor.getFields.asScala
      .map(_.getName)
      .mkString(", ")

}
