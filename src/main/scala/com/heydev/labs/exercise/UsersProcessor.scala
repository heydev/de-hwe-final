package com.heydev.labs.exercise

import java.text.SimpleDateFormat

import org.apache.hadoop.hbase.client.{ConnectionFactory, Get, Table}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DateType, StringType, StructField, StructType}

object UsersProcessor {
  val HBASE_IP = "35.184.255.239"
  val COLUMN_FAMILY = "f1"
  val BIRTHDATE_COLUMN = "birthdate"
  val EMAIL_COLUMN = "mail"
  val NAME_COLUMN = "name"
  val SEX_COLUMN = "sex"
  val USERNAME_COLUMN = "username"
  val USERS_TABLE = "brianrkeeter:users"
  val BIRTHDATE_FORMAT = "yyyy-mm-dd"
  val USERS_SCHEMA = StructField("user", StructType(Array(
    StructField(BIRTHDATE_COLUMN, DateType, nullable = false),
    StructField(EMAIL_COLUMN, StringType, nullable = false),
    StructField(NAME_COLUMN, StringType, nullable = false),
    StructField(SEX_COLUMN, StringType, nullable = false),
    StructField(USERNAME_COLUMN, StringType, nullable = false)
  )))

  def loadUserData(customerId: String, table: Table) = {
    val get = new Get(Bytes.toBytes(customerId))
    val result = table.get(get)

    Row(
      Row(
        new java.sql.Date(new SimpleDateFormat(BIRTHDATE_FORMAT).parse(new String(result.getValue(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes(BIRTHDATE_COLUMN)))).getTime),
        new String(result.getValue(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes(EMAIL_COLUMN))),
        new String(result.getValue(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes(NAME_COLUMN))),
        new String(result.getValue(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes(SEX_COLUMN))),
        new String(result.getValue(Bytes.toBytes(COLUMN_FAMILY), Bytes.toBytes(USERNAME_COLUMN)))
      )
    )
  }

  def enrichUserData(rows: Iterator[Row], getCustomerId: (Row) => String) = {
    val conf = HBaseConfiguration.create()
    conf.set("hbase.zookeeper.quorum", HBASE_IP)
    val connection = ConnectionFactory.createConnection(conf)
    val table = connection.getTable(TableName.valueOf(USERS_TABLE))

    val result = rows.map { row =>
      val userData = loadUserData(getCustomerId(row), table)
      Row.fromSeq(row.toSeq ++ userData.toSeq)
    }

    result
  }
}
