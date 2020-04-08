package com.heydev.labs.exercise

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Get}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.log4j.Logger

object HbaseExample {
  lazy val logger: Logger = Logger.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {
    logger.info("MyApp starting...")
    var connection: Connection = null
    try {
      val conf = HBaseConfiguration.create()
      conf.set("hbase.zookeeper.quorum", "35.184.255.239")
      connection = ConnectionFactory.createConnection(conf)
      val table = connection.getTable(TableName.valueOf("brianrkeeter:users"))
      val get = new Get(Bytes.toBytes("4793038"))
      val result = table.get(get)
      println(result)
    } catch {
      case e: Exception => logger.error("Error in main", e)
    } finally {
      if (connection != null) connection.close()
    }
  }
}