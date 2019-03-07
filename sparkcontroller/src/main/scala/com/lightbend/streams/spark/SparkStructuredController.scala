package com.lightbend.streams.spark

/**
  * Created by boris on 6/26/17.
  */

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{GroupState, GroupStateTimeout}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger

import com.lightbend.streams.config.StreamsConfig._

import scala.collection.mutable.ListBuffer

object SparkStructuredController {

  def main(args: Array[String]): Unit = {

    println(s"Spark Temperature controller, brokers ${kafkaConfig.brokers}")
    println(s"Input queue sensor ${kafkaConfig.heateroutputtopic}, input group ${kafkaConfig.heateroutputgroup} ")
    println(s"Input queue control ${kafkaConfig.temperaturesettopic}, input group ${kafkaConfig.temperaturesetgroup} ")
    println(s"Output queue ${kafkaConfig.heaterinputtopic}")

    // Create context
    val sparkSession = SparkSession.builder
      .appName("SparkModelServer").master("local")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.streaming.checkpointLocation", sparkConfig.checkpointingDir)
      .getOrCreate()

    sparkSession.sparkContext.setLogLevel("ERROR")
    import sparkSession.implicits._

    // Message parsing
    // In order to be able to uninon both streams we are using here combined format
    sparkSession.udf.register("deserializeData", (data: Array[Byte]) =>  DataModelTransform.sensorFromByteArrayUnified(data))
    sparkSession.udf.register("deserializeControl", (data: Array[Byte]) => DataModelTransform.controlFromByteArrayUnified(data))

    // Create data stream
    val sensorstream = sparkSession
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaConfig.brokers)
      .option("subscribe", kafkaConfig.heateroutputtopic)
      .option(ConsumerConfig.GROUP_ID_CONFIG, kafkaConfig.heateroutputgroup)
      .option(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
      .option("startingOffsets", "earliest")
      .option("failOnDataLoss", "false")
      .load().selectExpr("""deserializeData(value) AS data""")
      .select("data.sensorID", "data.sensorData", "data.temperatureControl")
      .as[UnifiedDataModel]

    // Create model stream
    val controlstream = sparkSession
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaConfig.brokers)
      .option("subscribe", kafkaConfig.temperaturesettopic)
      .option(ConsumerConfig.GROUP_ID_CONFIG, kafkaConfig.temperaturesetgroup)
      .option(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
      .option("startingOffsets", "earliest")
      .option("failOnDataLoss", "false")
      .load().selectExpr("""deserializeControl(value) AS data""")
      .select("data.sensorID", "data.sensorData", "data.temperatureControl")
      .as[UnifiedDataModel]

    // Order matters here - unioned stream is appended to the end. So in this case, all the control records will
    // be processed first and data records after them
    val heatercontrolstream = controlstream.union(sensorstream)
      .filter(_.sensorID >= 0)
      .groupByKey(_.sensorID)
      .mapGroupsWithState(GroupStateTimeout.NoTimeout())(controlTemperature).as[Seq[InternalHeaterControl]]
      .withColumn("value", explode($"value"))
      .select("value.sensorID", "value.action")
      .as[InternalHeaterControl]
      .map(DataModelTransform.toByteArray(_))

    heatercontrolstream.writeStream
      .outputMode("update")
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaConfig.brokers)
      .option("topic", kafkaConfig.heaterinputtopic)
      .trigger(Trigger.ProcessingTime("1 second"))
      .start

    //Wait for all streams to finish
    sparkSession.streams.awaitAnyTermination()
  }

  // A mapping function that implements actual Temperature Control
  // For some descriptions on documentation and how it works see:
  // http://www.waitingforcode.com/apache-spark-structured-streaming/stateful-transformations-mapgroupswithstate/read
  // and https://spark.apache.org/docs/latest/api/scala/index.html#org.apache.spark.sql.streaming.GroupState
  def controlTemperature(key: Int, values: Iterator[UnifiedDataModel], state: GroupState[TemperatureControlWithLastValue]) : Seq[InternalHeaterControl] = {
    var results = new ListBuffer[InternalHeaterControl]()
    values.foreach(value => {
      value.sensorData match {
        case null =>  // This is control
          println(s"New temperature settings ${value.temperatureControl}")
          val lastControl = if(state.exists) state.get.lastValue else -1
          state.update(TemperatureControlWithLastValue(lastControl, value.temperatureControl))
        case _ => // This is data
          if (state.exists) {
            val setting = state.get
            val action = (if (value.sensorData.temperature > (setting.temperatureControl.desired + setting.temperatureControl.upDelta)) 1
            else if (value.sensorData.temperature < (setting.temperatureControl.desired - setting.temperatureControl.downDelta)) 0 else -1)
            if((action >= 0) && (setting.lastValue != action)) {
              println(s"sending new control $action for sensor ${value.sensorID}")
              results += InternalHeaterControl(value.sensorID, action)
              state.update(TemperatureControlWithLastValue(action, setting.temperatureControl))
            }
          }
      }
    })
    results.toList
  }
}