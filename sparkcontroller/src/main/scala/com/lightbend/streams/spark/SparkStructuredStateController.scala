package com.lightbend.streams.spark

import com.lightbend.stream.messages.messages.{SensorData, TemperatureControl}
import com.lightbend.streams.config.StreamsConfig._
import com.lightbend.streams.transform.DataTransformer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent

import scala.collection._


object SparkStructuredStateControllerl {


  def main(args: Array[String]): Unit = {

    println(s"Spark Temperature controller, brokers ${kafkaConfig.brokers}")
    println(s"Input queue sensor ${kafkaConfig.heateroutputtopic}, input group ${kafkaConfig.heateroutputgroup} ")
    println(s"Input queue control ${kafkaConfig.temperaturesettopic}, input group ${kafkaConfig.temperaturesetgroup} ")
    println(s"Output queue ${kafkaConfig.heaterinputtopic}")

    // Create context
    val sparkSession = SparkSession.builder
      .appName("SparkModelServer").master("local[3]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.streaming.checkpointLocation", sparkConfig.checkpointingDir)
      .getOrCreate()

    val ssc = new StreamingContext(sparkSession.sparkContext, Seconds(1))
    sparkSession.sparkContext.setLogLevel("ERROR")
    import sparkSession.implicits._

    // Message parsing
    // In order to be able to uninon both streams we are using here combined format
    sparkSession.udf.register("deserializeData", (data: Array[Byte]) =>  DataModelTransform.sensorFromByteArray(data))

    // Current state of temperature settings
    val currenSettings = mutable.Map[Int, TemperatureControl]()

    // Create broadcast variable for the sink definition
    val temperatureControlProcessor = sparkSession.sparkContext.broadcast(new TemperatureController)

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
      .select("data.sensorID", "data.temperature")
      .as[SensorData].filter(_.sensorID >= 0)
      .map(data => {
        temperatureControlProcessor.value.control(data, currenSettings.get(data.sensorID))
      }).as[InternalHeaterControl].filter(_.sensorID >= 0)
      .map(DataModelTransform.toByteArray(_))

    // Start continious query writing result to kafka
    var sensorQuery = sensorstream
      .writeStream
      .outputMode("update")
      .format("kafka")
      .option("kafka.bootstrap.servers", kafkaConfig.brokers)
      .option("topic", kafkaConfig.heaterinputtopic)
      .trigger(Trigger.Continuous("5 second"))
      .start

    // Create settings kafka stream
    val kafkaParams = KafkaSupport.getKafkaConsumerConfig(kafkaConfig.brokers)
    val modelsStream = KafkaUtils.createDirectStream[Array[Byte], Array[Byte]](ssc,PreferConsistent,
      Subscribe[Array[Byte], Array[Byte]](Set(kafkaConfig.temperaturesettopic),kafkaParams))

    // Control stream
    modelsStream.foreachRDD( rdd =>
      if (!rdd.isEmpty()) {
        val settings = rdd.map(_.value).collect
          .map(DataTransformer.controlFromByteArray(_)).filter(_.isSuccess).map(_.get)
        val newSettings = settings.map(setting => {
          println(s"New temperature settings $setting")
          // Update state with the new model
          (setting.sensorID -> setting)
        }).toMap

        // Stop currently running data stream
        sensorQuery.stop

        // Merge maps
        newSettings.foreach{ case (name, value) => {
          currenSettings(name) = value
        }}

        // restatrt data stream
        sensorQuery = sensorstream
          .writeStream
          .outputMode("update")
          .format("kafka")
          .option("kafka.bootstrap.servers", kafkaConfig.brokers)
          .option("topic", kafkaConfig.heaterinputtopic)
          .trigger(Trigger.Continuous("5 second"))
          .start
      }
    )

    // Execute
    ssc.start()
    ssc.awaitTermination()
  }
}

// Process data
class TemperatureController{
  var previousControl = -1

  // Calculate control
  def control(data: SensorData, controlSetting : Option[TemperatureControl]) : InternalHeaterControl = {
    controlSetting match {
      case Some (setting) =>
        (if (data.temperature > (setting.desired + setting.upDelta)) 1
        else if (data.temperature < (setting.desired - setting.downDelta)) 0 else -1) match {
          case action if (action >= 0) =>
            if(action != previousControl) {
              println(s"sending new control $action for sensor ${data.sensorID}")
              previousControl = action
              InternalHeaterControl(data.sensorID, action)
            }
            else InternalHeaterControl(-1, -1)
          case _ => InternalHeaterControl(-1, -1)
        }
      case _ => InternalHeaterControl(-1, -1)
    }
  }
}