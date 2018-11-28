package com.lightbend.streams.flink

import java.util.Properties

import com.lightbend.streams.transform.DataTransformer
import org.apache.flink.api.scala._
import org.apache.flink.configuration._
import org.apache.flink.runtime.concurrent.Executors
import org.apache.flink.runtime.highavailability.HighAvailabilityServicesUtils
import org.apache.flink.runtime.minicluster.LocalFlinkMiniCluster
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}
import com.lightbend.stream.messages.messages.{HeaterCommand, HeaterControl, SensorData, TemperatureControl}
import com.lightbend.streams.config.StreamsConfig._

object TemperatureControllerJob {

  def main(args: Array[String]): Unit = {
//    executeLocal()
    executeServer()
  }

  // Execute on the local Flink server - to test queariable state
  def executeServer() : Unit = {

    val config = new Configuration()
    config.setInteger(JobManagerOptions.PORT, flinkConfig.port)
    config.setString(JobManagerOptions.ADDRESS, "localhost")
    config.setInteger(TaskManagerOptions.NUM_TASK_SLOTS, flinkConfig.parallelism)

    // In a non MiniCluster setup queryable state is enabled by default.
    config.setString(QueryableStateOptions.PROXY_PORT_RANGE, "9069")
    config.setInteger(QueryableStateOptions.PROXY_NETWORK_THREADS, 2)
    config.setInteger(QueryableStateOptions.PROXY_ASYNC_QUERY_THREADS, 2)

    config.setString(QueryableStateOptions.SERVER_PORT_RANGE, "9067")
    config.setInteger(QueryableStateOptions.SERVER_NETWORK_THREADS, 2)
    config.setInteger(QueryableStateOptions.SERVER_ASYNC_QUERY_THREADS, 2)


    // Create a local Flink server
    val flinkCluster = new LocalFlinkMiniCluster(
      config,
      HighAvailabilityServicesUtils.createHighAvailabilityServices(
        config,
        Executors.directExecutor(),
        HighAvailabilityServicesUtils.AddressResolution.TRY_ADDRESS_RESOLUTION),
      false)
    try {
      // Start server and create environment
      flinkCluster.start(true)
      val env = StreamExecutionEnvironment.createRemoteEnvironment("localhost", flinkCluster.getLeaderRPCPort)
       // Build Graph
      buildGraph(env)
      val jobGraph = env.getStreamGraph.getJobGraph
      // Submit to the server and wait for completion
      flinkCluster.submitJobAndWait(jobGraph, false)
    } catch {
      case e: Exception => e.printStackTrace()
    }
  }

  // Execute localle in the environment
  def executeLocal() : Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    buildGraph(env)
    System.out.println("[info] Job ID: " + env.getStreamGraph.getJobGraph.getJobID)
    env.execute()
  }

  // Build execution Graph
  def buildGraph(env : StreamExecutionEnvironment) : Unit = {
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.enableCheckpointing(flinkConfig.checkpointingInterval)

    println(s"Flink Temperature controller, brokers ${kafkaConfig.brokers}")
    println(s"Input queue sensor ${kafkaConfig.heateroutputtopic}, input group ${kafkaConfig.heateroutputgroup} ")
    println(s"Input queue control ${kafkaConfig.temperaturesettopic}, input group ${kafkaConfig.temperaturesetgroup} ")
    println(s"Output queue ${kafkaConfig.heaterinputtopic}")

    // configure Kafka consumer
    // Data
    val sensorKafkaProps = new Properties
    sensorKafkaProps.setProperty("bootstrap.servers", kafkaConfig.brokers)
    sensorKafkaProps.setProperty("group.id", kafkaConfig.heateroutputgroup)
    // always read the Kafka topic from the current location
    sensorKafkaProps.setProperty("auto.offset.reset", "latest")

    // Settings
    val controlKafkaProps = new Properties
    controlKafkaProps.setProperty("bootstrap.servers", kafkaConfig.brokers)
    controlKafkaProps.setProperty("group.id", kafkaConfig.temperaturesetgroup)
    // always read the Kafka topic from the current location
    controlKafkaProps.setProperty("auto.offset.reset", "earliest")

    // create a Kafka consumers
    // Sensor
    val sensorConsumer = new FlinkKafkaConsumer011[Array[Byte]](
      kafkaConfig.heateroutputtopic,
      new ByteArraySchema,
      sensorKafkaProps
    )

    // Setting
    val controlConsumer = new FlinkKafkaConsumer011[Array[Byte]](
      kafkaConfig.temperaturesettopic,
      new ByteArraySchema,
      controlKafkaProps
    )

    val heaterProducer = new FlinkKafkaProducer011[Array[Byte]](
      kafkaConfig.brokers,                            // broker list
      kafkaConfig.heaterinputtopic,                   // target topic
      new ByteArraySchema)                            // serialization schema

    // Create input data streams
    val controlStream = env.addSource(controlConsumer)
    val sensorStream = env.addSource(sensorConsumer)

    // Read data from streams
    val controls = controlStream.map(DataTransformer.controlFromByteArray(_))
      .flatMap(BadDataHandler[TemperatureControl])
      .keyBy(_.sensorID)
    val sensors = sensorStream.map(DataTransformer.sensorFromByteArray(_))
      .flatMap(BadDataHandler[SensorData])
      .keyBy(_.sensorID)

    // Merge streams
    sensors
      .connect(controls)
      .process(new TemperatureControlProcessor())
      .map(value => DataTransformer.toByteArray(value))
      .addSink(heaterProducer)
  }
}