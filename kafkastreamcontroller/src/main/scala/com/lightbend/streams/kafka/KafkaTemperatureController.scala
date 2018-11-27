package com.lightbend.streams.kafka

import java.util.{HashMap, Properties}

import com.lightbend.stream.messages.messages.{HeaterCommand, HeaterControl}
import org.apache.kafka.common.serialization.{Serde, Serdes}
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import com.lightbend.streams.config.StreamsConfig._
import com.lightbend.streams.transform.DataTransformer
import org.apache.kafka.streams.state.Stores
import org.apache.kafka.streams.scala.Serdes._
import org.apache.kafka.streams.scala.ImplicitConversions._
import org.apache.kafka.streams.scala.StreamsBuilder

import scala.util.control.NonFatal


object KafkaTemperatureController {

  private val port = 8888 // Port for queryable state

  def main(args: Array[String]): Unit = {

    println(s"Akka Temperature controller, brokers ${kafkaConfig.brokers}")
    println(s"Input queue sensor ${kafkaConfig.heateroutputtopic}, input group ${kafkaConfig.heateroutputgroup} ")
    println(s"Input queue control ${kafkaConfig.temperaturesettopic}, input group ${kafkaConfig.temperaturesetgroup} ")
    println(s"Output queue ${kafkaConfig.heaterinputtopic}")

    val streamsConfiguration = new Properties
    // Give the Streams application a unique name.  The name must be unique in the Kafka cluster
    // against which the application is run.
    streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "dynamically-controlled-streams")
    streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, kafkaConfig.heatersourcegroup)
    // Where to find Kafka broker(s).
    streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConfig.brokers)
    // Provide the details of our embedded http service that we'll use to connect to this streams
    // instance and discover locations of stores.
    streamsConfiguration.put(StreamsConfig.APPLICATION_SERVER_CONFIG, "127.0.0.1:" + port)
    // Default serdes
    streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.ByteArray.getClass)
    streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.ByteArray.getClass)

    // Store definition
    val logConfig = new HashMap[String, String]
    val storeSupplier = Stores.inMemoryKeyValueStore("TemperatureController")
    val storeBuilder = Stores.keyValueStoreBuilder(storeSupplier, Serdes.Integer, new TemperatureControlSerde).withLoggingEnabled(logConfig)

    // Create Stream builder
    val builder = new StreamsBuilder
    // Data input streams
    val sensor  = builder.stream[Array[Byte], Array[Byte]](kafkaConfig.heateroutputtopic)
    val control  = builder.stream[Array[Byte], Array[Byte]](kafkaConfig.temperaturesettopic)

    // DataStore
    builder.addStateStore(storeBuilder)

    // Control stream processing
    control
      .mapValues(value => DataTransformer.controlFromByteArray(value))
      .filter((key, value) => value.isSuccess)
      .process(() => new TemperatureControlProcessor, "TemperatureController")

    // Sensor stream processing
    sensor
      .mapValues(value => DataTransformer.sensorFromByteArray(value))
      .filter((key, value) => value.isSuccess)
      .transform(new SensorDataTransformer, "TemperatureController")
      .mapValues(value => {
        value.sensorID match {
          case Some(result) =>
            println(s"sending new control ${value.command} for sensor $result")
            Some(HeaterControl(value.command, HeaterCommand.fromValue(value.command)))
          case _ =>
            None
        }
      })
      .filter((key, value) => value.isDefined).mapValues(v => DataTransformer.toByteArray(v.get))
      .to(kafkaConfig.heaterinputtopic)

    // Create and build topology
    val topology = builder.build
    println(topology.describe)

    // Create strems
    val streams = new KafkaStreams(topology, streamsConfiguration)

    streams.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
      override def uncaughtException(t: Thread, e: Throwable): Unit = {
        System.out.println("Uncaught exception on thread " + t + " " + e.toString)
      }
    })

    // Start streams
    streams.start()

    // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
    sys.addShutdownHook{
      try {
        streams.close()
      } catch {
        case NonFatal(e) => println(s"During streams.close(), received: $e")
      }
    }
  }
}