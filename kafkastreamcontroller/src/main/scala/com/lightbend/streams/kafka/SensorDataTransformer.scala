package com.lightbend.streams.kafka

import java.util.Objects

import com.lightbend.stream.messages.messages.{SensorData, TemperatureControl}
import com.lightbend.streams.transform.MayBeHeaterControl
import org.apache.kafka.streams.kstream.Transformer
import org.apache.kafka.streams.processor.ProcessorContext
import org.apache.kafka.streams.state.KeyValueStore

import scala.util.Try
import scala.collection.mutable.Map

class SensorDataTransformer extends Transformer[Array[Byte], Try[SensorData], (Array[Byte], MayBeHeaterControl)]{

  private var controlStore: KeyValueStore[Integer, TemperatureControl] = null
  private val previousCommands = Map[Int, Int]()

  override def transform(key: Array[Byte], dataRecord: Try[SensorData]): (Array[Byte], MayBeHeaterControl) = {

    val sensor = dataRecord.get
    controlStore.get(sensor.sensorID) match {
      case setting if setting != null => // We are controlling
       (if(sensor.temperature > (setting.desired + setting.upDelta)) 1
          else if(sensor.temperature < (setting.desired - setting.downDelta)) 0 else -1) match {
          case action if(action < 0) => (key, MayBeHeaterControl(None, 0))
          case action =>
            val previousCommand = previousCommands.getOrElse(sensor.sensorID, -1)
            if(previousCommand != action) {
              previousCommands += (sensor.sensorID -> action)
              (key, MayBeHeaterControl(Some(sensor.sensorID), action))
            }
            else (key, MayBeHeaterControl(None, 0))
        }
      case _ => // No control
       (key, MayBeHeaterControl(None, 0))
    }
  }

  override def init(context: ProcessorContext): Unit = {
    controlStore = context.getStateStore("TemperatureController").asInstanceOf[KeyValueStore[Integer, TemperatureControl]]
    Objects.requireNonNull(controlStore, "State store can't be null")
  }

  override def close(): Unit = {}
}