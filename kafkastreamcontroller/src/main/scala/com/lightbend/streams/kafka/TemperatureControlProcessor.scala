package com.lightbend.streams.kafka

import java.util.Objects

import org.apache.kafka.streams.processor.{AbstractProcessor, ProcessorContext}
import org.apache.kafka.streams.state.KeyValueStore

import com.lightbend.stream.messages.messages.TemperatureControl
import scala.util.Try

class TemperatureControlProcessor extends AbstractProcessor[Array[Byte], Try[TemperatureControl]] {

  private var controlStore: KeyValueStore[Integer, TemperatureControl] = null

  override def process (key: Array[Byte], temperatureControl: Try[TemperatureControl]): Unit = {

    val control = temperatureControl.get
    controlStore.put(control.sensorID, control)
  }

  override def init(context: ProcessorContext): Unit = {
    controlStore = context.getStateStore("TemperatureController").asInstanceOf[KeyValueStore[Integer, TemperatureControl]]
    Objects.requireNonNull(controlStore, "State store can't be null")
  }
}