package com.lightbend.streams.kafka

import java.io.ByteArrayOutputStream
import java.util

import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}
import com.lightbend.stream.messages.messages.{TemperatureControl, HeaterControl}

class TemperatureControlSerde extends Serde[TemperatureControl] {

  private var mserializer = new TemperatureControlSerializer()
  private var mdeserializer = new TemperatureControlDeserializer()

  override def deserializer() = mdeserializer

  override def serializer() = mserializer

  override def configure(configs: util.Map[String, _], isKey: Boolean) = {}

  override def close() = {}
}


class TemperatureControlDeserializer extends Deserializer[TemperatureControl] {

  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}

  override def deserialize(topic: String, data: Array[Byte]): TemperatureControl = {
    TemperatureControl.parseFrom(data)
  }

  override def close(): Unit = {}

}

class TemperatureControlSerializer extends Serializer[TemperatureControl] {

  private val bos = new ByteArrayOutputStream()

  override def serialize(topic: String, state: TemperatureControl): Array[Byte] = {
    bos.reset
    state.writeTo(bos)
    bos.toByteArray
  }

  override def close(): Unit = {}

  override def configure(configs: util.Map[String, _], isKey: Boolean) = {}
}