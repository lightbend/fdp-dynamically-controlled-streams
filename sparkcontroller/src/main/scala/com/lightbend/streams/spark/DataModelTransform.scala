package com.lightbend.streams.spark

import com.lightbend.stream.messages.messages.{HeaterCommand, HeaterControl, SensorData, TemperatureControl}
import com.lightbend.streams.transform.DataTransformer


case class UnifiedDataModel(sensorID : Int, sensorData: SensorData, temperatureControl: TemperatureControl)

object DataModelTransform {

  val failedModel = UnifiedDataModel(-1, null, null)

  def controlFromByteArrayUnified(message: Array[Byte]): UnifiedDataModel = {
    DataTransformer.controlFromByteArray(message) match {
      case result if (result.isSuccess) => UnifiedDataModel(result.get.sensorID, null, result.get)
      case _ => failedModel
    }
  }

  def sensorFromByteArrayUnified(message: Array[Byte]): UnifiedDataModel = {
    DataTransformer.sensorFromByteArray(message) match {
      case result if (result.isSuccess) => UnifiedDataModel(result.get.sensorID, result.get, null)
      case _ => failedModel
    }
  }

  def sensorFromByteArray(message: Array[Byte]): SensorData = {
    DataTransformer.sensorFromByteArray(message) match {
      case result if (result.isSuccess) => result.get
      case _ => SensorData(-1, -1)
    }
  }

  def toByteArray(control: InternalHeaterControl): Array[Byte] = {
    DataTransformer.toByteArray(HeaterControl(control.sensorID, HeaterCommand.fromValue(control.action)))
  }
}

case class TemperatureControlWithLastValue(lastValue : Int, temperatureControl : TemperatureControl)

// It looks like Spark don't know how to read ScalaPB's enums and (oneofs).
// https://github.com/scalapb/sparksql-scalapb/issues/11
// So I am using this for internal calculation
case class InternalHeaterControl(sensorID : Int, action : Int)