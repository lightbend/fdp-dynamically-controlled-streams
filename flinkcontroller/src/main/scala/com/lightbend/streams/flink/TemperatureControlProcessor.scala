package com.lightbend.streams.flink

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.co.CoProcessFunction
import org.apache.flink.util.Collector
import com.lightbend.stream.messages.messages.{HeaterCommand, HeaterControl, SensorData, TemperatureControl}

import scala.collection.mutable.Map


class TemperatureControlProcessor extends CoProcessFunction[SensorData, TemperatureControl, HeaterControl]{

  var currentSettings : ValueState[Option[TemperatureControl]] = _
  private val previousCommands = Map[Int, Int]()

  override def open(parameters: Configuration): Unit = {

    val currentSettingDesc = new ValueStateDescriptor[Option[TemperatureControl]](
      "currentTemperatureSetting",                               // state name
      createTypeInformation[Option[TemperatureControl]])                // type information
    currentSettings = getRuntimeContext.getState(currentSettingDesc)
  }


  override def processElement2(control: TemperatureControl, ctx: CoProcessFunction[SensorData, TemperatureControl, HeaterControl]#Context, out: Collector[HeaterControl]): Unit = {

    if(currentSettings.value == null) currentSettings.update(None)

    println(s"New temperature settings $control")
    currentSettings.update (Some(control))
  }

  override def processElement1(record: SensorData, ctx: CoProcessFunction[SensorData, TemperatureControl, HeaterControl]#Context, out: Collector[HeaterControl]): Unit = {

    if(currentSettings.value == null) currentSettings.update(None)

    currentSettings.value match {
      case Some(setting) => // We are controlling
        val action = (if(record.temperature > (setting.desired + setting.upDelta)) 1
          else if(record.temperature < (setting.desired - setting.downDelta)) 0 else -1)
        if((action >= 0) && (previousCommands.getOrElse(record.sensorID, -1) != action)){
          println(s"sending new control $action for sensor ${record.sensorID}")
          previousCommands += (record.sensorID -> action)
          out.collect(HeaterControl(record.sensorID, HeaterCommand.fromValue(action)))
        }
      case _ => // No control
    }
   }
}
