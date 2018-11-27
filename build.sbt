import Dependencies._

name := "DynamicallyControlledStreams"

version := "0.1"

scalaVersion in ThisBuild := Versions.Scala
scalacOptions in ThisBuild ++= Seq("-target:jvm-1.8")


lazy val protobufs = (project in file("./protobufs"))
  .settings(
    PB.targets in Compile := Seq(
      scalapb.gen() -> (sourceManaged in Compile).value
    ))

// Configuration
lazy val support = (project in file("./support"))
  .settings(libraryDependencies ++=  configDependencies ++ kafkaBaseDependencies)
  .dependsOn(protobufs)

// Heater implementation
lazy val heater = (project in file("./heater"))
  .settings(libraryDependencies ++= Seq(kafka))
  .dependsOn(support)

// Akka stream controller implementation
lazy val akkastreamcontroller = (project in file("./akkastreamcontroller"))
  .settings(libraryDependencies ++= akkaServerDependencies)
  .dependsOn(support)

// Kafka stream controller implementation
lazy val kafkastreamcontroller = (project in file("./kafkastreamcontroller"))
  .settings(libraryDependencies ++= kafkaServerDependencies)
  .dependsOn(support)

// Flink controller implementation
lazy val flinkcontroller = (project in file("./flinkcontroller"))
  .settings(libraryDependencies ++= flinkServerDependencies)
  .dependsOn(support)

// Spark controller implementation
lazy val sparkcontroller = (project in file("./sparkcontroller"))
  .settings(libraryDependencies ++= sparkServerDependencies)
  .settings(dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-core"  % "2.6.7",
            dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.6.7"
  )
  .dependsOn(support)

lazy val DynamicallyControlledStream = (project in file("."))
  .aggregate(support, heater, protobufs, flinkcontroller, akkastreamcontroller, kafkastreamcontroller, sparkcontroller)