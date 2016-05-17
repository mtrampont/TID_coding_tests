import sbt._
import sbt.Keys._

lazy val commonSettings = Seq(
  organization := "com.github.mtrampont",
  scalaVersion := "2.11.7" //, scalacOptions += "-feature"
)

lazy val coding_tests = (project in file("coding_tests")).
    settings(commonSettings: _*).
    settings(
      name := "coding_tests",
      libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.0"
    )

lazy val root = (project in file(".")).
    aggregate(coding_tests).
    settings(commonSettings: _*).
    settings(
      aggregate in update := false
    )
