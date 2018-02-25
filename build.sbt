organization := "com.experiments"
version := "0.1"

scalaVersion := "2.12.4"

resolvers += Resolver.jcenterRepo

val akka     = "com.typesafe.akka"
val akkaV    = "2.5.10"
val circe    = "io.circe"
val circeV   = "0.9.1"
val phantom  = "com.outworkers"
val phantomV = "2.20.1"

lazy val `frontend` =
  (project in file("frontend"))
    .enablePlugins(JavaAppPackaging)
    .settings(
      libraryDependencies ++= {
        val beachape = "com.beachape"
        Seq(
          akka                  %% "akka-actor"        % akkaV,
          akka                  %% "akka-slf4j"        % akkaV,
          akka                  %% "akka-stream"       % akkaV,
          akka                  %% "akka-http"         % "10.1.0-RC2",
          akka                  %% "akka-stream-kafka" % "0.19",
          circe                 %% "circe-core"        % circeV,
          circe                 %% "circe-generic"     % circeV,
          circe                 %% "circe-parser"      % circeV,
          circe                 %% "circe-java8"       % circeV,
          "de.heikoseeberger"   %% "akka-http-circe"   % "1.20.0-RC2",
          "org.scalatest"       %% "scalatest"         % "3.0.4" % Test,
          phantom               %% "phantom-dsl"       % phantomV,
          phantom               %% "phantom-jdk8"      % phantomV,
          beachape              %% "enumeratum"        % "1.5.12",
          beachape              %% "enumeratum-circe"  % "1.5.15",
          "org.scala-lang"      % "scala-reflect"      % scalaVersion.value,
          "ch.qos.logback"      % "logback-classic"    % "1.2.3",
          "org.codehaus.groovy" % "groovy"             % "2.4.12",
          "net.agkn"            % "hll"                % "1.6.0"
        )
      }
    )

lazy val `unique-users-stream-processor` =
  (project in file("unique-users-stream-processor"))
    .enablePlugins(JavaAppPackaging)
    .settings(
      libraryDependencies ++= {
        Seq(
          akka                  %% "akka-actor"        % akkaV,
          akka                  %% "akka-slf4j"        % akkaV,
          akka                  %% "akka-stream"       % akkaV,
          akka                  %% "akka-http"         % "10.1.0-RC2",
          akka                  %% "akka-stream-kafka" % "0.19",
          circe                 %% "circe-core"        % circeV,
          circe                 %% "circe-generic"     % circeV,
          circe                 %% "circe-parser"      % circeV,
          circe                 %% "circe-java8"       % circeV,
          "org.scalatest"       %% "scalatest"         % "3.0.4" % Test,
          phantom               %% "phantom-dsl"       % phantomV,
          phantom               %% "phantom-jdk8"      % phantomV,
          "org.scala-lang"      % "scala-reflect"      % scalaVersion.value,
          "ch.qos.logback"      % "logback-classic"    % "1.2.3",
          "org.codehaus.groovy" % "groovy"             % "2.4.12",
          "net.agkn"            % "hll"                % "1.6.0"
        )
      }
    )

lazy val `clicks-and-impressions-stream-processor` =
  (project in file("clicks-and-impressions-stream-processor"))
    .enablePlugins(JavaAppPackaging)
    .settings(
      libraryDependencies ++= {
        Seq(
          akka                  %% "akka-actor"        % akkaV,
          akka                  %% "akka-slf4j"        % akkaV,
          akka                  %% "akka-stream"       % akkaV,
          akka                  %% "akka-http"         % "10.1.0-RC2",
          akka                  %% "akka-stream-kafka" % "0.19",
          circe                 %% "circe-core"        % circeV,
          circe                 %% "circe-generic"     % circeV,
          circe                 %% "circe-parser"      % circeV,
          circe                 %% "circe-java8"       % circeV,
          "org.scalatest"       %% "scalatest"         % "3.0.4" % Test,
          phantom               %% "phantom-dsl"       % phantomV,
          phantom               %% "phantom-jdk8"      % phantomV,
          "org.scala-lang"      % "scala-reflect"      % scalaVersion.value,
          "ch.qos.logback"      % "logback-classic"    % "1.2.3",
          "org.codehaus.groovy" % "groovy"             % "2.4.12"
        )
      }
    )