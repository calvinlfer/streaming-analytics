organization := "com.experiments"
version := "0.1"

scalaVersion := "2.12.4"

resolvers += Resolver.jcenterRepo

lazy val `frontend` =
  (project in file("frontend"))
    .settings(
      libraryDependencies ++= {
        val akka     = "com.typesafe.akka"
        val circe    = "io.circe"
        val beachape = "com.beachape"
        val circeV   = "0.9.1"

        Seq(
          akka     %% "akka-http"         % "10.1.0-RC2",
          akka     %% "akka-stream"       % "2.5.9",
          akka     %% "akka-stream-kafka" % "0.19",
          circe    %% "circe-core"        % circeV,
          circe    %% "circe-generic"     % circeV,
          circe    %% "circe-parser"      % circeV,
          circe    %% "circe-java8"       % circeV,
          beachape %% "enumeratum"        % "1.5.12",
          beachape %% "enumeratum-circe"  % "1.5.15"
        )
      }
    )
