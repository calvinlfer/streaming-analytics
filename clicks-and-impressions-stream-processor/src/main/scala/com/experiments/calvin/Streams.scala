package com.experiments.calvin

import akka.NotUsed
import akka.stream.FlowShape
import akka.stream.scaladsl._
import com.experiments.calvin.models._
import io.circe._
import io.circe.parser._

trait Streams {
  def filterBadMessages[A](implicit decoder: Decoder[A]): Flow[KafkaEnvelope[String], KafkaEnvelope[A], NotUsed] = {
    val cleaner: Flow[KafkaEnvelope[Either[Error, A]], KafkaEnvelope[A], NotUsed] = Flow.fromGraph(GraphDSL.create() {
      implicit builder =>
        import GraphDSL.Implicits._

        val broadcast = builder.add(Broadcast[KafkaEnvelope[Either[io.circe.Error, A]]](2))

        // Bad Messages (commit them)
        // TODO: should consider pushing them to an error topic
        broadcast
          .out(0)
          .filter(_.payload.isLeft)
          .map(_.offset)
          .map(_.commitScaladsl())
          .to(Sink.ignore)

        // Good messages
        val output = broadcast.out(1).filter(_.payload.isRight).map(ke => ke.copy(payload = ke.payload.right.get))

        FlowShape(broadcast.in, output.outlet)
    })
    Flow[KafkaEnvelope[String]]
      .map { case KafkaEnvelope(payloadStr, offset) => KafkaEnvelope(decode[A](payloadStr), offset) }
      .via(cleaner)
  }

}
