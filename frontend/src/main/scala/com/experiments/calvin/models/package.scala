package com.experiments.calvin

import java.time.Instant

import enumeratum.EnumEntry._
import enumeratum._

package object models {
  sealed trait Event extends EnumEntry with Lowercase
  case object Event extends Enum[Event] with CirceEnum[Event] {
    val values = findValues

    case object Click      extends Event
    case object Impression extends Event
  }

  case class UserInteraction(timestamp: Instant, event: Event)
}
