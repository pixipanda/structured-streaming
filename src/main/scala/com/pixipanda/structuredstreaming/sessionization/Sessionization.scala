package com.pixipanda.structuredstreaming.sessionization

import org.apache.spark.sql.streaming.{OutputMode, GroupState, GroupStateTimeout}
import org.apache.spark.sql.{Dataset, Encoder, Encoders, SparkSession}


object  Sessionization {


  def main(args: Array[String]) {

    implicit val userEventEncoder: Encoder[UserEvent] = Encoders.kryo[UserEvent]
    implicit val userSessionEncoder: Encoder[Option[UserSession]] = Encoders.kryo[Option[UserSession]]

    val sparkSession = SparkSession.builder
      .master("local")
      .appName("Sessionization")
      .getOrCreate()


    import  sparkSession.implicits._

    val userEventsStream: Dataset[String] = sparkSession.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 50050)
      .load()
      .as[String]


    val updatedUserSession = userEventsStream
      .map(UserEvent.parseEvent)
      .groupByKey(_.userId)
      .mapGroupsWithState(GroupStateTimeout.ProcessingTimeTimeout())(updateSessionEvents)
      .flatMap(userSession => userSession)


    updatedUserSession.writeStream
      .outputMode(OutputMode.Update())
      .format("console")
      .option("truncate", "false")
      .option("checkpointLocation", "/tmp/structured")
      .start()
      .awaitTermination()
  }


  def updateSessionEvents( userId: Int,
                           userEvents: Iterator[UserEvent],
                           state: GroupState[UserSession]): Option[UserSession] = {
    if (state.hasTimedOut) {
      // We've timed out, lets extract the state and send it down the stream
      println("TimeOut")
      val userSession = state.getOption
      state.remove()
      userSession
    } else {
      /*
       New data has come in for the given user id. We'll look up the current state
       to see if we already have something stored. If not, we'll just take the current user events
       and update the state, otherwise will concatenate the user events we already have with the
       new incoming events.
       */
      val existingState = state.getOption


      val updatedUserSession = existingState.fold(UserSession(userEvents.toSeq))(existingUserSession =>
          UserSession(existingUserSession.userEvents ++ userEvents.toSeq))

      state.update(updatedUserSession)

      if (updatedUserSession.userEvents.exists(_.isLast)) {
        /*
         If we've received a flag indicating this should be the last event batch, let's close
         the state and send the user session downstream.
         */
        val userSession = state.getOption
        state.remove()
        userSession
      } else {

        println("Setting 30 seconds timeout")
        state.setTimeoutDuration("30 seconds")

        None
      }
    }
  }
}