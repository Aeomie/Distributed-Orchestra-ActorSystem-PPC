package upmc.akka.leader

import akka.actor._

case class Message(content: String)

class DisplayActor extends Actor {

  // Import crash messages
  import upmc.akka.leader.CrashDisplayActor

  def receive = {

    case Message(content) => {
      println(content)
    }

    case CrashDisplayActor => {
      println("DisplayActor: Received crash command, throwing exception!")
      throw new RuntimeException("Test crash: DisplayActor")
    }

  }
}
