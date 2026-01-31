package upmc.akka.leader

import akka.actor._
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

case class Start()
case class Chef()
case class WaitTimeOut()

class Musicien(val id: Int, val terminaux: List[Terminal]) extends Actor {

  // Les differents acteurs du systeme
  val displayActor = context.actorOf(Props[DisplayActor], name = "displayActor")

  def receive = {

    // Initialisation
    case Start => {
      displayActor ! Message("Musicien " + this.id + " is created")

    }
    case Chef => {
      displayActor ! Message("Musicien " + this.id + " is the chef d'orchestre")
      context.system.scheduler.scheduleOnce(30.seconds, self, WaitTimeOut)
    }

    case WaitTimeOut => {
      displayActor ! Message(
        "Musicien " + this.id + " timeout expired, starting to play"
      )
    }
  }
}
