package upmc.akka.leader

import akka.actor._
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

case object Start
case object TryClaimChef
case object Abort
case object StartPerformance
case object StartTest
case object AreYouAlive
case object ImAlive

case class ChefIs(ref: ActorRef)
case class Register(id: Int, ref: ActorRef)
case object WindowExpired

class Musicien(val id: Int, val terminaux: List[Terminal]) extends Actor {

  // Les differents acteurs du systeme
  val displayActor = context.actorOf(Props[DisplayActor], name = "displayActor")

  // Orchestra state (only used if I'm chef)
  private var chefSlotRef: Option[ActorRef] = None
  private var joined = Set.empty[Int]
  private var musicians = Map.empty[Int, ActorRef]
  private var cancellable: Cancellable = Cancellable.alreadyCancelled
  private var isChef: Boolean = false
  private var foundChef: Boolean = false
  private var responsesReceived: Int = 0
  private var aliveResponses: Set[Int] = Set.empty

  def receive: Receive = follower

  private def follower: Receive = {

    // Initialisation
    case Start => {
      // Wait for cluster to stabilize before trying to claim chef
      context.system.scheduler.scheduleOnce(2.seconds) {
        self ! TryClaimChef
      }
    }

    case StartTest => {
      displayActor ! Message("Musicien " + this.id + " is created")
      displayActor ! Message(s"My path: ${self.path}")
      displayActor ! Message(s"System name: ${context.system.name}")
      aliveResponses = Set.empty
      val otherMusicians = terminaux.filter(_.id != this.id)

      for (terminal <- otherMusicians) {
        val remoteMusician = context.actorSelection(
          s"akka.tcp://MozartSystem${terminal.id}@${terminal.ip}:${terminal.port}/user/Musicien${terminal.id}"
        )
        displayActor ! Message(
          s"Musicien $id: SENDING AreYouAlive to akka.tcp://MozartSystem${terminal.id}@${terminal.ip}:${terminal.port}/user/Musicien${terminal.id}"
        )
        remoteMusician ! AreYouAlive
      }

      // Wait 1 second then show results
      context.system.scheduler.scheduleOnce(1.seconds) {
        val otherIds = terminaux.filter(_.id != this.id).map(_.id)
        for (otherId <- otherIds) {
          if (aliveResponses.contains(otherId)) {
            displayActor ! Message(s"Musicien $id: Musicien $otherId is ALIVE")
          } else {
            displayActor ! Message(
              s"Musicien $id: Musicien $otherId - NO RESPONSE"
            )
          }
        }
      }
    }

    case AreYouAlive => {
      displayActor ! Message(
        s"Musicien $id: RECEIVED AreYouAlive from ${sender().path}"
      )
      sender() ! ImAlive
    }

    case ImAlive => {
      displayActor ! Message(
        s"Musicien $id: RECEIVED ImAlive from ${sender().path}"
      )
      val senderPath = sender().path.toString
      val musicianIdMatch = """Musicien(\d+)""".r.findFirstMatchIn(senderPath)
      if (musicianIdMatch.nonEmpty) {
        val senderId = musicianIdMatch.get.group(1).toInt
        aliveResponses += senderId
      }
    }
    case StartPerformance => {
      displayActor ! Message(s"Musicien $id: starting to play 🎵")
    }
    case Abort => {
      displayActor ! Message(s"Musicien $id: abort received, stopping")
      context.stop(self)
    }

  }

  private def chef: Receive = {
    case Start => {
      displayActor ! Message(
        s"Chef $id: waiting for at least 1 musician to join (30s max)"
      )
      if (!cancellable.isCancelled)
        cancellable.cancel()
      cancellable =
        context.system.scheduler.scheduleOnce(30.seconds, self, WindowExpired)
      displayActor ! Message("Chef: 30s join window started")
    }

    case Register(mid, ref) => {
      musicians += (mid -> ref)
      joined += mid
      context.watch(ref)
      displayActor ! Message(s"Chef: musician $mid joined (${joined.size})")
      if (!cancellable.isCancelled)
        cancellable.cancel()
      ref ! StartPerformance
      displayActor ! Message(s"Chef: musician $mid started playing 🎵")
    }

    case Terminated(ref) => {
      musicians = musicians.filterNot { case (_, r) => r == ref }
      joined = joined.filter(id => musicians.contains(id))
      displayActor ! Message(
        s"Chef: a musician crashed, remaining=${joined.size}"
      )

      if (joined.isEmpty) {
        if (!cancellable.isCancelled) cancellable.cancel()
        cancellable =
          context.system.scheduler.scheduleOnce(30.seconds, self, WindowExpired)
        displayActor ! Message(
          "Chef: alone, waiting 30s for someone to come back"
        )
      }
    }

    case WindowExpired => {
      if (joined.isEmpty) {
        displayActor ! Message(
          "Chef: no musicians joined after 30 seconds, aborting performance"
        )
        context.stop(self)
        musicians.values.foreach(
          _ ! Abort
        )
      }
    }
  }

  private def becomeChef(): Unit = {
    displayActor ! Message(s"Musicien $id CLAIMED CHEF role")
    isChef = true
    context.watch(self)
    context.become(chef)
    self ! Start
  }
}
