package upmc.akka.leader

import akka.actor._
import scala.concurrent.duration._

case object Start
case object Abort
case object StartPerformance
case object StartTest
case object StartTestDone
case object AreYouAlive
case object ImAlive
case object ContinueElection
case object ComputeWinner
case object WhoIsChef
case object AskingChefTimeOut
case class ChefHere(id: Int, creationTime: Long)
case class Unregister(id: Int)
case object NoChefHere
case class WinnerAnnounceTimeout(winnerId: Int)

case class ChefIs(ref: ActorRef)
case class Register(id: Int)
case object WindowExpired

// Bully Election Messages
case object Alive
case class AliveResponse(id: Int, creationTime: Long)
case class ChefAnnouncement(id: Int, creationTime: Long, ref: ActorRef)
case object StartElection

class Musicien(val id: Int, val terminaux: List[Terminal]) extends Actor {

  val displayActor = context.actorOf(Props[DisplayActor], name = "displayActor")

  // Creation time for bully election (lower = higher priority)
  val creationTime: Long = System.currentTimeMillis()

  // Chef state
  private var currentChefRef: Option[ActorRef] = None
  private var currentChefId: Option[Int] = None
  private var currentChefCreationTime: Option[Long] = None
  private var isChef: Boolean = false

  // Election state
  private var electionInProgress: Boolean = false
  private var aliveResponses: Map[Int, (Long, ActorRef)] = Map.empty
  private var electionCancellable: Cancellable = Cancellable.alreadyCancelled

  // Orchestra state (only used if I'm chef)
  private var chefSlotRef: Option[ActorRef] = None
  private var joined = Set.empty[Int]
  private var musicians = Map.empty[Int, ActorRef]
  private var cancellable: Cancellable = Cancellable.alreadyCancelled
  private var foundChef: Boolean = false
  private var responsesReceived: Int = 0

  def receive: Receive = follower

  private def follower: Receive = {

    // Initialisation
    case Start => {
      displayActor ! Message(s"Musicien $id: started as follower")
      for (terminal <- terminaux.filter(_.id != this.id)) {
        val remoteMusician = context.actorSelection(
          s"akka.tcp://MozartSystem${terminal.id}@${terminal.ip}:${terminal.port}/user/Musicien${terminal.id}"
        )
        remoteMusician ! WhoIsChef
      }
      electionCancellable = context.system.scheduler.scheduleOnce(
        500.millis,
        self,
        AskingChefTimeOut
      )
    }

    case StartElection => {
      if (!electionInProgress) {
        startBullyElection()
      }
    }

    case StartTest => {
      displayActor ! Message("Musicien " + this.id + " is created")
      displayActor ! Message(s"My path: ${self.path}")
      displayActor ! Message(s"System name: ${context.system.name}")
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
      context.system.scheduler.scheduleOnce(1.seconds, self, StartTestDone)
    }

    case StartTestDone => {
      val otherIds = terminaux.filter(_.id != this.id).map(_.id)
      for (otherId <- otherIds) {
        displayActor ! Message(
          s"Musicien $id: Musicien $otherId - test completed"
        )
      }
    }

    case AskingChefTimeOut => {
      if (!foundChef && !electionInProgress) {
        self ! StartElection
      }
    }

    case WhoIsChef => {
      sender() ! NoChefHere
    }
    case ChefHere(chefId, chefCreationTime) => {
      displayActor ! Message(
        s"Musicien $id: received ChefHere from $chefId"
      )

      val betterThanCurrent =
        currentChefId.isEmpty ||
          chefCreationTime < currentChefCreationTime.get ||
          (chefCreationTime == currentChefCreationTime.get && chefId < currentChefId.get)

      if (betterThanCurrent) {
        // unregister old chef
        currentChefRef.foreach { oldChef =>
          if (oldChef != sender()) oldChef ! Unregister(this.id)
          context.unwatch(oldChef)
        }

        // Stop any pending election timers
        electionInProgress = false
        // Follow new chef
        isChef = false
        foundChef = true
        currentChefId = Some(chefId)
        currentChefCreationTime = Some(chefCreationTime)
        if (!electionCancellable.isCancelled) electionCancellable.cancel()

        // We don't have the ActorRef here, so we can't watch it
        currentChefRef = Some(sender())
        context.watch(sender())
        sender() ! Register(this.id)
        displayActor ! Message(
          s"Musicien $id: now following Chef $chefId"
        )
      } else {
        displayActor ! Message(
          s"Musicien $id: ignoring worse chef from $chefId"
        )
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
        displayActor ! Message(s"Musicien $id: Musicien $senderId is alive")
      }
    }
    case StartPerformance => {
      displayActor ! Message(s"Musicien $id: starting to play 🎵")
    }
    case Abort => {
      displayActor ! Message(s"Musicien $id: abort received, stopping")
      context.stop(self)
    }

    // ============ Bully Election Messages (Follower Mode) ============
    case Alive => {
      displayActor ! Message(
        s"Musicien $id: received Alive? from ${sender().path.name}"
      )
      val response = AliveResponse(
        id = this.id,
        creationTime = this.creationTime
      )
      sender() ! response
    }

    case resp: AliveResponse => {
      if (electionInProgress) {
        displayActor ! Message(
          s"Musicien $id: got AliveResponse from ${resp.id}"
        )
        aliveResponses += (resp.id -> (resp.creationTime, sender()))
      }
    }

    case ContinueElection => {
      continueElection()
    }
    case ComputeWinner => {
      electChefFromInfo()
    }
    case WinnerAnnounceTimeout(winnerId) => {
      if (electionInProgress && currentChefId.isEmpty) {
        // Winner didn't announce - probably crashed
        displayActor ! Message(
          s"Musicien $id: Chef $winnerId didn't announce, recalculating"
        )
        electionInProgress = false
        startBullyElection()
      }
    }
    case ChefAnnouncement(chefId, chefCreationTime, chefRef) => {
      displayActor ! Message(
        s"Musicien $id: received ChefAnnouncement from $chefId"
      )

      val betterThanCurrent =
        currentChefId.isEmpty ||
          chefCreationTime < currentChefCreationTime.get ||
          (chefCreationTime == currentChefCreationTime.get && chefId < currentChefId.get)

      if (betterThanCurrent) {
        if (!electionCancellable.isCancelled) electionCancellable.cancel()

        // STOP ELECTION IMMEDIATELY
        electionInProgress = false
        aliveResponses = Map.empty

        // Unwatch old chef if different
        currentChefRef.foreach { old =>
          if (old != chefRef) context.unwatch(old)
        }

        // If the announcement says *I* am the chef, actually become chef
        if (chefId == this.id) {
          // If I'm already chef, ignore; else switch properly
          if (!isChef) becomeChef()
        } else {
          // Follow new chef
          isChef = false
          foundChef = true
          currentChefId = Some(chefId)
          currentChefCreationTime = Some(chefCreationTime)
          currentChefRef = Some(chefRef)
          context.watch(chefRef)
          chefRef ! Register(this.id)

          displayActor ! Message(
            s"Musicien $id: now following Chef $chefId"
          )
        }
      } else {
        displayActor ! Message(
          s"Musicien $id: ignoring worse chef from $chefId"
        )
      }
    }

    case Terminated(ref) if currentChefRef.contains(ref) && !isChef =>
      displayActor ! Message(
        s"Musicien $id: Chef terminated, starting new election"
      )

      // we no longer have a chef
      foundChef = false
      currentChefRef = None
      currentChefId = None
      currentChefCreationTime = None

      // stop any pending timers to avoid overlapping elections
      if (!electionCancellable.isCancelled) electionCancellable.cancel()
      electionInProgress = false

      // start election (guarded inside StartElection anyway)
      self ! StartElection

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

    case WhoIsChef => {
      sender() ! ChefHere(this.id, this.creationTime)
    }
    case Register(mid) => {
      musicians += (mid -> sender())
      joined += mid
      context.watch(sender())
      displayActor ! Message(
        s"Chef: musician $mid registered and joined (${joined.size})"
      )
      if (!cancellable.isCancelled)
        cancellable.cancel()
      sender() ! StartPerformance
      displayActor ! Message(s"Chef: musician $mid started playing 🎵")
    }
    case Unregister(mid) => {
      musicians -= mid
      joined -= mid
      displayActor ! Message(
        s"Chef: musician $mid unregistered (${joined.size})"
      )
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

    // ============ Bully Election Messages (Chef Mode) ============
    case Alive => {
      displayActor ! Message(
        s"Chef $id: received Alive? from ${sender().path.name}"
      )
      // Chef should announce itself, not just respond as alive
      sender() ! AliveResponse(this.id, this.creationTime)
    }
    case ChefAnnouncement(chefId, chefCreationTime, chefRef) => {
      // Chef checks if this is a better chef
      if (
        chefCreationTime < this.creationTime ||
        (chefCreationTime == this.creationTime && chefId < this.id)
      ) {
        displayActor ! Message(
          s"Chef $id: stepping down for better chef $chefId"
        )
        isChef = false
        context.become(follower)
        self ! ChefAnnouncement(chefId, chefCreationTime, chefRef)
      }
    }

  }

  private def becomeChef(): Unit = {
    displayActor ! Message(s"Musicien $id CLAIMED CHEF role")

    // Stop election state
    electionInProgress = false
    if (!electionCancellable.isCancelled) electionCancellable.cancel()

    isChef = true
    currentChefId = Some(this.id)
    currentChefCreationTime = Some(this.creationTime)
    currentChefRef = Some(self)

    context.become(chef)
    self ! Start
  }

  /** Start the bully election process:
    *   1. Send Alive? to all other musicians
    *   2. If no one answers -> I become chef
    *   3. If someone answers -> check if there's a chef or elect one
    */
  private def startBullyElection(): Unit = {
    currentChefRef.foreach(context.unwatch)
    currentChefRef = None
    currentChefId = None
    currentChefCreationTime = None
    isChef = false
    electionInProgress = true
    aliveResponses = Map.empty

    displayActor ! Message(
      s"Musicien $id: starting election (creationTime=$creationTime)"
    )

    val otherMusicians = terminaux.filter(_.id != this.id)

    // Step 1: Ask who is alive
    for (terminal <- otherMusicians) {
      val remoteMusician = context.actorSelection(
        s"akka.tcp://MozartSystem${terminal.id}@${terminal.ip}:${terminal.port}/user/Musicien${terminal.id}"
      )
      remoteMusician ! Alive
    }

    // Wait 500ms for responses
    if (!electionCancellable.isCancelled) electionCancellable.cancel()
    electionCancellable =
      context.system.scheduler.scheduleOnce(500.millis, self, ContinueElection)
  }

  /** Continue election after Alive? timeout
    */
  private def continueElection(): Unit = {
    if (!electionInProgress) return

    if (aliveResponses.isEmpty) {
      // No one answered -> I'm the new chef
      displayActor ! Message(s"Musicien $id: I WON! Becoming chef...")
      becomeChef()

      // Announce to all others
      val otherMusicians = terminaux.filter(_.id != this.id)
      for (terminal <- otherMusicians) {
        val remoteMusician = context.actorSelection(
          s"akka.tcp://MozartSystem${terminal.id}@${terminal.ip}:${terminal.port}/user/Musicien${terminal.id}"
        )
        remoteMusician ! ChefAnnouncement(this.id, this.creationTime, self)
      }
    } else {
      // Someone answered - collect info from all alive musicians
      val otherMusicians = aliveResponses.keys.toList
      displayActor ! Message(
        s"Musicien $id: got responses from ${otherMusicians.mkString(", ")}"
      )

      // Wait a bit then calculate winner (we already have all info)
      if (!electionCancellable.isCancelled) {
        electionCancellable.cancel()
      }
      // Wait a bit then calculate winner (we already have all info)
      if (!electionCancellable.isCancelled) electionCancellable.cancel()
      electionCancellable =
        context.system.scheduler.scheduleOnce(100.millis, self, ComputeWinner)

    }
  }

  /** Elect chef from collected information
    */
  private def electChefFromInfo(): Unit = {
    if (!electionInProgress) return

    // Build candidate list: everyone who responded + me
    val allCandidates: Map[Int, Long] =
      aliveResponses.map { case (mid, (ctime, _)) => (mid, ctime) } +
        (this.id -> this.creationTime)

    // Winner: earliest creationTime, tie -> smallest id
    val (winnerId, winnerCreationTime) =
      allCandidates.minBy { case (mid, ctime) => (ctime, mid) }

    displayActor ! Message(s"Musicien $id: elected Chef $winnerId")

    if (winnerId == this.id) {
      // I won
      becomeChef()

      // Announce to all others (optional but useful)
      for (terminal <- terminaux.filter(_.id != this.id)) {
        val remoteMusician = context.actorSelection(
          s"akka.tcp://MozartSystem${terminal.id}@${terminal.ip}:${terminal.port}/user/Musicien${terminal.id}"
        )
        remoteMusician ! ChefAnnouncement(this.id, this.creationTime, self)
      }

    } else {
      // Someone else won -> follow immediately (no waiting)
      val winnerRef: ActorRef = aliveResponses(winnerId)._2

      foundChef = true
      electionInProgress = false
      if (!electionCancellable.isCancelled) electionCancellable.cancel()

      // Stop watching old chef if any
      currentChefRef.foreach(context.unwatch)

      isChef = false
      currentChefId = Some(winnerId)
      currentChefCreationTime = Some(winnerCreationTime)
      currentChefRef = Some(winnerRef)

      context.watch(winnerRef)
      winnerRef ! Register(this.id)

      displayActor ! Message(
        s"Musicien $id: now following elected Chef $winnerId"
      )
    }
  }

}
