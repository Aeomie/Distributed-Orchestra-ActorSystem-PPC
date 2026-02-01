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

// Bully Election Messages
case object Alive
case class AliveResponse(id: Int, creationTime: Long, ref: ActorRef)
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
      // Wait for cluster to stabilize before starting election
      context.system.scheduler.scheduleOnce(2.seconds) {
        self ! StartElection
      }
    }

    case StartElection => {
      if (!electionInProgress) {
        startBullyElection()
      }
    }

    case TryClaimChef => {
      // Deprecated, kept for compatibility
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
      context.system.scheduler.scheduleOnce(1.seconds) {
        val otherIds = terminaux.filter(_.id != this.id).map(_.id)
        for (otherId <- otherIds) {
          displayActor ! Message(
            s"Musicien $id: Musicien $otherId - test completed"
          )
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
        creationTime = this.creationTime,
        ref = self
      )
      sender() ! response
    }

    case resp: AliveResponse => {
      if (electionInProgress) {
        displayActor ! Message(
          s"Musicien $id: got AliveResponse from ${resp.id}"
        )
        aliveResponses += (resp.id -> (resp.creationTime, resp.ref))
      }
    }

    case ChefAnnouncement(chefId, chefCreationTime, chefRef) => {
      displayActor ! Message(
        s"Musicien $id: received ChefAnnouncement from $chefId"
      )

      val isBetterChef =
        currentChefId.isEmpty ||
          chefCreationTime < currentChefCreationTime.get ||
          (chefCreationTime == currentChefCreationTime.get && chefId < currentChefId.get)

      if (isBetterChef) {
        // Unwatch old chef
        if (currentChefRef.isDefined && !isChef) {
          context.unwatch(currentChefRef.get)
        }

        // Follow new chef
        currentChefId = Some(chefId)
        currentChefCreationTime = Some(chefCreationTime)
        currentChefRef = Some(chefRef)
        isChef = (chefId == this.id)

        displayActor ! Message(
          s"Musicien $id: now following Chef $chefId"
        )

        context.watch(chefRef)
        electionInProgress = false

        if (!electionCancellable.isCancelled) {
          electionCancellable.cancel()
        }
      } else {
        displayActor ! Message(
          s"Musicien $id: ignoring worse chef from $chefId"
        )
      }
    }

    case Terminated(ref) if currentChefRef.contains(ref) && !isChef => {
      displayActor ! Message(
        s"Musicien $id: Chef terminated, starting new election"
      )
      currentChefRef = None
      currentChefId = None
      currentChefCreationTime = None
      context.unwatch(ref)
      self ! StartElection
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

    // ============ Bully Election Messages (Chef Mode) ============
    case Alive => {
      displayActor ! Message(
        s"Chef $id: received Alive? from ${sender().path.name}"
      )
      val response = AliveResponse(
        id = this.id,
        creationTime = this.creationTime,
        ref = self
      )
      sender() ! response
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
    if (!electionCancellable.isCancelled) {
      electionCancellable.cancel()
    }
    electionCancellable = context.system.scheduler.scheduleOnce(500.millis) {
      self ! continueElection()
    }
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
      electionCancellable = context.system.scheduler.scheduleOnce(100.millis) {
        self ! electChefFromInfo()
      }
    }
  }

  /** Elect chef from collected information
    */
  private def electChefFromInfo(): Unit = {
    if (!electionInProgress) return

    // We already have all info from AliveResponse messages
    val allCandidates = aliveResponses.map { case (id, (time, _)) =>
      (id, time)
    } ++ Map(this.id -> this.creationTime)

    // Find winner: earliest creation time, or smallest id if same time
    val winner = allCandidates.minBy { case (id, time) =>
      (time, id)
    }
    val (winnerId, winnerCreationTime) = winner

    displayActor ! Message(
      s"Musicien $id: elected Chef $winnerId"
    )

    if (winnerId == this.id) {
      // I won!
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
      // Someone else won - wait for THEM to announce themselves
      displayActor ! Message(
        s"Musicien $id: waiting for Chef $winnerId to announce (2s timeout)"
      )

      // Wait 3 seconds for winner's announcement
      if (!electionCancellable.isCancelled) {
        electionCancellable.cancel()
      }
      electionCancellable = context.system.scheduler.scheduleOnce(3.seconds) {
        if (electionInProgress && currentChefId.isEmpty) {
          // Winner didn't announce - probably crashed
          displayActor ! Message(
            s"Musicien $id: Chef $winnerId didn't announce, recalculating"
          )
          electionInProgress = false
          startBullyElection()
        }
      }
    }
  }
}
