package upmc.akka.leader

import akka.actor._
import scala.concurrent.duration._
import upmc.akka.ppc.{PlayerActor, DataBaseActor}
import upmc.akka.ppc.DataBaseActor._
import scala.util.Random

// ==================== Core Messages ====================
case object Start
case object Abort
case object StartPerformance

// ==================== Basic Connectivity Messages ====================
case object AreYouAlive
case object ImAlive

// ==================== Chef Discovery Messages ====================
case object WhoIsChef
case class ChefHere(id: Int, creationTime: Long)
case object NoChefHere

// ==================== Registration Messages ====================
case class Register(id: Int)
case object RegisterAck
case class Unregister(id: Int)

// ==================== Orchestra Management Messages ====================
case object RequestMusic
case class FinishedPlaying(musicianId: Int)
case object WindowExpired

// ==================== Bully Election Messages ====================
case object StartElection
case object Alive
case class AliveResponse(id: Int, creationTime: Long)
case object ContinueElection
case object ComputeWinner
case class ChefAnnouncement(id: Int, creationTime: Long, ref: ActorRef)
case object AskingChefTimeOut
case class WinnerAnnounceTimeout(winnerId: Int)

// ==================== Heartbeat Messages ====================
case object ChefHeartbeat

class Musicien(val id: Int, val terminaux: List[Terminal]) extends Actor {
  import context.dispatcher
  import akka.actor.SupervisorStrategy._

  // ==================== Supervision Strategy ====================
  override val supervisorStrategy = OneForOneStrategy() { case _: Exception =>
    Restart // Restart any crashed child actor
  }

  // ==================== Child Actors ====================
  val displayActor = context.actorOf(Props[DisplayActor], name = "displayActor")
  val playerActor = context.actorOf(Props[PlayerActor], name = "playerActor")

  // ==================== Core Identity ====================
  val creationTime: Long =
    System.currentTimeMillis() // For bully election (lower = higher priority)

  // ==================== Chef State ====================
  private var currentChefRef: Option[ActorRef] = None
  private var currentChefId: Option[Int] = None
  private var currentChefCreationTime: Option[Long] = None
  private var isChef: Boolean = false

  // ==================== Election State ====================
  private var electionInProgress: Boolean = false
  private var aliveResponses: Map[Int, (Long, ActorRef)] = Map.empty
  private var electionCancellable: Cancellable = Cancellable.alreadyCancelled
  private var foundChef: Boolean = false

  // ==================== Orchestra State (Chef Only) ====================
  private var joined = Set.empty[Int]
  private var musicians = Map.empty[Int, ActorRef]
  private var currentPosition = 0 // 0-15 (16 measures)
  private var currentlyPlaying: Option[Int] = None
  private var requestQueue: List[Int] = List.empty
  private var pendingMusicianRef: Option[ActorRef] =
    None // Musician waiting for measure
  private var dbActor: Option[ActorRef] = None

  // ==================== Timers ====================
  private var cancellable: Cancellable = Cancellable.alreadyCancelled
  private var heartbeatCancellable: Cancellable = Cancellable.alreadyCancelled

  // ==================== Actor Behavior Entry Point ====================
  def receive: Receive = follower

  // ==================== FOLLOWER BEHAVIOR ====================
  private def follower: Receive = {

    // -------------------- Initialization --------------------
    case Start =>
      displayActor ! Message(s"Musicien $id: started as follower")
      discoverChef()

    // -------------------- Registration & Acknowledgment --------------------
    case RegisterAck =>
      handleRegistrationAck()

    case AskingChefTimeOut =>
      handleChefTimeout()

    // -------------------- Chef Discovery --------------------
    case WhoIsChef =>
      sender() ! NoChefHere

    case ChefHere(chefId, chefCreationTime) =>
      handleChefDiscovery(chefId, chefCreationTime)

    // -------------------- Basic Connectivity --------------------
    case AreYouAlive =>
      handleAliveCheck()

    case ImAlive =>
      handleAliveResponse()

    // -------------------- Performance --------------------
    case StartPerformance =>
      displayActor ! Message(s"Musicien $id: starting to play 🎵")

    case Measure(chords) =>
      playMeasure(chords)

    case PlayerActor.MeasureFinished =>
      notifyChefFinished()

    case Abort =>
      displayActor ! Message(s"Musicien $id: abort received, stopping")
      context.system.terminate()

    // -------------------- Bully Election (Follower Mode) --------------------
    case StartElection =>
      if (!electionInProgress) startBullyElection()

    case Alive =>
      handleElectionAliveQuery()

    case resp: AliveResponse =>
      handleElectionAliveResponse(resp)

    case ContinueElection =>
      continueElection()

    case ComputeWinner =>
      electChefFromInfo()

    case WinnerAnnounceTimeout(winnerId) =>
      handleWinnerTimeout(winnerId)

    case ChefAnnouncement(chefId, chefCreationTime, chefRef) =>
      handleChefAnnouncement(chefId, chefCreationTime, chefRef)

    // -------------------- Chef Termination --------------------
    case Terminated(ref) if currentChefRef.contains(ref) && !isChef =>
      handleChefTermination()
  }

  // ==================== CHEF BEHAVIOR ====================
  private def chef: Receive = {

    // -------------------- Initialization --------------------
    case Start =>
      initializeChef()

    // -------------------- Chef Discovery --------------------
    case WhoIsChef =>
      sender() ! ChefHere(this.id, this.creationTime)

    // -------------------- Music Distribution --------------------
    case Measure(chords) =>
      distributeMeasure(chords)

    // -------------------- Musician Registration --------------------
    case Register(mid) =>
      registerMusician(mid)

    case RequestMusic =>
      handleMusicRequest()

    case FinishedPlaying(mid) =>
      handleMusicianFinished(mid)

    case Unregister(mid) =>
      unregisterMusician(mid)

    // -------------------- Musician Failure --------------------
    case Terminated(ref) =>
      handleMusicianCrash(ref)

    case WindowExpired =>
      handleRegistrationTimeout()

    // -------------------- Bully Election (Chef Mode) --------------------
    case Alive =>
      handleElectionAliveQueryAsChef()

    case ChefHeartbeat =>
      sendHeartbeat()

    case ChefAnnouncement(chefId, chefCreationTime, chefRef) =>
      handleChefAnnouncementAsChef(chefId, chefCreationTime, chefRef)
  }

  // ==================== CORE BEHAVIORS ====================

  private def becomeChef(): Unit = {
    displayActor ! Message(s"Musicien $id CLAIMED CHEF role")

    // Stop election state
    electionInProgress = false
    if (!electionCancellable.isCancelled) electionCancellable.cancel()

    // Update state
    isChef = true
    currentChefId = Some(this.id)
    currentChefCreationTime = Some(this.creationTime)
    currentChefRef = Some(self)

    // Create DataBaseActor for music measures (clean up existing one first)
    dbActor.foreach(context.stop)
    dbActor = Some(
      context.actorOf(Props[DataBaseActor], name = "DataBaseActor")
    )
    currentPosition = 0

    // Switch behavior and initialize
    context.become(chef)
    self ! Start

    // Start heartbeat
    if (!heartbeatCancellable.isCancelled) heartbeatCancellable.cancel()
    heartbeatCancellable =
      context.system.scheduler.scheduleOnce(1.second, self, ChefHeartbeat)
  }

  // ==================== FOLLOWER HELPER METHODS ====================

  private def discoverChef(): Unit = {
    for (terminal <- terminaux.filter(_.id != this.id)) {
      val remoteMusician = context.actorSelection(
        s"akka.tcp://MozartSystem${terminal.id}@${terminal.ip}:${terminal.port}/user/Musicien${terminal.id}"
      )
      remoteMusician ! WhoIsChef
    }
    electionCancellable =
      context.system.scheduler.scheduleOnce(500.millis, self, AskingChefTimeOut)
  }

  private def handleRegistrationAck(): Unit = {
    displayActor ! Message(s"Musicien $id: registration acknowledged by chef")
    if (!electionCancellable.isCancelled) electionCancellable.cancel()
  }

  private def handleChefTimeout(): Unit = {
    if (!foundChef && !electionInProgress) {
      self ! StartElection
    } else if (foundChef && currentChefRef.isDefined) {
      displayActor ! Message(s"Musicien $id: retrying registration with chef")
      currentChefRef.foreach(_ ! Register(this.id))
      electionCancellable = context.system.scheduler.scheduleOnce(
        2.seconds,
        self,
        AskingChefTimeOut
      )
    }
  }

  private def handleChefDiscovery(chefId: Int, chefCreationTime: Long): Unit = {
    displayActor ! Message(s"Musicien $id: received ChefHere from $chefId")

    val betterThanCurrent = currentChefId.isEmpty ||
      chefCreationTime < currentChefCreationTime.get ||
      (chefCreationTime == currentChefCreationTime.get && chefId < currentChefId.get)

    if (betterThanCurrent) {
      // Unregister from old chef
      currentChefRef.foreach { oldChef =>
        if (oldChef != sender()) oldChef ! Unregister(this.id)
        context.unwatch(oldChef)
      }

      // Follow new chef
      electionInProgress = false
      isChef = false
      foundChef = true
      currentChefId = Some(chefId)
      currentChefCreationTime = Some(chefCreationTime)
      if (!electionCancellable.isCancelled) electionCancellable.cancel()

      currentChefRef = Some(sender())
      context.watch(sender())
      sender() ! Register(this.id)
      displayActor ! Message(s"Musicien $id: now following Chef $chefId")
    } else {
      displayActor ! Message(s"Musicien $id: ignoring worse chef from $chefId")
    }
  }

  private def handleAliveCheck(): Unit = {
    displayActor ! Message(
      s"Musicien $id: RECEIVED AreYouAlive from ${sender().path}"
    )
    sender() ! ImAlive
  }

  private def handleAliveResponse(): Unit = {
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

  private def playMeasure(chords: List[Chord]): Unit = {
    displayActor ! Message(
      s"Musicien $id: Playing measure with ${chords.length} chords"
    )
    playerActor ! Measure(chords)
  }

  private def notifyChefFinished(): Unit = {
    displayActor ! Message(s"Musicien $id: Finished playing, notifying chef")
    currentChefRef.foreach(_ ! FinishedPlaying(id))
  }

  private def handleChefTermination(): Unit = {
    displayActor ! Message(
      s"Musicien $id: Chef terminated, starting new election"
    )

    // Reset chef state
    foundChef = false
    currentChefRef = None
    currentChefId = None
    currentChefCreationTime = None

    // Stop timers and start election
    if (!electionCancellable.isCancelled) electionCancellable.cancel()
    electionInProgress = false
    self ! StartElection
  }

  // ==================== ELECTION HELPER METHODS ====================

  private def handleElectionAliveQuery(): Unit = {
    displayActor ! Message(
      s"Musicien $id: received Alive? from ${sender().path.name}"
    )
    val response = AliveResponse(id = this.id, creationTime = this.creationTime)
    sender() ! response
  }

  private def handleElectionAliveResponse(resp: AliveResponse): Unit = {
    if (electionInProgress) {
      displayActor ! Message(s"Musicien $id: got AliveResponse from ${resp.id}")
      aliveResponses += (resp.id -> (resp.creationTime, sender()))
    }
  }

  private def handleWinnerTimeout(winnerId: Int): Unit = {
    if (electionInProgress && currentChefId.isEmpty) {
      displayActor ! Message(
        s"Musicien $id: Chef $winnerId didn't announce, recalculating"
      )
      electionInProgress = false
      startBullyElection()
    }
  }

  private def handleChefAnnouncement(
      chefId: Int,
      chefCreationTime: Long,
      chefRef: ActorRef
  ): Unit = {
    val betterThanCurrent = currentChefId.isEmpty ||
      chefCreationTime < currentChefCreationTime.get ||
      (chefCreationTime == currentChefCreationTime.get && chefId < currentChefId.get)

    val isSameChef =
      currentChefId.contains(chefId) && currentChefCreationTime.contains(
        chefCreationTime
      )

    if (betterThanCurrent) {
      displayActor ! Message(
        s"Musicien $id: received ChefAnnouncement from $chefId"
      )

      if (!electionCancellable.isCancelled) electionCancellable.cancel()

      // Stop election immediately
      electionInProgress = false
      aliveResponses = Map.empty

      // Unwatch old chef if different
      currentChefRef.foreach { old =>
        if (old != chefRef) context.unwatch(old)
      }

      if (chefId == this.id) {
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
        displayActor ! Message(s"Musicien $id: now following Chef $chefId")
      }
    } else if (!isSameChef) {
      displayActor ! Message(
        s"Musicien $id: received ChefAnnouncement from $chefId"
      )
      displayActor ! Message(s"Musicien $id: ignoring worse chef from $chefId")
    }
  }

  /** Start the bully election process */
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

    // Send Alive queries to all other musicians
    for (terminal <- terminaux.filter(_.id != this.id)) {
      val remoteMusician = context.actorSelection(
        s"akka.tcp://MozartSystem${terminal.id}@${terminal.ip}:${terminal.port}/user/Musicien${terminal.id}"
      )
      remoteMusician ! Alive
    }

    // Wait for responses
    if (!electionCancellable.isCancelled) electionCancellable.cancel()
    electionCancellable =
      context.system.scheduler.scheduleOnce(500.millis, self, ContinueElection)
  }

  /** Continue election after Alive timeout */
  private def continueElection(): Unit = {
    if (!electionInProgress) return

    if (aliveResponses.isEmpty) {
      // No one answered -> I'm the new chef
      displayActor ! Message(s"Musicien $id: I WON! Becoming chef...")
      becomeChef()

      // Announce to all others
      for (terminal <- terminaux.filter(_.id != this.id)) {
        val remoteMusician = context.actorSelection(
          s"akka.tcp://MozartSystem${terminal.id}@${terminal.ip}:${terminal.port}/user/Musicien${terminal.id}"
        )
        remoteMusician ! ChefAnnouncement(this.id, this.creationTime, self)
      }
    } else {
      // Someone answered - calculate winner
      val otherMusicians = aliveResponses.keys.toList
      displayActor ! Message(
        s"Musicien $id: got responses from ${otherMusicians.mkString(", ")}"
      )

      if (!electionCancellable.isCancelled) electionCancellable.cancel()
      electionCancellable =
        context.system.scheduler.scheduleOnce(100.millis, self, ComputeWinner)
    }
  }

  /** Elect chef from collected information */
  private def electChefFromInfo(): Unit = {
    if (!electionInProgress) return

    // Build candidate list: everyone who responded + me
    val allCandidates: Map[Int, Long] =
      aliveResponses.map { case (mid, (ctime, _)) =>
        (mid, ctime)
      } + (this.id -> this.creationTime)

    // Winner: earliest creationTime, tie -> smallest id
    val (winnerId, winnerCreationTime) = allCandidates.minBy {
      case (mid, ctime) => (ctime, mid)
    }

    displayActor ! Message(s"Musicien $id: elected Chef $winnerId")

    if (winnerId == this.id) {
      // I won
      becomeChef()

      // Announce to all others
      for (terminal <- terminaux.filter(_.id != this.id)) {
        val remoteMusician = context.actorSelection(
          s"akka.tcp://MozartSystem${terminal.id}@${terminal.ip}:${terminal.port}/user/Musicien${terminal.id}"
        )
        remoteMusician ! ChefAnnouncement(this.id, this.creationTime, self)
      }
    } else {
      // Someone else won -> follow immediately
      val winnerRef: ActorRef = aliveResponses(winnerId)._2

      foundChef = true
      electionInProgress = false
      if (!electionCancellable.isCancelled) electionCancellable.cancel()

      currentChefRef.foreach(context.unwatch)

      isChef = false
      currentChefId = Some(winnerId)
      currentChefCreationTime = Some(winnerCreationTime)
      currentChefRef = Some(winnerRef)

      context.watch(winnerRef)
      winnerRef ! Register(this.id)

      // Set up timeout for registration acknowledgment
      electionCancellable = context.system.scheduler.scheduleOnce(
        2.seconds,
        self,
        AskingChefTimeOut
      )

      displayActor ! Message(
        s"Musicien $id: now following elected Chef $winnerId"
      )
    }
  }

  // ==================== CHEF HELPER METHODS ====================

  private def initializeChef(): Unit = {
    displayActor ! Message(
      s"Chef $id: waiting for at least 1 musician to join (30s max)"
    )
    if (!cancellable.isCancelled) cancellable.cancel()
    cancellable =
      context.system.scheduler.scheduleOnce(30.seconds, self, WindowExpired)
    displayActor ! Message("Chef: 30s join window started")
  }

  private def distributeMeasure(chords: List[Chord]): Unit = {
    pendingMusicianRef.foreach { musicianRef =>
      displayActor ! Message(
        s"Chef: Received measure ${currentPosition + 1}, sending to musician"
      )
      musicianRef ! Measure(chords)
    }
    pendingMusicianRef = None
    currentPosition = (currentPosition + 1) % 16
  }

  private def registerMusician(mid: Int): Unit = {
    musicians += (mid -> sender())
    joined += mid
    context.watch(sender())
    displayActor ! Message(
      s"Chef: musician $mid registered and joined (${joined.size})"
    )

    if (!cancellable.isCancelled) cancellable.cancel()

    // Send acknowledgment and start performance
    sender() ! RegisterAck
    sender() ! StartPerformance
    displayActor ! Message(s"Chef: musician $mid started playing 🎵")

    // Send music to first musician, queue others
    if (currentlyPlaying.isEmpty) {
      currentlyPlaying = Some(mid)
      sendMusic(mid, sender())
    } else {
      if (!requestQueue.contains(mid)) {
        requestQueue = requestQueue :+ mid
      }
    }
  }

  private def handleMusicRequest(): Unit = {
    val senderRef = sender()
    musicians.find(_._2 == senderRef).foreach { case (mid, ref) =>
      if (currentlyPlaying.isEmpty) {
        displayActor ! Message(
          s"Chef: Musician $mid requesting music, sending now"
        )
        currentlyPlaying = Some(mid)
        sendMusic(mid, ref)
      } else {
        if (!requestQueue.contains(mid)) {
          requestQueue = requestQueue :+ mid
          displayActor ! Message(
            s"Chef: Musician $mid queued (someone is playing)"
          )
        }
      }
    }
  }

  private def handleMusicianFinished(mid: Int): Unit = {
    displayActor ! Message(s"Chef: Musician $mid finished playing")
    currentlyPlaying = None

    // Pick next musician - either from queue or round-robin
    val nextMusician = if (requestQueue.nonEmpty) {
      val next = requestQueue.head
      requestQueue = requestQueue.tail
      next
    } else if (musicians.nonEmpty) {
      // Round-robin: pick next musician after the one who just finished
      val musicianIds = musicians.keys.toList.sorted
      val currentIndex = musicianIds.indexOf(mid)
      if (currentIndex >= 0) {
        musicianIds((currentIndex + 1) % musicianIds.size)
      } else {
        musicianIds.head
      }
    } else {
      mid // Fallback
    }

    musicians.get(nextMusician).foreach { ref =>
      displayActor ! Message(
        s"Chef: Sending next measure to Musician $nextMusician"
      )
      currentlyPlaying = Some(nextMusician)
      sendMusic(nextMusician, ref)
    }
  }

  private def unregisterMusician(mid: Int): Unit = {
    musicians -= mid
    joined -= mid
    displayActor ! Message(s"Chef: musician $mid unregistered (${joined.size})")
  }

  private def handleMusicianCrash(ref: ActorRef): Unit = {
    val crashedMusician = musicians.find(_._2 == ref).map(_._1)

    musicians = musicians.filterNot { case (_, r) => r == ref }
    joined = joined.filter(id => musicians.contains(id))
    displayActor ! Message(
      s"Chef: a musician crashed, remaining=${joined.size}"
    )

    // Clean up stale references
    crashedMusician.foreach { mid =>
      requestQueue = requestQueue.filterNot(_ == mid)

      if (currentlyPlaying.contains(mid)) {
        currentlyPlaying = None
        displayActor ! Message(
          s"Chef: Crashed musician $mid was playing, selecting next musician"
        )

        // Select next musician to continue performance
        if (joined.nonEmpty) {
          val nextMusician = if (requestQueue.nonEmpty) {
            val next = requestQueue.head
            requestQueue = requestQueue.tail
            next
          } else {
            musicians.keys.head
          }

          musicians.get(nextMusician).foreach { ref =>
            displayActor ! Message(
              s"Chef: Continuing performance with Musician $nextMusician"
            )
            currentlyPlaying = Some(nextMusician)
            sendMusic(nextMusician, ref)
          }
        }
      } else {
        displayActor ! Message(
          s"Chef: Musician $mid crashed but was not playing, continuing normally"
        )
      }
    }

    if (joined.isEmpty) {
      if (!cancellable.isCancelled) cancellable.cancel()
      cancellable =
        context.system.scheduler.scheduleOnce(30.seconds, self, WindowExpired)
      displayActor ! Message(
        "Chef: alone, waiting 30s for someone to come back"
      )
    }
  }

  private def handleRegistrationTimeout(): Unit = {
    if (joined.isEmpty) {
      displayActor ! Message(
        "Chef: no musicians joined after 30 seconds, aborting performance"
      )
      musicians.values.foreach(_ ! Abort)

      // Shutdown the entire actor system
      context.system.terminate()
    }
  }

  private def handleElectionAliveQueryAsChef(): Unit = {
    displayActor ! Message(
      s"Chef $id: received Alive? from ${sender().path.name}"
    )
    sender() ! AliveResponse(this.id, this.creationTime)
    sender() ! ChefAnnouncement(this.id, this.creationTime, self)
  }

  private def sendHeartbeat(): Unit = {
    for (terminal <- terminaux.filter(_.id != this.id)) {
      val remoteMusician = context.actorSelection(
        s"akka.tcp://MozartSystem${terminal.id}@${terminal.ip}:${terminal.port}/user/Musicien${terminal.id}"
      )
      remoteMusician ! ChefAnnouncement(this.id, this.creationTime, self)
    }
    heartbeatCancellable =
      context.system.scheduler.scheduleOnce(1.second, self, ChefHeartbeat)
  }

  private def handleChefAnnouncementAsChef(
      chefId: Int,
      chefCreationTime: Long,
      chefRef: ActorRef
  ): Unit = {
    // Chef checks if this is a better chef
    if (
      chefCreationTime < this.creationTime || (chefCreationTime == this.creationTime && chefId < this.id)
    ) {
      displayActor ! Message(s"Chef $id: stepping down for better chef $chefId")

      // Cancel heartbeat when stepping down
      if (!heartbeatCancellable.isCancelled) heartbeatCancellable.cancel()

      // Clean up DataBaseActor when stepping down
      dbActor.foreach { actor =>
        context.stop(actor)
        dbActor = None
      }

      isChef = false
      context.become(follower)
      self ! ChefAnnouncement(chefId, chefCreationTime, chefRef)
    }
  }
  // ==================== UTILITY METHODS ====================

  /** Roll dice and get measure number from Mozart's tables */
  private def rollDice(): Int = {
    val dice1 = Random.nextInt(6) + 1
    val dice2 = Random.nextInt(6) + 1
    val diceSum = dice1 + dice2

    // Mozart's tables (11 rows for dice sums 2-12, 8 columns for positions)
    val table1 = Array(
      Array(96, 22, 141, 41, 105, 122, 11, 30),
      Array(32, 6, 128, 63, 146, 46, 134, 81),
      Array(69, 95, 158, 13, 153, 55, 110, 24),
      Array(40, 17, 113, 85, 161, 2, 159, 100),
      Array(148, 74, 163, 45, 80, 97, 36, 107),
      Array(104, 157, 27, 167, 154, 68, 118, 91),
      Array(152, 60, 171, 53, 99, 133, 21, 127),
      Array(119, 84, 114, 50, 140, 86, 169, 94),
      Array(98, 142, 42, 156, 75, 129, 62, 123),
      Array(3, 87, 165, 61, 135, 47, 147, 33),
      Array(54, 130, 10, 103, 28, 37, 106, 5)
    )

    val table2 = Array(
      Array(70, 121, 26, 9, 112, 49, 109, 14),
      Array(117, 39, 126, 56, 174, 18, 116, 83),
      Array(66, 139, 15, 132, 73, 58, 145, 79),
      Array(90, 176, 7, 34, 67, 160, 52, 170),
      Array(25, 143, 64, 125, 76, 136, 1, 93),
      Array(138, 71, 150, 29, 101, 162, 23, 151),
      Array(16, 155, 57, 175, 43, 168, 89, 172),
      Array(120, 88, 48, 166, 51, 115, 72, 111),
      Array(65, 77, 19, 82, 137, 38, 149, 8),
      Array(102, 4, 31, 164, 144, 59, 173, 78),
      Array(35, 20, 108, 92, 12, 124, 44, 131)
    )

    // Select table and lookup measure
    val table = if (currentPosition < 8) table1 else table2
    val column = currentPosition % 8
    val row = diceSum - 2
    val measureNum = table(row)(column)

    displayActor ! Message(
      s"Chef: Rolling dice [$dice1 + $dice2 = $diceSum] for measure ${currentPosition + 1}/16 → Measure #$measureNum"
    )

    measureNum
  }

  /** Send music to a musician */
  private def sendMusic(musicianId: Int, musicianRef: ActorRef): Unit = {
    val measureNum = rollDice()
    displayActor ! Message(
      s"Chef: Requesting measure #$measureNum from database for Musician $musicianId"
    )

    // Store which musician should receive the measure
    pendingMusicianRef = Some(musicianRef)

    // Request measure from database
    dbActor.foreach(_ ! GetMeasure(measureNum - 1))
  }
}
