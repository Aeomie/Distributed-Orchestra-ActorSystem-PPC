package upmc.akka.leader

import akka.actor._
import scala.concurrent.duration._
import upmc.akka.ppc.{PlayerActor, DataBaseActor}
import upmc.akka.ppc.DataBaseActor._
import scala.util.Random

case object Start
case object Abort
case object StartPerformance
case class PlayNote(pitch: Int, vel: Int, dur: Int)
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
case object ChefHeartbeat
case object RequestMusic
case class FinishedPlaying(musicianId: Int)

// Bully Election Messages
case object Alive
case class AliveResponse(id: Int, creationTime: Long)
case class ChefAnnouncement(id: Int, creationTime: Long, ref: ActorRef)
case object StartElection

class Musicien(val id: Int, val terminaux: List[Terminal]) extends Actor {

  import context.dispatcher

  val displayActor = context.actorOf(Props[DisplayActor], name = "displayActor")
  val playerActor = context.actorOf(Props[PlayerActor], name = "playerActor")

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
  private var heartbeatCancellable: Cancellable = Cancellable.alreadyCancelled
  private var foundChef: Boolean = false
  private var responsesReceived: Int = 0
  private var dbActor: Option[ActorRef] = None
  private var currentPosition = 0 // 0-15 (16 measures)
  private var pendingMusicianRef: Option[ActorRef] =
    None // musician waiting for measure
  private var currentlyPlaying: Option[Int] = None // musician currently playing
  private var requestQueue: List[Int] = List.empty // musicians waiting to play

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
    case Measure(chords) => {
      displayActor ! Message(
        s"Musicien $id: Playing measure with ${chords.length} chords"
      )
      playerActor ! Measure(chords)
    }
    case PlayerActor.MeasureFinished => {
      displayActor ! Message(
        s"Musicien $id: Finished playing, notifying chef"
      )
      currentChefRef.foreach(_ ! FinishedPlaying(id))
    }
    case PlayNote(pitch, vel, dur) => {
      displayActor ! Message(
        s"Musicien $id: playing note (pitch=$pitch, vel=$vel, dur=$dur)"
      )
      playerActor ! PlayerActor.MidiNote(pitch, vel, dur, 0)
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
      val betterThanCurrent =
        currentChefId.isEmpty ||
          chefCreationTime < currentChefCreationTime.get ||
          (chefCreationTime == currentChefCreationTime.get && chefId < currentChefId.get)

      val isSameChef = currentChefId.contains(chefId) &&
        currentChefCreationTime.contains(chefCreationTime)

      if (betterThanCurrent) {
        displayActor ! Message(
          s"Musicien $id: received ChefAnnouncement from $chefId"
        )

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
      } else if (!isSameChef) {
        // Only print if it's a different (worse) chef, not heartbeat from current chef
        displayActor ! Message(
          s"Musicien $id: received ChefAnnouncement from $chefId"
        )
        displayActor ! Message(
          s"Musicien $id: ignoring worse chef from $chefId"
        )
      }
      // If isSameChef and !betterThanCurrent, it's just a heartbeat - stay silent
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
    case Measure(chords) => {
      // Chef received measure from database, send to musician (like Conductor → Player)
      pendingMusicianRef.foreach { musicianRef =>
        displayActor ! Message(
          s"Chef: Received measure ${currentPosition + 1}, sending to musician"
        )
        musicianRef ! Measure(chords)
      }
      pendingMusicianRef = None

      // Update position for next measure (like Conductor does)
      currentPosition = (currentPosition + 1) % 16
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
      displayActor ! Message(
        s"Chef: musician $mid started playing 🎵"
      )

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
    case RequestMusic => {
      val senderRef = sender()
      musicians.find(_._2 == senderRef).foreach { case (mid, ref) =>
        if (currentlyPlaying.isEmpty) {
          // No one playing, send music immediately
          displayActor ! Message(
            s"Chef: Musician $mid requesting music, sending now"
          )
          currentlyPlaying = Some(mid)
          sendMusic(mid, ref)
        } else {
          // Someone is playing, add to queue
          if (!requestQueue.contains(mid)) {
            requestQueue = requestQueue :+ mid
            displayActor ! Message(
              s"Chef: Musician $mid queued (someone is playing)"
            )
          }
        }
      }
    }
    case FinishedPlaying(mid) => {
      displayActor ! Message(s"Chef: Musician $mid finished playing")
      currentlyPlaying = None

      // Pick next musician - either from queue or round-robin through registered musicians
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
      // Make bully election reliable: candidate counts me as alive immediately
      sender() ! AliveResponse(this.id, this.creationTime)
      // Also tell them I'm chef (optional but helpful)
      sender() ! ChefAnnouncement(this.id, this.creationTime, self)
    }

    case ChefHeartbeat => {
      // Periodically announce chef status to all musicians
      for (terminal <- terminaux.filter(_.id != this.id)) {
        val remoteMusician = context.actorSelection(
          s"akka.tcp://MozartSystem${terminal.id}@${terminal.ip}:${terminal.port}/user/Musicien${terminal.id}"
        )
        remoteMusician ! ChefAnnouncement(this.id, this.creationTime, self)
      }
      // Schedule next heartbeat
      heartbeatCancellable =
        context.system.scheduler.scheduleOnce(1.second, self, ChefHeartbeat)
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
        // Cancel heartbeat when stepping down
        if (!heartbeatCancellable.isCancelled) heartbeatCancellable.cancel()
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

    // Create DataBaseActor for music measures
    dbActor = Some(
      context.actorOf(Props[DataBaseActor], name = "DataBaseActor")
    )
    currentPosition = 0

    context.become(chef)
    self ! Start

    // Start heartbeat to periodically announce chef status
    if (!heartbeatCancellable.isCancelled) heartbeatCancellable.cancel()
    heartbeatCancellable =
      context.system.scheduler.scheduleOnce(1.second, self, ChefHeartbeat)
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

  // Helper function: Roll dice and get measure number from Mozart's tables
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

  // Helper function: Send music to a musician
  private def sendMusic(musicianId: Int, musicianRef: ActorRef): Unit = {
    val measureNum = rollDice()
    displayActor ! Message(
      s"Chef: Requesting measure #$measureNum from database for Musician $musicianId"
    )

    // Store which musician should receive the measure (like Conductor stores player ref)
    pendingMusicianRef = Some(musicianRef)

    // Request measure from database
    dbActor.foreach(_ ! GetMeasure(measureNum - 1))
  }

}
