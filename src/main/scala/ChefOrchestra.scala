import akka.actor._
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

case class Start()
case class Register(id: Int, ref: ActorRef)
case class Join(id: Int)
case object WindowExpired
case object StartPerformance
case object Abort

class Orchestra(n: Int, val terminaux: List[Terminal]) extends Actor {
  private var joined = Set.empty[Int]
  private var musicians = Map.empty[Int, ActorRef]
  private var windowStarted = false
  val displayActor = context.actorOf(Props[DisplayActor], name = "displayActor")
  var cancellable: akka.actor.Cancellable = _
  var waitingForMusicians = false

  def receive: Receive = {

    case Start =>
      displayActor ! Message(
        s"Orchestra Chef is created, waiting for $n musicians to join"
      )
      if (!windowStarted) {
        windowStarted = true
        cancellable =
          context.system.scheduler.scheduleOnce(30.seconds, self, WindowExpired)
        displayActor ! Message("Orchestra: 30s join window started")
      }

    case Register(id, ref) =>
      musicians += (id -> ref)

    case Join(id) =>
      joined += id
      displayActor ! Message(
        s"Orchestra: musician $id joined (${joined.size}/$n)"
      )
      if (joined.size == (n - 1)) {
        displayActor ! Message("Orchestra: all musicians have joined")
        cancellable.cancel()
        musicians.values.foreach(_ ! StartPerformance)
      }
    case Left(id) =>
      joined -= id
      displayActor ! Message(
        s"Orchestra: musician $id left (${joined.size}/$n)"
      )
      if (joined.size == 0) {
        displayActor ! Message(
          "Orchestra: all musicians have left, stopping chef"
        )
        waitingForMusicians = true;
        cancellable =
          context.system.scheduler.scheduleOnce(30.seconds, self, WindowExpired)
      }

    case WindowExpired =>
      if (joined.size > 0 && waitingForMusicians) {
        displayActor ! Message(
          "Orchestra: musicians still present, continuing performance"
        )
        waitingForMusicians = false
        cancellable.cancel()
        musicians.values.foreach(_ ! StartPerformance)
        
      } else if (joined.size >= n - 1) {
        displayActor ! Message(
          "Orchestra: enough musicians, starting performance"
        )
        cancellable.cancel()
        musicians.values.foreach(_ ! StartPerformance)
      } else {
        displayActor ! Message(
          "Orchestra: not enough musicians, aborting (chef leaves)"
        )
        musicians.values.foreach(_ ! Abort)
        context.stop(self)
      }
  }
}
