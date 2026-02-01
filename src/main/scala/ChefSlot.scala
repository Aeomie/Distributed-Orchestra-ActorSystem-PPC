package upmc.akka.leader

import akka.actor._

case class SetChef(ref: ActorRef)
case object GetChef

class ChefSlot extends Actor {
  private var chef: Option[ActorRef] = None

  def receive: Receive = {
    case SetChef(ref) =>
      chef.foreach(context.unwatch)
      chef = Some(ref)
      context.watch(ref)

    case GetChef =>
      chef.foreach(c => sender() ! ChefIs(c)) // c reference the ActorRef chef

    case Terminated(ref) if chef.contains(ref) =>
      context.stop(self)
  }
}
