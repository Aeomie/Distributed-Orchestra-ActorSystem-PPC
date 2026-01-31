package upmc.akka.leader

import akka.actor._

case class Start ()

class Musicien (val id:Int, val terminaux:List[Terminal]) extends Actor {

     // Les differents acteurs du systeme
     val displayActor = context.actorOf(Props[DisplayActor], name = "displayActor")

     def receive = {

          // Initialisation
          case Start => {
               displayActor ! Message ("Musicien " + this.id + " is created")
               
          }

     }
}
