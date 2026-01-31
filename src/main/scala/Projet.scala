package upmc.akka.leader

import com.typesafe.config.ConfigFactory
import akka.actor._

case class Terminal(id: Int, ip: String, port: Int)

object Projet {

  def main(args: Array[String]) {
    var first = true;
    // Gestion des erreurs
    if (args.size != 1) {
      println("Erreur de syntaxe : run <num>")
      sys.exit(1)
    }

    val id: Int = args(0).toInt
     val n: Int = 3
    if (id < 0 || id > n) {
      println(s"Erreur : <num> doit etre compris entre 0 et $n")
      sys.exit(1)
    }

    var musicienlist = List[Terminal]()

    // recuperation des adresses de tous les musiciens
    // hardcoded path name
    for (i <- 3 to 0 by -1) {
      val address = ConfigFactory
        .load()
        .getConfig("system" + i)
        .getValue("akka.remote.netty.tcp.hostname")
        .render()
      val port = ConfigFactory
        .load()
        .getConfig("system" + i)
        .getValue("akka.remote.netty.tcp.port")
        .render()
      musicienlist = Terminal(i, address, port.toInt) :: musicienlist
    }

    println(musicienlist)

    Initialisation du node <id>
    if (first) {
     val system = ActorSystem("MozartSystem" + id, ConfigFactory.load().getConfig("system" + id))
     val orchestra_chef = system.actorOf(Props(new ChefOrchestra(id, musicienlist)), "Musicien"+id);
     orchestra_chef ! Start
      first = false
    }else{
     val system = ActorSystem("MozartSystem" + id, ConfigFactory.load().getConfig("system" + id))
     val musicien = system.actorOf(Props(new Musicien(id, musicienlist)), "Musicien"+id)

     musicien ! Start
    }
  }

}
