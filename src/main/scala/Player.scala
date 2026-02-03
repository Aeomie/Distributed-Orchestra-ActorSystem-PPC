package upmc.akka.ppc

import math._

import javax.sound.midi._
import javax.sound.midi.ShortMessage._

import scala.concurrent.duration._
import scala.concurrent.ExecutionContext
import ExecutionContext.Implicits.global

import akka.actor.{Props, Actor, ActorRef, ActorSystem}

object PlayerActor {
  case class MidiNote(pitch: Int, vel: Int, dur: Int, at: Int)
  case object MeasureFinished
  val info =
    MidiSystem.getMidiDeviceInfo().filter(_.getName == "Gervill").headOption
  val device = info.map(MidiSystem.getMidiDevice).getOrElse {
    println("[ERROR] Could not find Gervill synthesizer.")
    sys.exit(1)
  }

  val rcvr = device.getReceiver()

  def note_on(pitch: Int, vel: Int, chan: Int): Unit = {
    val msg = new ShortMessage
    msg.setMessage(NOTE_ON, chan, pitch, vel)
    rcvr.send(msg, -1)
  }

  def note_off(pitch: Int, chan: Int): Unit = {
    val msg = new ShortMessage
    msg.setMessage(NOTE_ON, chan, pitch, 0)
    rcvr.send(msg, -1)
  }

}

class PlayerActor() extends Actor {
  import DataBaseActor._
  import PlayerActor._
  device.open()

  def receive = {
    case Measure(chords) => {
      val originalSender = sender() // Save the sender to reply to
      println("Player: Playing measure with " + chords.length + " chords")

      // Calculate total duration of the measure
      val totalDuration =
        if (chords.isEmpty) 0
        else {
          chords.flatMap { chord =>
            chord.notes.map(note => chord.date + note.dur)
          }.max
        }

      // Schedule all notes
      for (chord <- chords) {
        for (note <- chord.notes) {
          self ! MidiNote(note.pitch, note.vol, note.dur, chord.date)
        }
      }

      // Notify sender when measure finishes
      context.system.scheduler.scheduleOnce(totalDuration.milliseconds) {
        originalSender ! MeasureFinished
      }
    }
    case MidiNote(p, v, d, at) => {
      context.system.scheduler.scheduleOnce((at) milliseconds)(
        note_on(p, v, 10)
      )
      context.system.scheduler.scheduleOnce((at + d) milliseconds)(
        note_off(p, 10)
      )
    }
  }
}
