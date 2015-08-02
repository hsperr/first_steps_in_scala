/**
 * Created by henning.sperr on 7/29/15.
 */
//hide build in types since we write our own
import scala.{Option => _, Some => _, Either => _, None => _}

sealed trait Option[+A] {
  def map[B](f: A => B): Option[B] =
    this match {
      case None => None
      case Some(a) => Some(f(a))
  }

  def flatMap[B](f: A => Option[B]): Option[B] = {
    this.map(f).getOrElse(None)
  }

  def getOrElse[B >: A](default: => B): B =
    this match{
      case None => default
      case Some(a) => a

  }

  def orElse[B >: A](ob: => Option[B]): Option[B] = {
    this.map(Some(_)).getOrElse(ob)
  }

  def filter(f: A => Boolean): Option[A] = {
    this.flatMap(a => if(f(a)) Some(a) else None)
  }

  def lift[A,B](f: A => B): Option[A] => Option[B] = _ map f


}
case class Some[+A](get: A) extends Option[A]
case object None extends Option[Nothing]

object Option {
  def map2[A,B,C](a: Option[A], b: Option[B])(f: (A, B) => C): Option[C] = {
    a.flatMap(aa => b.map(bb => f(aa, bb)))
  }
}
