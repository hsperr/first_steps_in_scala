/**
 * Created by henning.sperr on 7/29/15.
 */

trait Stream[+A] {
  def uncons: Option[(A, Stream[A])]
  def isEmpty: Boolean = false

  def toList[A]: List[A] = {
    List()
  }
}

object Stream {
  def empty[A]: Stream[A] =
    new Stream[A] { def uncons = None }

  def cons[A](hd: => A, tl: => Stream[A]): Stream[A] =
    new Stream[A] {
      lazy val uncons = Some((hd, tl))
    }

  def apply[A](as: A*): Stream[A] =
    if (as.isEmpty) empty
    else cons(as.head, apply(as.tail: _*))

}

object StrictLazy {
  def main(args: Array[String]): Unit ={
    val myList = scala.collection.immutable.List(1, 2, 3, 4)
    println(myList.map(_ + 10)
                  .filter(_ % 2 == 0)
                  .map(_ * 3))
  }

}
