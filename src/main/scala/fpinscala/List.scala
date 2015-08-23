/**
 * Created by henning.sperr on 7/23/15.
 */
sealed trait List[+A]
case object Nil extends List[Nothing]
case class Cons[+A](head: A, tail: List[A]) extends List[A]

object List {

  def foldLeft[A, B](ds: List[A], z: B)(f: (A, B) => B): B = {
    @scala.annotation.tailrec
    def _foldLeft(ds: List[A], acc:B)(f: (A, B) => B): B =
      ds match {
        case Nil => acc
        case Cons(x, xs) => _foldLeft(xs, f(x, acc))(f)
    }
    _foldLeft(ds, z)(f)
  }

  def foldRight[A, B](ds: List[A], z: B)(f: (A, B) => B): B =
    ds match{
      case Nil => z
      case Cons(x, xs) => f(x, foldRight(xs, z)(f))
  }

  def product3(ds: List[Double]): Double = {
    foldLeft(ds, 1.0)(_ * _)
  }

  def sum3(ints: List[Int]): Int = {
    foldLeft(ints, 0)(_ + _)
  }

  def product2(ds: List[Double]): Double = {
    foldRight(ds, 1.0)(_ * _)
  }

  def sum2(ints: List[Int]): Int = {
    foldRight(ints, 0)(_ + _)
  }

  def sum(ints: List[Int]): Int =
    ints match {
      case Nil => 0
      case Cons(x, xs) => x + sum(xs)
  }

  def product(ds: List[Double]): Double =
    ds match {
     case Nil => 1.0
     case Cons(0.0, _) => 0.0
     case Cons(x, xs) => x * product(xs)
  }

  def tail[A](df: List[A]): List[A] =
    df match{
     case Nil => Nil
     case Cons(x, xs) => xs
  }

  @scala.annotation.tailrec
  def drop[A](df: List[A], n:Int): List[A] =
    df match{
     case Nil => Nil
     case Cons(x, xs) => if(n>0) drop(xs, n-1) else df
  }

  @scala.annotation.tailrec
  def dropWhile[A](df:List[A], f:A => Boolean): List[A] =
    df match{
      case Nil => Nil
      case Cons(x, xs) => if(f(x)) dropWhile(xs, f) else df
  }

  def setHead[A](df:List[A], n:A): List[A] =
    df match{
     case Nil => Cons(n, Nil)
     case Cons(x, xs) => Cons(n, xs)
  }

  def length[A](df: List[A]): Int =
    df match{
      case Nil => 0
      case Cons(x, xs) => 1 + length(xs)
    }

  def length2[A](df: List[A]): Int = {
    foldRight(df, 0)((_, acc) => acc + 1)
  }

  def reverseUsingFold[A](df: List[A]): List[A] = {
    foldLeft(df, Nil: List[A])(Cons(_, _))
  }

  def appendUsingFold[A](df: List[A], x: A): List[A] = {
    foldRight(df, List(x))(Cons(_, _))
  }

  def reverse[A](df:List[A]): List[A] = {
    def _reverse(df: List[A], newList: List[A]): List[A] =
      df match {
        case Nil => newList
        case Cons(x, xs) => _reverse(xs, Cons(x, newList))
      }
    _reverse(df, Nil)
  }

  def init[A](df:List[A]): List[A] = {
    reverse(tail(reverse(df)))
  }

  def apply[A](as: A*): List[A] = {
    if (as.isEmpty) Nil
    else Cons(as.head, apply(as.tail: _*))
  }

  val example = Cons(1, Cons(2, Cons(3, Nil)))
  val example2 = List(1, 2, 3)
  val total = sum(example)

}

object ListExampleRunner{
  def main(args: Array[String]): Unit ={
    val myList = List(1,2,3,4,5)
    println(List.tail(myList))
    println(List.drop(myList, 3))
    println(List.drop(myList, 10))
    println(List.dropWhile(myList, (x:Int) => x<5))
    println(List.setHead(myList, 10))
    println(List.init(myList))
    println(List.foldRight(myList, Nil:List[Int])(Cons(_, _)))
    println(List.length(myList))
    println(List.length2(myList))


    var myLongList  = List(1,2,3,4,6)
    for(i <- 1 to 100000){
      myLongList = Cons(i, myLongList)
    }
    try {
      println(List.foldRight(myLongList, 0)(_ + _))
    } catch{
      case soe: java.lang.StackOverflowError => println("foldRightOverflow")
    }
    println(List.foldLeft(myLongList, 0)(_ + _))

    println(List.reverseUsingFold(myList))
    println(List.appendUsingFold(myList, 1))
  }
}
