import scala.annotation.tailrec

/**
 * Created by henning.sperr on 7/22/15.
 */


object Exercises {

  def main(args: Array[String]): Unit = {
    for (i <- 0 to 10)
      println(fib(i))

    println(isSorted(Array(1.0, 2.0, 3.0, 4.0, 5.0), gt))
    println(isSorted(Array(1.0, 2.0, 3.0), gt))
    println(isSorted(Array(13.0, 2.0, 3.0), gt))
    println(isSorted(Array(13.0, 2.0, 53.0), gt))

    println(partial1(5, (a:Int, b:Int)=>a*b)(10))
  }

  def gt(a:Double, b:Double): Boolean = {
    if(a<b) true
    else false
  }

  @tailrec
  def fib(n: Int, n1: Int=1, n2: Int = 0): Int = {
    if (n <= 1) n1
    else fib(n-1, n1+n2, n1)
  }

  def isSorted[A](data: Array[A], gt:(A, A) => Boolean): Boolean = {
    @scala.annotation.tailrec
    def _isSorted(n:Int): Boolean = {
      if(n == data.length-1) true
      else if(!gt(data(n), data(n+1))) false
      else _isSorted(n+1)
    }
    _isSorted(0)
  }

  def partial1[A, B, C](a: A, f: (A, B) => C): B => C = {
    (b: B) => f(a, b)
  }

}
