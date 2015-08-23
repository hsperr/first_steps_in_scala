
def loop : Boolean = loop

def and(x: Boolean, y: => Boolean): Boolean =
  if(x) y else false

and(false, loop)

def abs(x: Double) = if(x>=0) x else -x

def sqrt(x: Double): Double = {
  def sqrtIter(guess: Double):Double =
    if(isGoodEnough(guess)) guess
    else sqrtIter(improve(guess))

  def isGoodEnough(guess: Double) =
    abs(guess * guess - x) / x < 0.001

  def improve(guess: Double) =
    (guess + x/guess) / 2

  sqrtIter(1.0)
}

sqrt(2)
sqrt(4e50)
sqrt(4e-20)
