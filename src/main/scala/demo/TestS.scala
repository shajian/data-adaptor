package demo

object TestS extends App {
  var c = 0
  for (i1 <- 1 to 7) {
    for (i2 <- 1 to 7) {
      for (i3 <- 1 to 7) {
        for (i4 <- 1 to 7) {
          if (i1 + i2 + i3 + i4 == 7) {
            c=c+1
//            println(s"$i1 + $i2 + $i3 + $i4 = 7")
          }
        }
      }
    }
  }
  println(s"solution count: $c")
}
