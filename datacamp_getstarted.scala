
//https://learn.datacamp.com/courses/introduction-to-scala 
def DataCamp() // BlackJack
{
  // Unit 1: a scalable language
  val difference = 8-5
  // Print the difference
  println(difference ) 

  // val = immutable; var = muttble 
  // Define immutable variables for clubs 2♣ through 4♣
  val twoClubs:   Int = 2
  val threeClubs: Int = 3
  val fourClubs:  Int = 4

  // Define immutable variables for player names
  val playerA: String = "Alex" ; 
  val playerB: String = "Chen" ;
  val playerC: String = "Umberto"; //"Marta";

  // Change playerC from Marta to Umberto
  // playerC = "Umberto" -- val is imutable
  var aceClubs 	: Int = 1  
  var aceDiamonds : Int = 1 
  var aceHearts 	: Int = 1 
  var aceSpades 	: Int = 1 

  // Create a mutable variable for Alex as player A
  var playerA: String = "Alex" ; 
  // Change the point value of A♦ from 1 to 11
  var aceDiamonds : Int = 11
  var jackClubs 	: Int = 10
  // Calculate hand value for J♣ and A♦
  println(aceDiamonds + jackClubs)
}

// https://www.analyticsvidhya.com/blog/2017/01/scala/ 
def ScalaBasis(){
  // 10 
  var Var3 =1 
  if (Var3 ==1){
  println("True")}else{
  println("False")}      // Output: True

  // 11
  for( a <- 1 to 10){
  println( "Value of a: " + a );
  }
  //12 
  def mul2(m: Int): Int = m * 10  // Output: mul2: (m: Int)Int
  mul2(2) //Output: res9: Int = 20

  //13
  var name = Array("Faizan","Swati","Kavya", "Deepak", "Deepak")
  var name:Array[String] = new Array[String](3)
  var name = new Array[String](3)
  // output : name: Array[String] = Array(null, null, null)
  name(0) = "jal"; name(1) = "Faizy"; name(2) = "Expert in deep learning"
  val numbers = List(1, 2, 3, 4, 5, 1, 2, 3, 4, 5)
  number1 = List( List(1, 0, 0), List(0, 1, 0), List(0, 0, 1) )
  numbers(2) // res: Int 3 


  //14
  // . Scala programmer can’t use static methods because they use singleton objects. To read more about singleton object
  //HelloWorld.scala => scalac HelloWorld.scala => scala HelloWorld
  object HelloWorld {
  def main(args: Array[String]) {
    println("Hello, world!")
    }
  }

  // main reasons to learn scala: 
  // Working with Scala is more productive than working with Java
  // Scala is faster than Python and R because it is compiled language
  // Scala is a functional language

}


// Scala - Spark 
def ScalaSparkBasis(){
// 18 creating RDD
val data = Array(1, 2, 3, 4, 5,6,7,8,9,10)
val distData = sc.parallelize(data)



}

