
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.collection.immutable.ListMap
import scala.util.control._
import scala.collection.mutable.ArrayBuffer

//question 4 

object TopUsersAvgAgeAdd {

 
  val INPUT_DIR = "/Users/vivek/Desktop/Spring 2016/Big Data/Assignment 2/DataSet/friend_list.txt"
  val USER_INPUT_DIR = "/Users/vivek/Documents/Code/Java/DataSet/userdata.txt"
  val OUTPUT_DIR = "/Users/vivek/Documents/Code/Scala/output/"
	
  
  def mapper1_1(friendsList : String) : Array[(String, String)] = {  
    val f = friendsList.split("\t")
   //taking in the first value
    val id = f(0)
    // emit (friendID, id)
    var emit = new ArrayBuffer[(String, String)]()
    if(f.length != 2) {
      return emit.toArray
    }
    val friendIDs = f(1).split(",")
  
    for(i<- 0 until friendIDs.length) {
      emit += ((friendIDs(i), id))
    }
    return emit.toArray
  }
  
 
  def mapper1_2(friendsDetails : String) : (String, String) = {
    val f = friendsDetails.split(",")
    val age = f(9).split("/")
   if(age.length != 3)
      println(f(9))
//    else
      (f(0), "age:"+(2016 - age(2).toInt).toString())
    
  }
  
 
  def mapper_2(id : String, ages : Iterable[String]) : (String, String) = {
    var sum = 0
    var count = 0
    ages.foreach { age => {
      sum += age.toInt
      count += 1
    } } 
    (id, (sum/count).toString())
  }
  
 
  def main(args: Array[String]) {
    //reference http://spark.apache.org/docs/latest/quick-start.html
    val conf = new SparkConf().setAppName("Friend Recommendation Project").setMaster("local")
		conf.set("spark.hadoop.validateOutputSpecs", "false")
		//reference http://spark.apache.org/docs/latest/quick-start.html
    val sc = new SparkContext(conf)
		val input = sc.textFile(INPUT_DIR)
		val userDetails = sc.textFile(USER_INPUT_DIR)
    // flat map 
    
    val friendID = input.flatMap { line => mapper1_1(line) }
    val friendIDAge = userDetails.map { line => mapper1_2(line) }
    val f1 = friendID.join(friendIDAge)
    val f2 = f1.map(f => {
      if(f._2._1.contains("age"))
        (f._2._2, f._2._1.split(":")(1))
       else
         (f._2._1, f._2._2.split(":")(1))
    })

    val f3 = f2.groupByKey()
    val f4 = f3.map(f => mapper_2(f._1, f._2))
    val f5 = f4.sortBy(f=>f._2, false)
    val top20 = f5.take(20)
    val top20IDs = top20.map(f => f._1)
    val address = userDetails.map {line => line.split(",")}.filter { x => top20IDs.contains(x(0)) }.map { x => (x(0), x(3), x(4), x(5))}
    for( v <- address) {
      for( t <- top20) {
        if( v._1.equals(t._1)){
          println(v._1 + ", " + v._2 +", " + v._3 +", " + v._4 +", "+t._2)
        }
      }
    }
  }
}