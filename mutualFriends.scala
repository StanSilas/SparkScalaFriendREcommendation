import org.apache.spark.rdd.RDD
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import scala.io.Source
import scala.collection.immutable.HashSet
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.collection.mutable.ArrayBuffer

object mutualFriends {
  
  def main(args:Array[String]){
    
    var conf = new SparkConf().setAppName("MutualFriendFinder").setMaster("local[4]")
    var sc = new SparkContext(conf)
    
    val input_file = sc.textFile("friends.txt")
    
    var user_one = "35"
    var user_two = "36"
    
    val first_user_friends = input_file.filter(line => (line.split("\t")(0).equals(user_one))).map(line => (line.split("\t")(1)))
    first_user_friends.take(1).foreach(println)
    
    val second_user_friends = input_file.filter(line => (line.split("\t")(0).equals(user_two))).map(line => (line.split("\t")(1)))
    second_user_friends.take(1).foreach(println)
    
    val mutualFriends = MutualFriendFind(first_user_friends,second_user_friends)
    print(mutualFriends)
  }
  
  def MutualFriendFind(rdd1: RDD[String],rdd2: RDD[String]):Set[String]={ 
    val first_one= rdd1.map { friends => friends.split(",") }.collect().apply(0).toSet
    val second_one= rdd2.map { friends => friends.split(",") }.collect().apply(0).toSet
    
    one.intersect(second_one)
  }
  
}