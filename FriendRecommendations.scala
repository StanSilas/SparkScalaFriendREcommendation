import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.collection.immutable.ListMap
import scala.util.control._
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.{SparkContext, SparkConf}

object FriendRecommendations {
	//reading the input
	val INPUT_DIR = "/Users/vivek/Desktop/Spring 2016/Big Data/Assignment 2/DataSet/friend_list.txt"
	val OUTPUT_DIR = "/Users/vivek/Documents/Code/Scala/output/"
	
			//setting friend and not friend counters
			val IS_FRIEND = -1
			val IS_NOT_FRIEND = 1


	//this is the map process
	def mapProcess(args: String): Array[(String, (String, Int))] = {
					var emit = new ArrayBuffer[(String, (String, Int))]()
							val userAndFriend = args.split("\\t")
							// Check if current line is correct 
							if (userAndFriend.length < 2) {
								return emit.toArray            // should not return any null values 
							}

					val id = userAndFriend(0)
							val friendList = userAndFriend(1).split(",")
							// emitting direct friends
							for(i <- 0 until friendList.length) {
								emit += ((id, (friendList(i), IS_FRIEND)))
							}
					// emiting indirect friends
					for(i <- 0 until friendList.length) {
						for(j <- i+1 until friendList.length) {
							emit += ((friendList(i), (friendList(j), IS_NOT_FRIEND)))
							emit += ((friendList(j), (friendList(i), IS_NOT_FRIEND)))
						}
					}
					emit.toArray
	}



	//this is the reduce process

	def reduceProcess(args:(String, Iterable[(String, Int)])): String = {
			val key = args._1
					var friendMap = Map[String, Int]()
					args._2.foreach(friend => {
						val friendId = friend._1
								val relation = friend._2
								if (friendMap.contains(friendId)) {
									if (friendMap(friendId) == IS_FRIEND) {
										// Ignore. He is already direct friend
									}
									else if (relation.equals(IS_FRIEND)) { // id, friendId are direct friends
										friendMap += (friendId -> IS_FRIEND)
									} else { // (id, friendId) have one another mutual friend
										friendMap += (friendId -> (friendMap(friendId) + 1))
									}
								} else {
									if (relation.equals(IS_FRIEND)) {
										friendMap += (friendId -> IS_FRIEND)
									} else {
										friendMap += (friendId -> 1)
									}
								}
					})
					
					// sort the candidates by the count of mutual friends with target user
					val friendMapSorted = ListMap(friendMap.toSeq.sortWith(_._2 > _._2):_*)
					var count = 0
					val recommendedFriends = new StringBuilder()
			    recommendedFriends.append(key+"\t")
					val loop = new Breaks
					loop.breakable {
			      for((friendId, relation) <- friendMapSorted) {
			        if(relation == IS_FRIEND) loop.break()
    			    count += 1
    			    if(count != 1) recommendedFriends.append(", ")
    			    recommendedFriends.append(friendId)
				    }
					}
			recommendedFriends.toString()
	}

	def main(args: Array[String]) {
		val conf = new SparkConf().setAppName("Friend Recommendation Project").setMaster("local")
				conf.set("spark.hadoop.validateOutputSpecs", "false")

				val sc = new SparkContext(conf)
				//reading the file into input
				val input = sc.textFile(INPUT_DIR)
				//saving the results of map
				val mapResult = input.flatMap(line => mapProcess(line))
				//grouping all the results based on the key 
				val groupMapByFriendID = mapResult.groupByKey()
				//generating the final recommendations
				val recommendedFriends = groupMapByFriendID.map(reduceProcess) // thia is the final output 

				//saving output as a text file
				recommendedFriends.saveAsTextFile(OUTPUT_DIR + "test")
	}
}