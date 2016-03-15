import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

//import org.apache.spark.SparkContext
//import org.apache.spark.SparkConf


object FriendsAndZip {
		def main(args: Array[String]) {
        // if ( args.length != 2 ) {
        //     println("usage:Friend Recommendation Input_address output_address")
        //     sys.exit(-1)
        // }
        //
		
        val sc = new SparkContext(new SparkConf().setAppName("Simple Application").setMaster("local[4]"))
        val lines=sc.textFile("friends.txt")
        val userA = readLine("Enter UserA : ")
        val userB = readLine("Enter UserB : ")

        val friends = lines.map(line=>line.split("\\t")).filter(l1 =>
(l1.size == 2)).filter(line=>(userB==line(0))).flatMap(line=>line(1).split(","))
        val friends1 = lines.map(line=>line.split("\\t")).filter(l1 =>
(l1.size == 2)).filter(line=>(userA==line(0))).flatMap(line=>line(1).split(","))

        val frd=friends1.intersection(friends).collect()

        println(frd.mkString(" "))


        // Another alternative would be to take the read in the file and then output 

        val userData = sc.textFile("userdata.txt")
        val details1 = userData.map(line=>line.split(",")).filter(line=>frd.contains(line(0))).map(line=>(line(1),line(6))).saveAsTextFile("out")

    }

}

