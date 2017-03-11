package com.shas.getcab
import kafka.serializer.StringDecoder
import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.kafka._
import org.apache.kafka.clients.producer.{ ProducerConfig, KafkaProducer, ProducerRecord }
import scala.collection.immutable.HashMap
import scala.collection.JavaConversions.mapAsJavaMap

object RunGetCab {
  case class Location(lat: Float, long: Float)

  //function to update latest cptn state
  def updateLoc(newLoc: Seq[Location], oldLoc: Option[Location]) = {
    var latestLoc = newLoc.lastOption.getOrElse(oldLoc.get)
    Some(latestLoc)
  }

  //function to calculate distance between each customer n cptn 
  def getNearest(custLocRdd: RDD[(String, Location)], cptnLocRdd: RDD[(String, Location)]) = {
    //check if cust Loc not empty
    if (!custLocRdd.isEmpty) {
      //cartestian join updated state and cust loc
      val custCaptRdd = custLocRdd.cartesian(cptnLocRdd)
      //calculate distance
      custCaptRdd.map {
        case ((custId, custLoc), (cptnId, cptnLoc)) => {
          val latDif = custLoc.lat - cptnLoc.lat
          val longDif = custLoc.long - cptnLoc.long
          val dist = math.sqrt(math.pow(latDif, 2) + math.pow(longDif, 2))
          (custId, cptnId, dist)
        }
      }
    } else
      custLocRdd.map(l => ("0", "0", 0.toDouble))
  }

  def parseArgs(args: Array[String]) = {
    val usage = """
				  |Usage: RunGetCab <cptnTopics> <custTopics> <cptnCustTopics> <kafkaBrkrs> <chkPntFile> [<master>]
				  | <cptnTopics> comma-seperated list of topics for Captain locations
				  | <custTopics> comma-seperated list of topics for Customer locations
				  | <cptnCustTopics> comma-seperated list of topics for Captain Customer assignment
				  | <kafkaBrkrs> comma-seperated list of Kafka brokers
				  | <chkPntFile> is full path to checkpoint file
				  | [<master>] is optional SparkMaster, by default local[*]
				  |
				  """.stripMargin
    args.length match {
      case 5 => args :+ "local[*]"
      case 6 => args
      case _ => {
        System.err.println(usage)
        System.exit(1)
        args
      }
    }
  }

  def writeNearestToKafka(distRdd: RDD[(String, String, Double)],kafkaBrkrs: String,cptnCustTopics: String) = {
    distRdd.map { case (custId, cptnId, dist) => (custId, (cptnId, dist)) }
      .reduceByKey((a, b) => if (a._2 < b._2) a else b)
      .foreachPartition(partition => {
        val props = Map[String, Object](ProducerConfig.BOOTSTRAP_SERVERS_CONFIG -> kafkaBrkrs,
                       ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringSerializer",
                       ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringSerializer")
        val producer = new KafkaProducer[String, String](props)
        partition.foreach(record => {
          val data = record.toString
          val message = new ProducerRecord[String,String](cptnCustTopics,null,data)
          producer.send(message)
        })
      })
  }

  //main
  def main(args: Array[String]) {
    val Array(cptnTopics, custTopics, cptnCustTopics, kafkaBrkrs, chkPntFile, master) = parseArgs(args)

    val conf = new SparkConf().setAppName("GetCab").setMaster(master)
    val ssc = new StreamingContext(conf, Seconds(60))
    ssc.checkpoint(chkPntFile)
    val kafkaParams = Map[String, String]("metadata.broker.list" -> kafkaBrkrs)

    //get captain locations from stream
    //val cptnLocStream = ssc.socketTextStream("localhost",3000)
    val cptnTopicSet = cptnTopics.split(',').toSet
    val cptnLocStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, cptnTopicSet)
    val newCptnLoc = cptnLocStream.map(_._2.split(","))
      .map(data => (data(0), Location(data(1).toFloat, data(2).toFloat)))

    //update recent captain location
    val cptnLoc = newCptnLoc.updateStateByKey[Location](updateLoc _)

    //get customer locations from stream
    //val custLocStream = ssc.socketTextStream("localhost",3001)
    val custTopicSet = custTopics.split(',').toSet
    val custLocStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, custTopicSet)
    val custLoc = custLocStream.map(_._2.split(","))
      .map(data => (data(0), Location(data(1).toFloat, data(2).toFloat)))

    //calculate distance between each cust n cptn, get nearest cptn for each cust, write to kafka cptnCustTopics                             
    custLoc.transformWith(cptnLoc, getNearest _)
      .foreachRDD(distRdd => writeNearestToKafka(distRdd,kafkaBrkrs,cptnCustTopics))

    //sort by distance 

    ssc.start()
    ssc.awaitTermination()
  }
}