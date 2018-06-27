package org.template.productranking

import org.apache.predictionio.controller.P2LAlgorithm
import org.apache.predictionio.controller.Params
import org.apache.predictionio.data.storage.BiMap

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.{Rating => MLlibRating}

import org.apache.predictionio.data.store.PEventStore
import org.apache.predictionio.data.storage.Event
import org.apache.predictionio.controller.PDataSource
import org.apache.predictionio.data.store.LEventStore

import grizzled.slf4j.Logger

import scala.collection.parallel.immutable.ParVector

case class ALSAlgorithmParams(
  rank: Int,
  numIterations: Int,
  lambda: Double,
  seed: Option[Long],
  appName :String,
  property :Array[String]
) extends Params

class ALSModel(
  val rank: Int,
  val userFeatures: Map[Int, Array[Double]],
  val productFeatures: Map[Int, Array[Double]],
  val userStringIntMap: BiMap[String, Int],
  val itemStringIntMap: BiMap[String, Int]
) extends Serializable {

  @transient lazy val itemIntStringMap = itemStringIntMap.inverse

  override def toString = {
    s" rank: ${rank}" +
    s" userFeatures: [${userFeatures.size}]" +
    s"(${userFeatures.take(2).toList}...)" +
    s" productFeatures: [${productFeatures.size}]" +
    s"(${productFeatures.take(2).toList}...)" +
    s" userStringIntMap: [${userStringIntMap.size}]" +
    s"(${userStringIntMap.take(2).toString}...)]" +
    s" itemStringIntMap: [${itemStringIntMap.size}]" +
    s"(${itemStringIntMap.take(2).toString}...)]"
  }
}

class ALSAlgorithm(val ap: ALSAlgorithmParams)
  extends P2LAlgorithm[PreparedData, ALSModel, Query, PredictedResult] {

  @transient lazy val logger = Logger[this.type]

  def train(sc: SparkContext, data: PreparedData): ALSModel = {
    require(!data.viewEvents.take(1).isEmpty,
      s"viewEvents in PreparedData cannot be empty." +
      " Please check if DataSource generates TrainingData" +
      " and Preprator generates PreparedData correctly(this is a view events error check).")
    require(!data.users.take(1).isEmpty,
      s"users in PreparedData cannot be empty." +
      " Please check if DataSource generates TrainingData" +
      " and Preprator generates PreparedData correctly.(this is user event error")
    require(!data.items.take(1).isEmpty,
      s"items in PreparedData cannot be empty." +
      " Please check if DataSource generates TrainingData" +
      " and Preprator generates PreparedData correctly.(this is item event error")
    // create User and item's String ID to integer index BiMap
    val userStringIntMap = BiMap.stringInt(data.users.keys)
    val itemStringIntMap = BiMap.stringInt(data.items.keys)

    val mllibRatings = data.viewEvents
      .map { r =>
      
        // Convert user and item String IDs to Int index for MLlib
        val a = r.v
        logger.info(s"r.viewEvent valuemm ${r.v}")
        val uindex = userStringIntMap.getOrElse(r.user, -1)
        val iindex = itemStringIntMap.getOrElse(r.item, -1)

        if (uindex == -1)
          logger.info(s"Couldn't convert nonexistent user ID ${r.user}"
            + " to Int index.")
        

        if (iindex == -1)
          logger.info(s"Couldn't convert nonexistent item ID ${r.item}"
            + " to Int index.")

        ((uindex, iindex), a)
      }.filter { case ((u, i), v) =>
        // keep events with valid user and item index
        (u != -1) && (i != -1)
      }.reduceByKey(_ + _) // aggregate all view events of same user-item pair
      .map { case ((u, i), v) =>
        // MLlibRating requires integer index for user and item
        MLlibRating(u, i, v)
      }

    // MLLib ALS cannot handle empty training data.
    require(!mllibRatings.take(1).isEmpty,
      s"mllibRatings cannot be empty." +
      " Please check if your events contain valid user and item ID.")

    // seed for MLlib ALS
    val seed = ap.seed.getOrElse(System.nanoTime)

    val m = ALS.trainImplicit(
      ratings = mllibRatings,
      rank = ap.rank,
      iterations = ap.numIterations,
      lambda = ap.lambda,
      blocks = -1,
      alpha = 1.0,
      seed = seed)

    new ALSModel(
      rank = m.rank,
      userFeatures = m.userFeatures.collectAsMap.toMap,
      productFeatures = m.productFeatures.collectAsMap.toMap,
      userStringIntMap = userStringIntMap,
      itemStringIntMap = itemStringIntMap
    ) 
  }

  def predict(model: ALSModel, query: Query): PredictedResult = {

    val itemStringIntMap = model.itemStringIntMap
    val productFeatures = model.productFeatures
    var scoreOpt_new : Double = 0 // new phase var
    var pr = propertyReader(query)
    // default itemScores array if items are not ranked at all

    lazy val notRankedItemScores = query.items.map(i => ItemScore(i,0)).toArray

    model.userStringIntMap.get(query.user).map { userIndex =>
      // lookup userFeature for the user
      model.userFeatures.get(userIndex)
    }.flatten // flatten Option[Option[Array[Double]]] to Option[Array[Double]]
    .map { userFeature =>
      val scores: Vector[Option[Double]] = query.items.toVector
        .par // convert to parallel collection for parallel lookup
        .map { iid =>
          // convert query item id to index
          val featureOpt: Option[Array[Double]] = itemStringIntMap.get(iid)
          // productFeatures may not contain the item
            .map (index => productFeatures.get(index))
            // flatten Option[Option[Array[Double]]] to Option[Array[Double]]
            .flatten
          featureOpt.map(f => dotProduct(f, userFeature))
        }.seq // convert back to sequential collection
        println(s"scores at 158 are :: ${scores}")
        println(s"Item-Score -Int- Map is : ${itemStringIntMap}")
      // check if all scores is None (get rid of all None and see if empty)
      val isAllNone = scores.flatten.isEmpty
      if (isAllNone) {
        logger.info(s"No productFeature for all items ${query.items}.")
        PredictedResult(
          itemScores = notRankedItemScores,
          isOriginal = true
        )
      } else {
        // sort the score
        val ord = Ordering.by[ItemScore, Double](_.score).reverse
        logger.info(s"167:ALS::query.items.zip(scores) ${ query.items.zip(scores)}.")
        val sorted : Array[ItemScore]= query.items.zip(scores).map{ case (iid, scoreOpt) =>
          if(pr.exists(_._1 == iid)){
            println(s"Code is running for iid : ${iid} and score is ${scoreOpt}")
            println(s"score from property reader to be added is : ${pr(iid)}")
            println(s"scoreOpt.get gives :: ${scoreOpt.getOrElse[Double](0)}")
            scoreOpt_new = (scoreOpt.getOrElse[Double](0)) + pr(iid)
            pr -= iid
            pr += (iid-> scoreOpt_new)
            println(s"updated value of scoreOpt is ${scoreOpt_new}")
            println(s"updated map is ${pr}")
          }
          else{
            println(s"Code is running for iid : ${iid} and score is ${scoreOpt}")
            println(s"item does not exits in new map so scores of old map will be included")
            pr += (iid-> scoreOpt.getOrElse[Double](0)) 
            println(s"updated value of scoreOpt is ${scoreOpt_new}")
            println(s"updated map is ${pr}")
          }
              
          ItemScore(
            item = iid,
            score = pr(iid)
            // replace score by following to obtain original score w/o bias
            //score = scoreOpt.getOrElse[Double](0)
          )
        }.sorted(ord).toArray

        println(s"Value of sorted is ${sorted.head}" )

        PredictedResult(
          itemScores = sorted,
          isOriginal = false  
        )
      }
    }.getOrElse {
      logger.info(s"No userFeature found for user ${query.user}.")
      PredictedResult(
        itemScores = notRankedItemScores,
        isOriginal = true
      )
    }
  }

  private
  def dotProduct(v1: Array[Double], v2: Array[Double]): Double = {
    val size = v1.size
    var i = 0
    var d: Double = 0
    while (i < size) {
      d += v1(i) * v2(i)
      i += 1 
    }
    d
  }

  def propertyReader(query: Query) : collection.mutable.Map[String, Double] = {
    //RDD if item-property
    var d: Double = 0
    val appName = ap.appName
    var iprop :  Iterator[Event] = null
    var map = collection.mutable.Map[String, Double]()
    var count : Double  = 0
    //https://github.com/actionml/universal-recommender/blob/c6d8175eaead615598f751e878e91daad4b66150/src/main/scala/URAlgorithm.scala#L798
   
    val uprop = LEventStore.findByEntity(
      appName=appName,
      entityType="user",
      entityId = query.user ,
      eventNames = Some(List("$set"))
      )
    
    for (uevent <- uprop){
    for(q <- query.items){ 
      logger.info(s"q at start of for loop is ${q}")
      iprop = LEventStore.findByEntity(
      appName=appName,
      entityType="item",
      entityId = q,
      eventNames = Some(List("$set"))
      )
            for (ievent <- iprop){
              count = 0
              for ( x <- ap.property )
              {
             // println(s"Checking for ITEM :${q} :: USER:${query.user} :: property:${x}")
              if (ievent.properties.fields.exists(_._1 == x))
                {  
                    if(ievent.properties.fields(x)==uevent.properties.fields(x))
                    {
                      //println(s"for item ${q} :: user ${query.user} :: property:${x}  EQUAL FOUND")
                      count += 1
                      map += (q -> 0.2* count)
                    }
                    else{
                     // println(s"for item ${q} :: user ${query.user} :: property:${x} NOT EQUAL FOUND")
                      if (map.exists(_._1 == q)==0)
                      map += (q -> 0)
                    }
                }
              else{
                  println(s"Property ${x} does not exists in item ${q}")
              }}
            }
            printf(s"map to be returned is : ${map}")
             
      }

    }
  map
  }

}