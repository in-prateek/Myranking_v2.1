import org.apache.predictionio.data.store.PEventStore
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.predictionio.controller.Params
import org.apache.spark.rdd.RDD

import org.apache.predictionio.controller.PDataSource
import org.apache.predictionio.data.storage.Event

import grizzled.slf4j.Logger

case class DataSourceParams(appName: String) extends Params
class property(val dsp: DataSourceParams){
@transient lazy val logger = Logger[this.type]
def propertyReader(sc: SparkContext) : PropertyData = {

	//RDD if item-property
	val ItemProperty: RDD[(String,Property)] = PEventStore.aggregateProperties(
      appName = dsp.appName,
      entityType = "item"
    )(sc).map { case (entityId, properties) =>
  val property = try {
  	 logger.info(s"PROPERTY_READER_LOGGER_::_genre::${ properties.getOrElse[String]("Genre",s"Unk")} and country :: ${properties.getOrElse[String]("Country",s"Unk")}")
    Property(genre = properties.getOrElse[String]("Genre",s"Unk"),
          country = properties.getOrElse[String]("Country",s"Unk"),
          rating = properties.getOrElse[String]("Rating",s"0")
  )
}catch {
        case e: Exception => {
          logger.error(s"PROPERTY_READER_::_Failed to get properties ${properties} of" +
            s" item ${entityId}. Exception: ${e}.")
          throw e
        }
      }
}
(entityId, item)
}.cache()

}

case class Property(genre: String, country: String, rating: String)

class PropertyData(
val ItemProperty: RDD[(String,Property)]
)
