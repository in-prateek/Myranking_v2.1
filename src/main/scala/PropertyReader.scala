import org.apache.predictionio.data.store.PEventStore

class property(){
def propertyReader() : PropertyData = {

	//RDD if item-property
	val ItemProperty: RDD[(String,Property)] = PEventStore.aggregateProperties(
      appName = "capp",
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
