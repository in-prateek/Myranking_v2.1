def propertyReader(sc: SparkContext, appname: String) : PropertyData = {

	//RDD if item-property
	val ItemProperty: RDD[(String,Property)] = PEventStore.aggregateProperties(
      appName = dsp.appName,
      entityType = "item"
  		)(sc).map { case (entityId, properties) =>
  val property = try {
  	property(genre = properties.getOrElse[String]("Genre",s"Unk"),
          country = properties.getOrElse[String]("Country",s"Unk"),
          rating = properties.getOrElse[String]("Rating",s"0")
  	)
}catch {
        case e: Exception => {
          logger.error(s"Failed to get properties ${properties} of" +
            s" item ${entityId}. Exception: ${e}.")
          throw e
        }
      }
}
(entityId, item)
}.cache()