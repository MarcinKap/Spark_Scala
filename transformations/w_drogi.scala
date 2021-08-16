import org.apache.spark.sql._
import org.apache.spark.sql.functions.{row_number}
import org.apache.spark.sql.expressions.Window
import spark.implicits._

case class Road(id: BigInt, road_category: String, road_type: String)

// change username
val username = "bochra_piotr"

def readCsv(path: String) = {
  spark.read.
    format("org.apache.spark.csv").
    option("header", true).
    option("inferSchema", true).
    csv(path).cache()
}

spark.sql("DROP TABLE IF EXISTS w_drogi")
spark.sql(
  """CREATE TABLE IF NOT EXISTS `w_drogi` (
    `id` int,
    `road_category` string,
    `road_type` string)
      ROW FORMAT SERDE
    'org.apache.hadoop.hive.ql.io.orc.OrcSerde'
      STORED AS INPUTFORMAT
    'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'
      OUTPUTFORMAT
    'org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat'"""
)

val scotlandRoadsPath = s"/user/$username/proj/spark/mainDataScotland.csv"
val northEnglandRoadsPath = s"/user/$username/proj/spark/mainDataNorthEngland.csv"
val southEnglandRoadsPath = s"/user/$username/proj/spark/mainDataSouthEngland.csv"

val scotlandRoads = readCsv(scotlandRoadsPath)
val northEnglandRoads = readCsv(northEnglandRoadsPath)
val southEnglandRoads = readCsv(southEnglandRoadsPath)

val windowSortByRoadCategory = Window.orderBy("road_category")

scotlandRoads
  .select(
    "road_category",
    "road_type"
  ).union(northEnglandRoads
  .select(
    "road_category",
    "road_type"
  )).union(southEnglandRoads
  .select(
    "road_category",
    "road_type"
  )).dropDuplicates().withColumn("id", row_number().over(windowSortByRoadCategory))
  .select(
    "id",
    "road_category",
    "road_type"
  ).toDF().as[Road].write.insertInto("w_drogi")

val drogi = spark.sql("SELECT id, road_category, road_type FROM w_drogi")

drogi.show()