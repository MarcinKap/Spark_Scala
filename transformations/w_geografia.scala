import org.apache.spark.sql._

// change username
val username = "username"

val scotlandAuthorities = spark.read.
 format("org.apache.spark.csv").
 option("header", true).
 option("inferSchema", true).
 csv(s"/user/$username/proj/spark/authoritiesScotland.csv").cache()

val scotlandRegions = spark.read.
 format("org.apache.spark.csv").
 option("header", true).
 option("inferSchema", true).
 csv(s"/user/$username/proj/spark/regionsScotland.csv").cache()

 scotlandAuthorities.join(scotlandRegions, 
    scotlandAuthorities("region_ons_code") === scotlandRegions("region_ons_code")).
    select(
        scotlandAuthorities("local_authority_ons_code"),
        scotlandAuthorities("local_authority_name"),
        scotlandRegions("region_ons_code"),
        scotlandRegions("region_name")
    ).write.insertInto("w_geografia")

val geografia = spark.sql("SELECT id_jednostki_adm, nazwa FROM w_geografia")

geografia.show()