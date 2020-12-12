import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.Row
import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType, DoubleType, FloatType}
import org.apache.log4j.Logger
import org.apache.spark.sql.functions
import org.apache.log4j.Level


object App {
  	
  	def main(args: Array[String]):Unit={
	    
	    println("Hello, world!")

	     val spark = SparkSession.builder().appName("job-1").master("local[*]").getOrCreate()

	     val communes_df = spark.read.option("header",true).option("delimiter", ";").csv("/home/amrta/Documents/spark-projects/spark_test/src/main/scala/esgi_tp/Communes.csv").persist().cache()
	     //communes_df.show()
	     //communes_df.printSchema()

	     val cp_df = spark.read.option("header",true).option("delimiter", ";").csv("/home/amrta/Documents/spark-projects/spark_test/src/main/scala/esgi_tp/code-insee-postaux-geoflar.csv").persist().cache()
	     //cp_df.show()
	     //cp_df.printSchema()

	     val poste_synop_df = spark.read.option("header",true).option("delimiter", ";").csv("/home/amrta/Documents/spark-projects/spark_test/src/main/scala/esgi_tp/postesSynop.txt").persist().cache()
	     //poste_synop_df.show()
	     //poste_synop_df.printSchema()


	     val synop_df = spark.read.option("header",true).option("delimiter", ";").csv("/home/amrta/Documents/spark-projects/spark_test/src/main/scala/esgi_tp/synop.2020120512.txt").persist().cache()
	     //synop_df.show()
	     //synop_df.printSchema()

	     val df4 = synop_df.select(synop_df("date"),synop_df("t").cast(DoubleType).alias("temperature"),synop_df("numer_sta").alias("id_sta")).toDF()
	     df4.show()

	     val df1 = communes_df.select(communes_df("DEPCOM").alias("CODE INSEE"),communes_df("PTOT")).toDF()
	     //df1.show()

	     val df2 = cp_df.select(cp_df("CODE INSEE"),cp_df("Code Dept"),cp_df("geom_x_y")).toDF()
	     //df2.show()

	     val df3 = poste_synop_df.select(poste_synop_df("ID").alias("id_sta"),poste_synop_df("Latitude"),poste_synop_df("Longitude")).toDF()
	     //df3.show()

	    

	     // Traitement population par département

	     val join_pop_dep = df1.join(df2,df1("CODE INSEE")===df2("CODE INSEE"),"left").drop(df2("CODE INSEE"))toDF()
	     //join_pop_dep.show()

	     val agg_pop_dept = join_pop_dep.groupBy("Code Dept").agg(sum(join_pop_dep("PTOT")).cast(IntegerType).alias("population")).orderBy($"Code Dept".asc)
	     //agg_pop_dept.show()

		 val agg_pop_dept_column_rename = agg_pop_dept.withColumnRenamed("Code Dept", "departement")
		 //agg_pop_dept_column_rename.show()

		
		 // Traitement fichiers météo

		 var join_meteo = df4.join(df3,df4("id_sta")===df3("id_sta"),"left").drop(df3("id_sta"))toDF()
	     join_meteo.show()

	     val udfTemp = udf((row: Double) => (row - 273.15).toInt )

	     val x = join_meteo.select(udfTemp(join_meteo("temperature"))as "temperature").toDF().show()


	     

	     //val join_meteo2 = join_meteo.join(x,join_meteo("temperature")===,"left").drop(join_meteo("temperature")).toDF()
	     // val join_meteo2 = join_meteo.withColumn("temperature",udfTemp(join_meteo("temperature")) as "temperature2")).toDF()

	     //join_meteo2.show()













	     






	     //departement_modifie.map(line => line.toString().substring(0,2))
	     //departement_modifie.foreach(println)

	     //sqlDF.foreach { row =>   row.foreach { col => println(col) }


	     /******Les actions à effectuer******/

	     /**

		Votre but est de fournir chaque jour un rapport au format parquet combinant les données des températures fourni par Météo-France avec les données de population des départements. 

		Le résultat peut être constitué d'un ou plusieurs fichiers parquet.
		Le rapport doit contenir l'intégralité des départements français et des stations Météo-France.
		Si deux stations sont dans le même département, les infos de population doivent être dupliquées.
		Si un département ne contient pas de station météorologique, des valeurs Null doivent apparaître.
		Si une station n'est pas en France, elle doit être ignorée.
		On rattachera les stations météo à la commune la plus proche géographiquement en utilisant 
		les latitude et longitude pour déterminer son département







	     **/


	     //val ktoc_temp = udf((s: Double) => (s - 273.15).toInt )


	     //csv_df_v2.write.parquet("csv_df_v2_parquet_file.parquet")




	 }
}

