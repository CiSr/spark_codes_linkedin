


//Replacing the age feature with mean + 3*sigma value. 
def na_fill(input_array: RDD[Double]):Double={
// Finding the Mean 
val mean_value  = input_array.reduce(_+_)/input_array.count
//Finding the variance value
val variance_array = input_array.map(x => (x - mean_value)*(x-mean_value))
val variance_value = variance_array.fold(0)((x,y) => x+y) / variance_array.count 
//Get the standard deivation 
val std_value = Math.sqrt(variance_value)
return mean_value + 3*std_value
} 
//Convert the dataframe column to rdd and pass it to the function 
val age_feature  = df.select("Age").rdd.map(r => r(0).asInstanceOf[Double])
val map = Map("Age" -> na_fill(age_feature))
val df1 = df.na.fill(map)

//Imputing a categorical feature. 



val calculateWOE= udf((Survived: Double, Died: Double ) => {
        Math.log(Survived/Died)
        })


val grouped_sex = df1.groupBy("Sex")
					.agg(mean("Survived").alias("Survived"))
                    .withColumn("Died",  lit(1) - $"Survived")
                    .withColumn("Sex_WoE",calculateWOE(col("Survived"), col("Died")) )
                    .select("Sex_WoE")
					.withColumnRenamed("Sex", "Gender")					 


