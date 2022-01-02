//spark 2.x can't read multodelimiter file like spark 3.0 does
    val df3 = spark.read.option("delimiter","$$").option("header","true").option("inferSchema","true").csv("data/multidelimiter_2.txt")
    df3.show()
    //How to process multi delimiter file in spark 2.x
    //spark 2.x can't read multi delimiter file --> let's ee in spark 2.0
    val input_df = spark.read.option("delimiter","$$").csv("data/multidelimiter_2.txt")
    Java.lang.IllegalArgumentException: Delimiter cannot be more than one character : $$
    //First Approach: creating case class and creating rdd on top of the file and split the data with 2 $$ and apply the case class on top of the schema and create a dataframe
    //Define a case class or use struct type 
    case class TechnologySchema(Technolgy:String,LinkedIn:Int,Indeed:Int,no_of_job_openings:Int)
    //reading it as RDD
    val tech_rdd = spark.sparkContext.textFile("data/multidelimiter_2.txt")
    tech_rdd.collect
    //split the data 
    val tech_df = tech_rdd.filter(x=>!x.toString.contains("Tech"))..map(x=>x.mkString.split("\\$\\$")).map(x=>TechnologySchema(x(0),x(1).toInt,x(2).toInt,x(3).toInt)).toDf
    
    

    
    
    
    
