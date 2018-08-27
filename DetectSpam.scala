import org.apache.spark.ml.fpm.FPGrowth
import org.apache.spark.sql.functions._
import scala.collection.mutable.WrappedArray
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.rdd._
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.SparkSession

object FinalProject {

  def main(args: Array[String]) {
    // You can find all functions used to process the stream in the
    // Utils.scala source file, whose contents we import here
    //import Utils._

    /** ******
      * Let's check to make sure user has entered correct parameters
      *
      * *******/
    if (args.length < 2) {
      System.out.println("Usage: InputFilePath OutputFilePath")
      return
    }


    val sc = new SparkContext(new SparkConf().setAppName("Part3"))
    val spark = SparkSession.builder.appName("Simple Application").getOrCreate()
    val sqlContext = spark.sqlContext
    import spark.implicits._
    val reviews = spark.read.option("header", "true").csv(args(0))

    val reviews_cols = reviews.select("ProductId", "UserId", "Score")

    val reviewDF = reviews_cols.withColumn("Score1", reviews_cols("Score").cast(IntegerType)).drop("Score").withColumnRenamed("Score1","Score")

    val reviewerCount = reviewDF.groupBy("UserId").count().select((('count divide 2).cast("int")*2) as "Reviewer_Range")

    val reviewerCount1 = reviewerCount.groupBy("Reviewer_Range").count().orderBy("Reviewer_Range")

    val userTotal = reviewDF.select("UserId").distinct.count()

    val AvgStd_DF = reviewDF.groupBy($"ProductId").agg(mean(reviewDF("Score")) as "Avg_Score", stddev_pop(reviewDF("Score")) as "Std_Dev")

    val df1 = reviewDF.as("abc")
    val df2 = AvgStd_DF.as("def")

    val joinedDF = df1.join(df2, col("abc.ProductId") === col("def.ProductId"))

    val reviewGraph = joinedDF.groupBy("Score").count().orderBy("Score")

    val scaledScore = AvgStd_DF.select($"ProductId",(('Avg_Score divide 0.5).cast("int")*0.5) as "Avg_Range")

    val productGraph = scaledScore.groupBy("Avg_Range").count().orderBy("Avg_Range")

    val resultDF = joinedDF.filter(joinedDF.col("Score")<joinedDF.col("Avg_Score")-joinedDF.col("Std_Dev"))

    val cntneg = resultDF.groupBy("UserId").count().select($"UserId",$"count".alias("negativeCount"))

    val cnttot = reviewDF.groupBy("UserId").count().filter($"count">=10).select($"UserId",$"count".alias("totalCount"))

    val d1 = cntneg.as("bc")
    val d2 = cnttot.as("ef")

    val joinedDF2 = d1.join(d2, col("bc.UserId") === col("ef.UserId"))

    val negUsers = joinedDF2.filter($"bc.negativeCount"/$"ef.totalCount" > 0.6).select($"bc.userId",$"bc.negativeCount",($"ef.totalCount"-$"bc.negativeCount") as "positiveCount")

    val negUserTotal = negUsers.count()

    /*val tempDF = Seq(userTotal, negUserTotal)
    val compareDF = tempDF.toDF()
    display(compareDF)*/

    val negUsersDesc = negUsers.sort(desc("negativeCount")).collect()


    var out="The Negative review giving Users are: \t" + "\n" +"UserId," + "\t" + "No. of Negative Reviews," + "\t" + "No. of Positive Reviews" + "\n"
    val out1=sc.parallelize(List(out))

    val opassoc= sc.parallelize(negUsersDesc)
    val src = opassoc.map(_.mkString(","))

    var output1 = out1 ++ src

    output1.coalesce(1,true).saveAsTextFile(args(1))
  }
}
