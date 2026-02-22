// Copyright 2025 Tampere University
// This notebook and software was developed for a Tampere University course COMP.CS.320.
// This source code is licensed under the MIT license. See LICENSE in the exercise repository root directory.
// Author(s): Ville Heikkilä (ville.heikkila@tuni.fi)

// The example solutions for exercise 5
package dip25.ex5

// some imports that might be required in the tasks
import java.io.File
import java.nio.file.StandardCopyOption.REPLACE_EXISTING
import java.nio.file.{FileSystems, Files, Path, Paths}
import scala.collection.JavaConverters.asScalaIteratorConverter
import scala.concurrent.duration.{Duration, DurationInt, FiniteDuration}
import scala.util.{Try, Success, Failure}

import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.{LinearRegression, LinearRegressionModel}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.nspl.{Color, InLegend, line, par, point, xyplot}
import org.nspl.awtrenderer.{defaultAWTFont, renderToFile, shapeRenderer, textRenderer}


object Ex5Main extends App {
    // COMP.CS.320 Data-Intensive Programming, Exercise 5
    //
    // This exercise is in three parts.
    // - Tasks 1-4 are tasks related to using the Spark machine learning (ML) library.
    // - Tasks 5-7 contain tasks for aggregated data frames with streaming data.
    //  - Task 8 is a theory question related to the lectures.
    //
    // This is the Scala version intended for local development.
    //
    // Each task is separated by the printTaskLine() function. Add your solutions to replace the question marks.
    // There is test code and example output following most of the tasks that involve producing code.
    //
    // At the end of the file, there is a question regarding the use of AI or other collaboration when working the tasks.
    // Please remember to answer the AI question. And finally, don't forget to submit your solutions to Moodle.


    // Some resources that can help with the tasks in this exercise:
    //
    // - The tutorial notebook from our course: in the repository at: /ex1/Basics-of-using-Databricks-notebooks.ipynb
    // - Chapters 8 and 10 in Learning Spark, 2nd Edition: https://learning.oreilly.com/library/view/learning-spark-2nd/9781492050032/
    //     - There are additional code examples in the related GitHub repository: https://github.com/databricks/LearningSparkV2
    //     - The book related notebooks can be imported to Databricks by choosing `import` in your workspace and using the URL
    //       https://github.com/databricks/LearningSparkV2/blob/master/notebooks/LearningSparkv2.dbc
    // - Apache Spark documentation on all available functions that can be used on DataFrames:
    //   https://spark.apache.org/docs/3.5.6/sql-ref-functions.html
    // The full Spark Scala functions API listing for the functions package might have some additional functions listed that
    // have not been updated in the documentation: https://spark.apache.org/docs/3.5.6/api/scala/org/apache/spark/sql/functions$.html


    // In Databricks, the Spark session is created automatically, and you should not create it yourself.
	val spark: SparkSession = SparkSession
        .builder()
        .appName("ex5-solution")
        .config("spark.driver.host", "localhost")
        .master("local")
        .getOrCreate()

    // suppress informational log messages related to the inner working of Spark
    spark.sparkContext.setLogLevel(org.apache.log4j.Level.WARN.toString())

    // reduce the number of shuffle partitions from the default 200 to have more efficient local execution
    spark.conf.set("spark.sql.shuffle.partitions", 8)


    // helper function for removing all files from a given folder
    def removeFiles(folder: String): Unit = {
        Try {
            Files.list(Paths.get(folder))
                .iterator().asScala
                .foreach(file => Files.delete(file))
        } match {
            case Failure(_: java.nio.file.NoSuchFileException) => // the folder did not exist => do nothing
            case Failure(exception) => throw exception
            case Success(_) => // the files were removed successfully => do nothing
        }
    }


    // the initial data for the linear regression tasks
    val hugeSequenceOfXYData: Seq[Row] = Seq(
        (9.44, 14.41), (0.89, 1.77), (8.65, 12.47), (10.43, 15.43), (7.39, 11.03), (10.06, 15.18), (2.07, 3.19), (1.24, 1.45),
        (3.84, 5.45), (10.78, 16.51), (10.23, 16.11), (9.32, 13.96), (7.98, 12.32), (0.99, 1.02), (6.85, 9.62), (8.59, 13.39),
        (7.35, 10.44), (9.85, 15.26), (4.59, 7.26), (2.43, 3.35), (1.58, 2.71), (1.59, 2.2), (2.1, 2.95), (0.62, 0.47),
        (5.65, 9.02), (5.9, 9.58), (8.5, 12.39), (8.74, 13.73), (1.93, 3.37), (10.22, 15.03), (10.25, 15.63), (1.97, 2.96),
        (8.03, 12.03), (2.05, 3.23), (0.69, 0.9), (7.58, 11.01), (9.99, 14.83), (10.53, 15.92), (6.12, 9.48), (1.34, 2.83),
        (3.87, 5.27), (4.98, 7.21), (4.72, 6.48), (8.15, 12.19), (2.37, 3.45), (10.19, 15.16), (10.28, 15.39), (8.6, 12.76),
        (7.46, 11.11), (0.25, 0.41), (6.41, 9.55), (10.49, 15.61), (5.18, 7.92), (3.74, 6.18), (6.27, 9.25), (7.51, 11.11),
        (4.07, 6.63), (5.17, 6.95), (9.61, 14.85), (4.17, 6.31), (4.12, 6.31), (9.22, 13.96), (5.54, 8.2), (0.58, 0.46),
        (10.13, 14.68), (0.53, 1.25), (6.87, 10.0), (7.17, 10.35), (0.09, -0.55), (10.8, 16.6), (10.31, 15.96), (4.74, 6.53),
        (1.6, 2.31), (5.45, 7.84), (0.65, 1.02), (2.89, 3.93), (6.28, 9.21), (8.59, 13.05), (6.6, 10.51), (8.42, 12.91)
    ).map({case(x, y) => Row(x, y)})
    val dataRDD: RDD[Row] = spark.sparkContext.parallelize(hugeSequenceOfXYData)



    printTaskLine(1)
    // Task 1 - Linear regression - Training and test data
    //
    // In statistics, Simple linear regression is a linear regression model with a single explanatory variable.
    // That is, it concerns two-dimensional sample points with one independent variable and one dependent variable
    // (conventionally, the x and y coordinates in a Cartesian coordinate system) and finds a linear function (a non-vertical straight line)
    // that, as accurately as possible, predicts the dependent variable values as a function of the independent variable.
    // The adjective simple refers to the fact that the outcome variable is related to a single predictor.
    // Wikipedia, Simple linear regression: https://en.wikipedia.org/wiki/Simple_linear_regression
    //
    // You are given an RDD of Rows, `dataRDD`, where the first element are the `x` and the second the `y` values.
    // We are aiming at finding a simple linear regression model for the dataset using Spark ML library.
    // I.e. find a function `f` so that `y = f(x)` (for the 2-dimensional case `f(x)=ax+b`).
    //
    // Task instructions
    //
    // Transform the given `dataRDD` to a DataFrame `dataDF`, with two columns `X` (of type Double) and `label` (of type Double).
    // (`label` used here because that is the default dependent variable name in Spark ML library)
    //
    // Then split the rows in the data frame into training and testing data frames.

    val dataSchema: StructType = new StructType(
        Array(
            new StructField("X", DoubleType, false),
            new StructField("label", DoubleType, false)
        )
    )
    val dataDF: DataFrame = spark.createDataFrame(dataRDD, dataSchema)

    // Split the data into training and testing datasets (roughly 80% for training, 20% for testing)
    val trainTestArray: Array[DataFrame] = dataDF.randomSplit(Array(0.8, 0.2), seed=1)

    val trainingDF: DataFrame = trainTestArray(0)
    val testDF: DataFrame = trainTestArray(1)


    println(s"Training set size: ${trainingDF.count()}")
    println(s"Test set size: ${testDF.count()}")
    println("Training set (showing only the first 6 points):")
    trainingDF.limit(6).show()


    // Example output:
    // ===============
    // Training set size: 64
    // Test set size: 16
    // Training set (showing only the first 6 points):
    // +----+-----+
    // |   X|label|
    // +----+-----+
    // |0.09|-0.55|
    // |0.25| 0.41|
    // |0.53| 1.25|
    // |0.58| 0.46|
    // |0.65| 1.02|
    // |0.69|  0.9|
    // +----+-----+



    printTaskLine(2)
    // Task 2 - Linear regression - Training the model
    //
    // To be able to use the ML algorithms in Spark, the input data must be given as a vector in one column.
    // To make it easy to transform the input data into this vector format, Spark offers VectorAssembler objects.
    //
    // - Create a `VectorAssembler` for mapping the input column `X` to `features` column.
    //   And apply it to training data frame, `trainingDF,` in order to create an assembled training data frame.
    // - Then create a `LinearRegression` object.
    //   And use it with the assembled training data frame to train a linear regression model.

    val vectorAssembler: VectorAssembler = new VectorAssembler()
        .setInputCols(Array("X"))
        .setOutputCol("features")

    val assembledTrainingDF: DataFrame = vectorAssembler.transform(trainingDF)


    // print the schema and the first 6 rows of the assembled data frame:
    assembledTrainingDF.printSchema()
    assembledTrainingDF.limit(6).show()


    val lr: LinearRegression = new LinearRegression()
        .setFeaturesCol("features")  // optional, since "features" is the default name
        .setLabelCol("label")        // optional, since "label" is the default name
        // using the default parameters, some other combination might give better results

    // you can print explanations for all the parameters that can be used for linear regression by uncommenting the following:
    // println(lr.explainParams())

    val lrModel: LinearRegressionModel = lr.fit(assembledTrainingDF)


    // print out a sample of the predictions
    lrModel.summary.predictions.limit(6).show()


    // Example output:
    // ===============
    // root
    //  |-- X: double (nullable = false)
    //  |-- label: double (nullable = false)
    //  |-- features: vector (nullable = true)
    // +----+-----+--------+
    // |   X|label|features|
    // +----+-----+--------+
    // |0.09|-0.55|  [0.09]|
    // |0.25| 0.41|  [0.25]|
    // |0.53| 1.25|  [0.53]|
    // |0.58| 0.46|  [0.58]|
    // |0.65| 1.02|  [0.65]|
    // |0.69|  0.9|  [0.69]|
    // +----+-----+--------+
    //
    // and
    //
    // +----+-----+--------+--------------------+
    // |   X|label|features|          prediction|
    // +----+-----+--------+--------------------+
    // |0.09|-0.55|  [0.09]|0.056041394095783625|
    // |0.25| 0.41|  [0.25]|  0.2978850018439783|
    // |0.53| 1.25|  [0.53]|  0.7211113154033191|
    // |0.58| 0.46|  [0.58]|  0.7966874428246298|
    // |0.65| 1.02|  [0.65]|   0.902494021214465|
    // |0.69|  0.9|  [0.69]|  0.9629549231515135|
    // +----+-----+--------+--------------------+



    printTaskLine(3)
    // Task 3 - Linear regression - Test the model
    //
    // Apply the trained linear regression model from task 2 to the test dataset.
    //
    // Then calculate the RMSE (root mean square error) for the test dataset predictions
    // using `RegressionEvaluator` from Spark ML library.
    //
    // (The Python cell after the example output can be used to visualize the linear regression tasks)

    val testPredictions: DataFrame = lrModel.transform(vectorAssembler.transform(testDF))

    val testEvaluator: RegressionEvaluator = new RegressionEvaluator()
        .setLabelCol("label") // optional, since "label" is the default column for the actual labels
        .setPredictionCol("prediction")  // optional, since "prediction", is the default column for the predictions
        .setMetricName("rmse")  // optional, since "rmse" is the default evaluation metric

    // you can print explanations for all the parameters that can be used for the regression evaluator by uncommenting the following:
    // println(testEvaluator.explainParams())

    val accuracy: Double = testEvaluator.evaluate(testPredictions)


    println(s"The RMSE for the model is ${accuracy}")
    testPredictions.limit(6).show()


    // Example output:
    // ===============
    // [info] The RMSE for the model is 0.4021886546595933
    // [info] +----+-----+--------+------------------+
    // [info] |   X|label|features|        prediction|
    // [info] +----+-----+--------+------------------+
    // [info] |0.62| 0.47|  [0.62]|0.8571483447616786|
    // [info] | 1.6| 2.31|   [1.6]| 2.338440442219371|
    // [info] |1.93| 3.37|  [1.93]|2.8372428832000223|
    // [info] |1.97| 2.96|  [1.97]|2.8977037851370713|
    // [info] |2.07| 3.19|  [2.07]|3.0488560399796927|
    // [info] |4.72| 6.48|  [4.72]|7.0543907933091665|
    // [info] +----+-----+--------+------------------+
    //
    // 0 for RMSE would indicate perfect fit and the more deviations there are the larger the RMSE will be.
    // You can try a different seed for dividing the data and different parameters for the linear regression to get different results.


    // Visualization of the linear regression exercise (can be done since there are only limited number of source data points)
    // The plot will be created into a PDF file "linear_regression.pdf"
    val trainingData = trainingDF.collect().map({case Row(x: Double, y: Double) => (x, y)}).toSeq
    val testData = testDF.collect().map({case Row(x: Double, y: Double) => (x, y)}).toSeq
    val predictionData = testPredictions.select("X", "prediction").collect().map({case Row(x: Double, y: Double) => (x, y)}).toSeq
    val minX: Double = (trainingData ++ testData).map({case (x, _) => x}).min
    val maxX: Double = (trainingData ++ testData).map({case (x, _) => x}).max
    val minY: Double = (trainingData ++ testData).map({case (_, y) => y}).min
    val maxY: Double = (trainingData ++ testData).map({case (_, y) => y}).max

    val plot = xyplot(
        (trainingData, List(point(color = Color.BLUE)), InLegend("training data")),
        (testData, List(point(color = Color.GREEN)), InLegend("test data")),
        (predictionData, List(line(color = Color.RED)), InLegend("predictions"))
    )(
        par(
            xlab = "x",
            ylab = "y",
            xlim = Some(minX, maxX),
            ylim = Some(minY, maxY),
            noLegend = false
        )
    )
    renderToFile(f = new File("linear_regression.pdf"), elem = plot, width = 800, mimeType = "application/pdf")



    printTaskLine(4)
    // Task 4 - Linear regression - Making new predictions
    //
    // Use the trained `LinearRegressionModel` from task 2 to predict the `y` values for the following `x` values:
    // -2.8, 3.14, 9.9, 11.11, 22.22, 123.45

    val newData: Seq[Double] = Seq(-2.8, 3.14, 9.9, 11.11, 22.22, 123.45)
    val newDataRows: Seq[Row] = newData.map(value => Row(value))
    val newDataSchema: StructType = new StructType(
        Array(
            new StructField("X", DoubleType, false)
        )
    )
    val newDataFrame: DataFrame = spark.createDataFrame(spark.sparkContext.parallelize(newDataRows), newDataSchema)

    // or alternative without going through the RDD creation first by
    // using the implicit transformations available from the SparkSession
    import spark.implicits._
    val newDataFrame2: DataFrame = newData.toDF("X")

    val assembledNewDataDF: DataFrame = vectorAssembler.transform(newDataFrame)
    val newPredictions: DataFrame = lrModel.transform(assembledNewDataDF)


    newPredictions.select("X", "prediction").show()

    // Extra: you can get the slope and the constant term for the prediction line from the trained model

    val a = lrModel.coefficients(0)
    val b = lrModel.intercept

    println("Prediction line: y = ax + b")
    println(s"a = ${a}")
    println(s"b = ${b}")

    val x_test = 123.45
    val y_test = a * x_test + b
    println(s"Prediction for x = ${x_test}  =>  y = ${y_test}")


    // Example output:
    // ===============
    // +------+-------------------+
    // |     X|         prediction|
    // +------+-------------------+
    // |  -2.8|-4.3122587708559825|
    // |  3.14|  4.666185166795745|
    // |   9.9|  14.88407759415697|
    // | 11.11|  16.71301987775269|
    // | 22.22| 33.506035390767956|
    // |123.45|  186.5174629679539|
    // +------+-------------------+



    printTaskLine(5)
    // Task 5 - Best selling days for sales data
    //
    // This task is in two parts, the first part is to calculate a reference result with static data.
    // And the second part is to start preparing to do a similar calculation with streaming data.
    //
    // In the weekly-exercises repository, the "data/ex5/static" folder contains "superstore_sales.csv" file sales data from a retailer.
    // The data is based on a dataset from https://www.kaggle.com/datasets/soumyashanker/superstore-us
    //
    //
    // ## Background on the streaming data simulation
    //
    // In this exercise, tasks 6-8, streaming data is simulated by copying CSV files from a source folder to a target folder.
    // The target folder can thus be considered as streaming data, with a new file appearing after each file is copied.
    // A helper function to handle the file copying is given in task 8 which you don't need to modify.
    // The source folder from which the files are copied is under the `data` folder at the root of the repository.
    // The target folder will be the `data/streaming` folder within this project.
    //
    // ## The tasks
    //
    // Part 1: calculation with static data
    // - Read the data from the CSV file into a data frame called `staticSalesDF`.
    // - Calculate the total sales for each day, and show the eight best selling days.
    //     - Each row has a sales record for a specific product.
    //     - The column `productPrice` contains the price for an individual product.
    //     - The column `productCount` contains the count for how many items were sold in the given sale.
    //
    // Part 2: setting up a streaming data frame for upcoming data
    // - Create a streaming data frame for similar retailer sales data as was used in part 1.
    // The streaming data frame should point to the folder given by `myStreamingFolder`.
    //
    // Hint: Spark cannot infer the schema of streaming data, so you have to give it explicitly.
    // You can assume that the streaming data will have the same format as the static data used in part 1,
    // except that there will not be any header rows in the streaming data.

    val staticDataPath: String = "../../data/ex5/static/superstore_sales.csv"
    val staticSalesDF: DataFrame = spark
        .read
        .option("header", true)
        .option("sep", ";")
        .option("inferSchema", true)
        .csv(staticDataPath)

    val staticBestDaysDF: DataFrame = staticSalesDF
        .withColumn("totalPrice", col("productPrice") * col("productCount"))
        .groupBy("orderDate")
        .agg(sum("totalPrice").alias("totalSales"))
        .orderBy(col("totalSales").desc)
        .limit(8)

    // Output for part 1 with the static data
    println("The best selling days with the static data:")
    staticBestDaysDF.show()


    val myStreamingFolder: String = "data/streaming"
    // Ensure the folder exists and is empty in the Azure storage when creating the streaming data frame
    Files.createDirectories(Paths.get(myStreamingFolder))
    removeFiles(myStreamingFolder)


    val streamingSalesDF: DataFrame = spark
        .readStream
        .option("header", false)
        .option("sep", ";")
        .schema(staticSalesDF.schema)
        .csv(myStreamingFolder)


    // Example output:
    // ===============
    // The best selling days with the static data:
    // +----------+------------------+
    // | orderDate|        totalSales|
    // +----------+------------------+
    // |2014-03-18|          52148.32|
    // |2014-09-08|           22175.9|
    // |2017-11-04|          19608.77|
    // |2016-11-25|19608.739999999998|
    // |2016-10-02|18580.819999999996|
    // |2017-10-22|          18317.99|
    // |2016-05-23|16754.809999999998|
    // |2015-09-17|           16601.4|
    // +----------+------------------+
    //
    // Note that you cannot really test part 2 of this task before you have also done the tasks 6 and 7.
    // I.e. there is no checkable output from part 2.



    printTaskLine(6)
    // Task 6 - Best-selling days with streaming data
    //
    // Find the best-selling days using the streaming data frame from task 5, part 2.<br>
    // Also, this time include the total number of individual items sold on each day in addition to the total sales in the result.
    //
    // Note that in this task with the streaming data you don't need to limit the result
    // only to the best eight selling days like was done in task 5, part 1.

    val streamingBestDaysDF: DataFrame = streamingSalesDF
        .withColumn("totalPrice", col("productPrice") * col("productCount"))
        .groupBy("orderDate")
        .agg(
            sum("totalPrice").alias("totalPrice"),
            sum("productCount").alias("totalCount")
        )
        .orderBy(col("totalPrice").desc)
        // NOTE: limit is not supposed to work with streaming data
        // https://spark.apache.org/docs/3.5.6/structured-streaming-programming-guide.html#unsupported-operations


    // Note that you cannot really test this task before you have also done the task 7.
    // I.e. there is no checkable output from this task.



    printTaskLine(7)
    // Task 7 - Running a streaming query
    //
    // Test your streaming data solution from tasks 5 and 6 by creating and starting a streaming query.
    //
    // The cell below this one contains helper scripts for copying the files (to simulate streaming data).
    // The processing time for the streaming query might depend on the number of active users in Databricks.
    // You might need to adjust the defined delays to get the system working as intended.

    // There can be delays with the streaming data frame processing. You can try to adjust the wait time if you want.
    val waitAfterFirstCopy: FiniteDuration = 5.seconds
    val normalTimeInterval: FiniteDuration = 3.seconds

    def copyFiles(targetFolder: String): Unit = {
        val sourceFolder: Path = Paths.get("../../data/ex5/streaming")
        val inputFileList: Seq[Path] = Files.list(sourceFolder).iterator().asScala.toSeq.sorted

        inputFileList.zipWithIndex.foreach { case (csvFile, index) =>
            val outputFilePath: Path = Paths.get(targetFolder).resolve(csvFile.getFileName())

            // copy file from the source folder to the target folder
            Files.copy(csvFile, outputFilePath, REPLACE_EXISTING)
            val waitTime: FiniteDuration = if (index == 0) waitAfterFirstCopy else normalTimeInterval
            println(s"Copied file ${csvFile.getFileName()} (${index + 1}/${inputFileList.size}) to ${outputFilePath} - waiting for ${waitTime}")
            Thread.sleep(waitTime.toMillis)
        }
    }


    // remove all files from the target folder before starting the streaming query to have a fresh run each time
    removeFiles(myStreamingFolder)


    // start the streaming query with the console format and limit the output to the first 8 rows
    val myStreamingQuery: StreamingQuery = streamingBestDaysDF
        .writeStream
        .format("console")
        .outputMode("complete")
        .option("numRows", 8)
        .queryName(s"ex5_streaming")
        .start()


    // call the helper function to copy files to the target folder to simulate streaming data
    copyFiles(myStreamingFolder)

    // wait and show the processing of the streaming query
    myStreamingQuery.awaitTermination(normalTimeInterval.toMillis)

    // stop the streaming query
    myStreamingQuery.stop()


    // Example output:
    // ===============
    // If all copied files were handled by the streaming query, the final state should match the output from task 5, part 1
    // (the `orderDate` and the `totalPrice` columns apart from possible rounding errors).
    //
    // Copied file 0001.csv (1/10) to data/streaming/0001.csv - waiting for 5 seconds
    // -------------------------------------------
    // Batch: 0
    // -------------------------------------------
    // +----------+------------------+----------+
    // | orderDate|        totalPrice|totalCount|
    // +----------+------------------+----------+
    // |2014-07-26|          10931.66|        11|
    // |2017-12-07| 9454.390000000001|         8|
    // |2016-05-07|            4199.9|        10|
    // |2017-09-10|3943.4700000000003|         8|
    // |2017-09-09|3848.2000000000003|        15|
    // |2017-07-14|           3737.88|        12|
    // |2015-05-22|3716.6500000000005|         7|
    // |2016-12-03|3501.5699999999997|        12|
    // +----------+------------------+----------+
    // only showing top 8 rows
    // Copied file 0002.csv (2/10) to data/streaming/0002.csv - waiting for 3 seconds
    // -------------------------------------------
    // Batch: 1
    // -------------------------------------------
    // +----------+------------------+----------+
    // | orderDate|        totalPrice|totalCount|
    // +----------+------------------+----------+
    // |2017-11-17|          11052.78|        15|
    // |2014-07-26|11041.289999999999|        18|
    // |2017-12-07| 9454.390000000001|         8|
    // |2016-04-16| 9153.849999999999|        13|
    // |2017-08-17|           8917.08|        21|
    // |2017-10-13| 7259.849999999999|        10|
    // |2015-01-28|           7162.74|        13|
    // |2015-12-15| 6498.539999999999|        11|
    // +----------+------------------+----------+
    // only showing top 8 rows
    //
    // ...
    // ...
    // ...
    //
    // Copied file 0010.csv (10/10) to data/streaming/0010.csv - waiting for 3 seconds
    // -------------------------------------------
    // Batch: 9
    // -------------------------------------------
    // +----------+------------------+----------+
    // | orderDate|        totalPrice|totalCount|
    // +----------+------------------+----------+
    // |2014-03-18|          52148.32|        48|
    // |2014-09-08|           22175.9|       112|
    // |2017-11-04|          19608.77|        58|
    // |2016-11-25|19608.739999999998|        53|
    // |2016-10-02|18580.819999999992|        27|
    // |2017-10-22|18317.989999999994|        49|
    // |2016-05-23|16754.809999999998|        23|
    // |2015-09-17|           16601.4|        86|
    // +----------+------------------+----------+
    // only showing top 8 rows



    printTaskLine(8)
    // Task 8 - Theory question
    //
    // Using your own words, answer the following questions:
    //
    // 1. Spark provides both low-level APIs (RDDs) and high-level APIs (DataFrames and Datasets) for data processing.
    //     - When should you prefer using RDDs over DataFrames?
    //     - When should you prefer using DataFrames over RDDs?
    // 2. The last coding tasks in this exercise related to streaming data.
    //     - What are the differences between processing static data and processing streaming data?
    //     - Using Spark, are there any limitations with processing streaming data compared to static data?
    //       If there are, give some examples.
    //
    // Extensive answers are not required here.
    // If your answers do not fit into one screen, you have likely written more than what was expected.

    // 1. RDDs vs DataFrames
    //   - RDDs can be used when you want to specify exactly what Spark should do with the data and how.
    //     RDDs can be an easier to start working with unstructured data than DataFrames.
    //     In Python, the map function is not available for DataFrames, and if you need to map an entire row using RDDs
    //     might be simpler than working with the methods provided by DataFrames.
    //     In addition, some legacy third-party APIs might require the use of RDDs.
    //   - DataFrames should be preferred by default. They provide optimizations and better performance compared to RDDs,
    //     i.e., DataFrames can optimize the transformations asked by the user while RDDs process the data in the way the user has instructed.
    //     For any structured data, it is likely better to handle them with DataFrames. With unstructured data,
    //     it might be reasonable to start with RDDs and then transform to DataFrame and handle the rest of the data processing using them.
    //   - RDDs can be transformed into DataFrames and vice versa, but the transformation will always cause some overhead and decreased performance.
    // 2. Streaming data
    //   - Static data is fully available at the start of the data processing, while streaming data arrives continuously and must be
    //     processed incrementally. By default, there is no end to the processing of streaming data.
    //   - There are some limitations with handling streaming data compared to static data:
    //     https://spark.apache.org/docs/3.5.6/structured-streaming-programming-guide.html#unsupported-operations
    //   - Some aggregations, joins, or sorting are either restricted or require careful state and watermark management.
    //   - For example, getting the total number of rows with .count() is not supported with streaming data frames.
    //     The same with trying to use .show() for printing the first few rows.
    //   - In addition, when dealing with streaming data, special consideration have to be made with
    //     fault tolerance and other streaming specific features.



    printAIQuestionTaskLine()
    // Use of AI and collaboration
    //
    // Using AI and collaborating with other students is allowed when doing the weekly exercises.
    // However, the AI use and collaboration should be documented.
    //
    // - Did you use AI tools while doing this exercise?
    //   - Did they help? And how did they help?
    // - Did you work with other students to complete the tasks?
    //   - Only extensive collaboration is expected to be reported. If you only got help
    //     for a couple of the tasks, you don't need to report it here.

    // AI tool usage and other collaboration should be mentioned here.



    // Helper function to separate the task outputs from each other
    def printTaskLine(taskNumber: Int): Unit = {
        println(s"======\nTask $taskNumber\n======")
    }

    def printAIQuestionTaskLine(): Unit = {
        println("======\nAI and collaboration\n======")
    }

    // Typically, at the end you would stop the Spark session to free up resources.
    // DO NOT do this in Databricks! It will restart the entire cluster for all users.
    // (that is why it is commented out here too, getting to the end will stop the session automatically)
    // spark.stop()
}
