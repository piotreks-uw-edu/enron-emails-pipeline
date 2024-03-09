// Databricks notebook source
// DBTITLE 1,Set the environment
// Set the errant environment variable...
val storageAccountName = "FAKE"
val accessKey = "FAKE"
spark.sparkContext.hadoopConfiguration.set(s"fs.azure.account.key.${storageAccountName}.dfs.core.windows.net", accessKey)

// COMMAND ----------

// DBTITLE 1,Set directories
val enronDirName = s"FAKE"
val myDirName = s"FAKE"

val enronDataDir = s"abfss://${enronDirName}@${storageAccountName}.dfs.core.windows.net/"
val emailDir = enronDataDir + "/maildir/*/*/*"
val myDataDir = s"abfss://${myDirName}@${storageAccountName}.dfs.core.windows.net/"

// COMMAND ----------

// DBTITLE 1,Parse email function
def parseEmail(email: String): (String, String, String, String, String) = {
  def extractField(field: String): String = {
    // thec cell content starts from the end of 'field'
    val startIndex = email.indexOf(field) + field.length
    //and ends with the line break
    val endIndex = email.indexOf("\n", startIndex)
    if (endIndex > -1)
    //get rid of spaces around the phrase
    email.substring(startIndex, endIndex).trim 
    else ""
  }
  val messageId = extractField("Message-ID: ")
  val date = extractField("Date: ")
  val from = extractField("From: ")
  val to = extractField("To: ")
  val emailBody = email
  (messageId, date, from, to, emailBody)
}


// COMMAND ----------

// DBTITLE 1,Read and split emails
// Read emails from the whole directory structure
val emailsRDD = sc.wholeTextFiles(emailDir)

val splitEmailsRDD = emailsRDD.map(fileTuple => {
  val splitEmail = parseEmail(fileTuple._2)
  splitEmail
}
)

// COMMAND ----------

// DBTITLE 1,Output
import spark.implicits._

//convert RDD to DataFrame
val emailDF = splitEmailsRDD.toDF("messageId", "date", "from", "to", "emailBody")

//write CSV as one file
val coalescedCsvDF = emailDF.coalesce(1)
coalescedCsvDF.write.mode("overwrite").option("header", "true").csv(myDataDir + "enron/emails.csv")

//write Parquet
emailDF.write.mode("overwrite").parquet(myDataDir + "enron/emails.parquet")
