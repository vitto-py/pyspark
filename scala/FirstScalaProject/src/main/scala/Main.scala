import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

import org.apache.spark.sql.streaming.GroupState
import org.apache.spark.sql.streaming.{GroupStateTimeout, OutputMode}



// this is the information provided by the JSON
case class RackInfo(rack:String, temperature:Double, ts:java.sql.Timestamp)

// our state keeper
// the lastTS variable is used to compare the time between previous and current temperature reading
case class RackState(var rackId:String, var highTempCount:Int,
                     var status:String,
                     var lastTS:java.sql.Timestamp)

object Main {
  /*2 Conditions
1) Temperature is above 100 degrees
2) 3 consecutive measures within a 60 second time gap between each measure
*/
  def updateRackState(rackState: RackState, rackInfo: RackInfo): RackState = {
    /*
  rackInfo describes the current information from the JSON
  RackState keeps track of
  */
    // rackState.lastTS if rackState.lastTS else rackInfo.ts
    val lastTS = Option(rackState.lastTS).getOrElse(rackInfo.ts)

    // is the distance less than 60 seconds?
    val withinTimeThreshold = (rackInfo.ts.getTime - lastTS.getTime) <= 60000
    // is temperature above 100
    val greaterThanEqualTo100 = rackInfo.temperature >= 100.0

    // Corrector of withinTimeThreshold logic, when count = 0, always TRUE
    // so if the temperatue is above 100, always enters into the first case
    val meetCondition = if (rackState.highTempCount < 1) true else withinTimeThreshold

    //
    (greaterThanEqualTo100, meetCondition) match {
      case (true, true) => {
        rackState.highTempCount = rackState.highTempCount + 1
        rackState.status = if (rackState.highTempCount >= 3) "Warning" else "Normal"
      }
      case _ => { // any other case (normal else of python)
        rackState.highTempCount = 0
        rackState.status = "Normal"
      }
    }

    // update lastState
    rackState.lastTS = rackInfo.ts

    // return
    rackState
  }


  // call-back function to provide mapGroupsWithState API
  def updateAcrossAllRackStatus(rackId: String,
                                inputs: Iterator[RackInfo],
                                oldState: GroupState[RackState]): RackState = {
    var rackState = if (oldState.exists) oldState.get else RackState(rackId, 5, "", null)

    // sort the inputs by timestamp in ascending order
    inputs.toList.sortBy(_.ts.getTime).foreach(input => {
      rackState = updateRackState(rackState, input)
      // very important to update the rackState in the state holder class GroupState
      oldState.update(rackState)
    })
    //return
    rackState
  }

  def main(args: Array[String]): Unit = {


    //println("Hello world!")
    val spark = SparkSession.builder()
      .appName("DataStateful")
      .master("local")
      .getOrCreate()

    // NO MORE INFO MESSAGES
    spark.sparkContext.setLogLevel("ERROR")

    // WEIRD SCALA - From here come the encoders, if you don't put it, the compiler complains
    import spark.implicits._

    // schema for the IoT data
    val iotDataSchema = new StructType()
      .add("rack",StringType, false)
      .add("temperature", DoubleType, false)
      .add("ts", TimestampType, false)

    /* ------------- NORMAL DF -------------
    seems that IntellJ directories start on the main dir
    val df = spark.read.schema(iotDataSchema).json("data/file1.json")
    df.show()*/

    // ---- STREAMING ---------
    /* For this section a new folder called dataStraaming was created,
    you must copy and paste file1.json, file2.json, .... one at the time
    in order to simulate streaming
    */

    // StreamDataFrame (starts empty, but gets updated constantly
    val iotSSDF = spark.readStream.schema(iotDataSchema).json("dataStreaming/")

    // PerformAction
    /*
    ----------- mapGroupsWithState -----------
    dataset
      .groupByKey(...)
      .mapGroupsWithState(GroupStateTimeout.ProcessingTimeTimeout)(mappingFunction)
     */
    val iotPatternDF = iotSSDF.as[RackInfo] // converts the DataFrame into a Dataset of RackInfo objects
      .groupByKey(_.rack) // groups the RackInfo objects by their rack field, effectively organizing the data based on different racks.
      .mapGroupsWithState[RackState,RackState](GroupStateTimeout.NoTimeout)(updateAcrossAllRackStatus)
    // [RackState,RackState] Type Parameters: (state,output) in this case, same type

    // setup the output and start the streaming query
    val iotPatternSQ = iotPatternDF.writeStream.format("console").outputMode("update").start()

    iotPatternSQ.awaitTermination()
    //iotPatternSQ.stop()
  }
}




