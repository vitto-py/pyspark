import java.sql.Timestamp
import org.apache.spark.sql.streaming.GroupState
import scala.collection.mutable.ListBuffer

/*
flatMapGroupsWithState API,
which supports the ability to output more than one row per group.

the user activity input data is represented
by the UserActivity class. The intermediate state of user session data is represented by
the UserSessionState class, and the UserSessionInfo class represents the user session
output

*/

case class UserActivity(user:String, action:String, page:String, ts:Timestamp)
// CLASS THAT IS ALSO A VARIABLE -> why VAR is needed? Direct Field Modification: Each field of the UserSessionState (status, lastTS, numPage, startTS, endTS) is modified in place without creating a new instance.
case class UserSessionState(var user:String,
                            var status:String,
                            var startTS:Timestamp,
                            var endTS:Timestamp,
                            var lastTS:Timestamp,
                            var numPage:Int)

// the end time stamp is filled when the session has ended.
case class UserSessionInfo(userId:String, start:Timestamp, end:Timestamp, numPage:Int)

/* updateUserActivity, which
is responsible for updating the user session state based on user activity. the startTS and endTS
*/
def updateUserActivity(userSessionState: UserSessionState, userActivity: UserActivity): UserSessionState = {
  
  userSessionState
}
/*
updateAcrossAllUserActivities.
is the callback function that is passed into the flatMapGroupsWithState function
This function has two main responsibilities.
  The first one is to handle the timeout of the intermediate session
  state, and it updates the user session end time when such a condition arises.
  The other responsibility is to determine when and what to send to the output. The desired output
  is to emit one row when a user session is started and another one when a user session is ended.
*/