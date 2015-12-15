import StateStructures.*;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.javers.core.Javers;
import org.javers.core.JaversBuilder;
import org.javers.core.diff.Change;
import org.javers.core.metamodel.object.CdoSnapshot;
import org.javers.repository.jql.QueryBuilder;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.*;

/**
 * Created by Pavel Smirnov
 */
public class ClusterStateMonitor {

    private static Log logger = LogFactory.getLog(ClusterStateMonitor.class);
    private Javers javers;
    private int sleepInterval = 3000;
    private CommonMongoClient mongoClient;

    public static void main(String[] args){

        ClusterStateMonitor monitor = new ClusterStateMonitor();
        monitor.mongoClient = new CommonMongoClient();
        monitor.StartMonitoring();
    }

    public ClusterStateMonitor(){
        //MongoRepository mongoRepo = new MongoRepository(db);
        //javers = JaversBuilder.javers().registerJaversRepository(mongoRepo).build();

    }

    public void StartMonitoring(){
        //while(1==1){
            try {
                ClusterState state = getMesosState("http://192.168.92.11:5050/master/state.json");
                saveStateToDB(state);
                //javers.commit("clusterState", state);
                Thread.sleep(sleepInterval);
            } catch (InterruptedException e){
                logger.error(e);
            }
        //}
    }


    public ClusterState getMesosState(String ApiUrl) {

        String[] frameworkKeys = new String[]{"frameworks", "completed_frameworks"};
        String[] taskKeys = new String[]{     "tasks", "completed_tasks"};

        JSONObject json = Utils.getJsonFromUrl(ApiUrl);
        HashMap state = Utils.getHashMapFromJSONObject(json);
        Double startedTime = (Double) state.get("start_time");

        ClusterState clusterState = null;
        if (startedTime != null){
            Date started = new Date((long) (startedTime * 1000));
            clusterState = new ClusterState("Mesos", started);

            List<FrameworkState> frameworkStates = new ArrayList<FrameworkState>();
            for (String frkey : frameworkKeys){
                for (HashMap framework : (ArrayList<HashMap>) state.get(frkey)){

                    String status = (frkey.contains("completed") ? "Completed" : "Active");
                    FrameworkState frameworkState = new FrameworkState(framework.get("id").toString(),
                                                                       framework.get("name").toString(),
                                                                       clusterState,
                                                                       status);
                    for (HashMap executor : (ArrayList<HashMap>) framework.get("executors")){
                        HashMap command = (HashMap)executor.get("command");
                        String test1 = "123";
                    }

                    List<TaskState> taskStates = new ArrayList<TaskState>();
                    for (String taskKey : taskKeys){
                        for (HashMap task : (ArrayList<HashMap>) framework.get(taskKey)){


                            TaskState taskState = new TaskState(task.get("id").toString(),
                                                                frameworkState,
                                                                status,
                                                                task.get("slave_id").toString());
                            //resources = (HashMap<String, Object>) Utils.getJSONObjectByKey(task, "resources");
                            //taskState.setResources(resources);

                            List<StatusChange> statusChanges = new ArrayList<StatusChange>();
                            for (HashMap statusChangeObj : (ArrayList<HashMap>) task.get("statuses")){
                                status = statusChangeObj.get("state").toString();
                                Double doublestamp = (Double) statusChangeObj.get("timestamp");
                                Date timestamp = new Date((long) (doublestamp * 1000));

                                StatusChange statusChange = new StatusChange(status, timestamp);
                                statusChanges.add(statusChange);
                            }
                            taskState.setStatusChanges(statusChanges);
                            taskStates.add(taskState);
                        }
                    }
                    frameworkState.setTaskStates(taskStates);
                    frameworkStates.add(frameworkState);
                }
            }
            clusterState.setFrameworkStates(frameworkStates);

            List<HostState> hostStates = new ArrayList<HostState>();
            for (HashMap slave : (ArrayList<HashMap>) state.get("slaves")){
                HostState hostState = new HostState(slave.get("id").toString(), slave.get("pid").toString(), slave.get("hostname").toString());
                hostStates.add(hostState);
            }
            clusterState.setHostStates(hostStates);
        }
        return clusterState;
    }

    public void saveStateToDB(ClusterState state){
        mongoClient.saveObjectToDB(state, "clusterStates");
    }

    public void RestoreStates(){

        List<CdoSnapshot> snapshots = javers.findSnapshots(QueryBuilder.byInstanceId("1", ClusterState.class).build());
        JaversShapshotsCompiler repReader = new JaversShapshotsCompiler(javers);
        CdoSnapshot snapshot = snapshots.get(0);
        ClusterState prevState = (ClusterState)repReader.compileEntityStateFromSnapshot(snapshots.get(0));
        ClusterState prevState1 = (ClusterState)repReader.compileEntityStateFromSnapshot(snapshots.get(1));

        //List<Change> changes = StatesCompare(prevState, currState);
        //saveChangesToDB(changes);
        //saveStateToDB(currState);

        //List<Change> changes = StatesCompare(prevState, currState);
        //saveChangesToDB(changes);
        //saveStateToDB(currState);
        //}

    }

    public List<Change> CompareStates(ClusterState clusterState1, ClusterState clusterState2){
        Javers javers = JaversBuilder.javers().build();
        List<Change> ret = javers.compare(clusterState1, clusterState2).getChanges();
        return ret;
    }

    public ClusterState getStateFromDB(String id){
        ClusterState ret = null;
//        MongoCollection coll = db.getCollection("clusterStates");
//        Document bson = (Document) coll.find(new Document("ID", id)).projection(new Document("_id", 0)).first();
//        ret = Utils.CreateObjectFromJSON(bson.toJson(), ClusterState.class);
        return ret;
    }


    public void saveChangesToDB(List<Change> changes){
        //saveJSONToDB(changes, "clusterStatesChanges");
        //JSONObject stateJSON = new JSONObject(changes);
//        String jsonStr = stateJSON.toString();
//        Document bson = Document.parse(jsonStr);
//        MongoCollection coll = db.getCollection("clusterStatesChanges");
        //coll.insertOne(bson);
    }



    public void applyChangesToObject(List<Change> changes){

    }

}
