package ifmo.escience.dapris.monitoring.clusterStateMonitor;
import ifmo.escience.dapris.monitoring.clusterStateMonitor.StateStructures.*;
import ifmo.escience.dapris.monitoring.common.Utils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;

import java.util.*;

/**
 * Created by Pavel Smirnov
 */
public class MesosRestClient implements IStateDataProvider {
    private String masterHost;
    private static Logger log = LogManager.getLogger(MesosRestClient.class);
    public MesosRestClient(String masterHost){
        this.masterHost = masterHost;
    }

    public ClusterState GetData(){
        Long operationStarted = System.currentTimeMillis();
        String[] frameworkKeys = new String[]{"frameworks", "completed_frameworks"};
        String[] taskKeys = new String[]{     "tasks", "completed_tasks"};

        String url = "http://" + this.masterHost + ":5050/master/state.json";
        //String url = "http://192.168.1.36/state.json";

        JSONObject json = Utils.getJsonFromUrl(url);
        HashMap state = Utils.getHashMapFromJSONObject(json);
        Double started = (Double) state.get("start_time");

        ClusterState clusterState = null;
        if (started != null){

            clusterState = new ClusterState(started, "Mesos@"+this.masterHost);

            List<Slave> slaves = new ArrayList<Slave>();
            for (HashMap slavemap : (ArrayList<HashMap>) state.get("slaves")){
                String slid= slavemap.get("pid").toString();
                Slave slave = new Slave(slavemap.get("id").toString(), slid, slavemap.get("hostname").toString());
                List <Pair> resources = TransformToPairs((HashMap)slavemap.get("resources"), slid+"-");
                slave.setResources(resources);
                slaves.add(slave);
            }
            clusterState.setSlaves(slaves);

            List<Framework> frameworks = new ArrayList<Framework>();
            for (String frkey : frameworkKeys){
                int frameworkI=0;
                ArrayList<HashMap> frameworkList = (ArrayList<HashMap>) state.get(frkey);
                for (frameworkI=0; frameworkI<frameworkList.size() /*&& frameworkI<1*/; frameworkI++){
                    HashMap frameworkmap = frameworkList.get(frameworkI);
                    String status = (frkey.contains("completed") ? "Completed" : "Active");
                    String frid = frameworkmap.get("id").toString();
                    Framework framework = new Framework(frid,frameworkmap.get("name").toString(), clusterState.getId() ,status);
                    List <Pair> resources = TransformToPairs((HashMap)frameworkmap.get("resources"), frid+"-");
                    framework.setResources(resources);

                    List<Executor> executors = new ArrayList<Executor>();
                    ArrayList<HashMap>executorList = (ArrayList<HashMap>) frameworkmap.get("executors");
                    for (int executorI=0; executorI<executorList.size() /*&& executorI<1*/; executorI++){
                        HashMap executormap = executorList.get(executorI);

                        String execid = executormap.get("executor_id").toString();
                        String slaveId = executormap.get("slave_id").toString();
                        HashMap<String, Object> commandMap = (HashMap<String, Object>)executormap.get("command");
                        List<Pair> command = TransformToPairs(commandMap, execid+"-");
                        resources = TransformToPairs((HashMap)executormap.get("resources"), execid+"-");
                        executors.add(new Executor(execid, frid, slaveId, "Active", command, resources));
                    }
                    framework.setExecutors(executors);

                    List<Task> tasks = new ArrayList<Task>();
                    for (String taskKey : taskKeys){
                        int taskI=0;
                        ArrayList<HashMap> taskList = (ArrayList<HashMap>)frameworkmap.get(taskKey);
                        for (taskI=0; taskI<taskList.size() /*&& taskI<1*/; taskI++){
                            HashMap taskmap = taskList.get(taskI);
                            String slaveId = taskmap.get("slave_id").toString();
                            String tid = frid+"-T"+taskmap.get("id").toString();
                            Task task = new Task(tid, framework.getId(), slaveId);

                            resources = TransformToPairs((HashMap)taskmap.get("resources"), tid+"-");
                            task.setResources(resources);

                            List<StatusChange> statusChanges = new ArrayList<StatusChange>();
                            for (HashMap statusChangeObj : (ArrayList<HashMap>) taskmap.get("statuses")){
                                status = statusChangeObj.get("state").toString();
                                Double timestamp = (Double) statusChangeObj.get("timestamp");
                                //DateTime timestamp = new DateTime((long) (doublestamp * 1000));

                                StatusChange statusChange = new StatusChange(tid+"-"+status, status, timestamp);
                                statusChanges.add(statusChange);
                            }
                            task.setStatusChanges(statusChanges);
                            //if(tid.equals("3"))
                            tasks.add(task);
                        }
                    }
                    framework.setTasks(tasks);
                    frameworks.add(framework);
                }
            }
            clusterState.setFrameworks(frameworks);

        }
        log.trace("GetActualClusterState took: " + (System.currentTimeMillis() - operationStarted) / 1000 + " seconds");
        return clusterState;
    }

    private static List<Pair> TransformToPairs(HashMap<String, Object> map, String prefix){
        ArrayList<Pair> ret = new ArrayList<Pair>();
        for (String key : map.keySet()){
            Object value = map.get(key);
            if (value instanceof HashMap){
                //value = TransformToPairs((HashMap<String, Object>)value, prefix+key+".");
            }
            if (value instanceof List)
                for (int i=0; i<((List)value).size(); i++){
                    Object value2 = ((List)value).get(i);
                    if (value2 instanceof HashMap) {
                        //((List)value).set(i, TransformToPairs((HashMap<String, Object>)value2, prefix+key+"."+i+"."));
                    }
                }

            ret.add(new Pair(key, value, prefix+key));
        }
        return ret;
    }
}
