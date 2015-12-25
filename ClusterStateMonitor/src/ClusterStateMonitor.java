import StateStructures.*;
import StateStructures.Task;
import com.mongodb.BasicDBObject;
import com.mongodb.client.FindIterable;
import ifmo.escience.dapris.common.entities.*;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import org.bson.Document;

import org.javers.core.Javers;
import org.javers.core.JaversBuilder;
import org.javers.core.diff.Diff;
import org.javers.core.metamodel.object.CdoSnapshot;
import org.javers.repository.jql.QueryBuilder;
import org.javers.repository.mongo.MongoRepository;
import org.json.JSONObject;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;
/**
 * Created by Pavel Smirnov
 */
public class ClusterStateMonitor {

    private static Logger log = LogManager.getLogger(ClusterStateMonitor.class);
    private Javers javers;
    private int sleepInterval = 3000;
    public boolean useVersioning = false;
    public String masterHost;
    private CommonMongoClient mongoClient;

    public static void main(String[] args){
        ClusterStateMonitor monitor = new ClusterStateMonitor("192.168.92.11", new CommonMongoClient());
        if (args!=null  && args.length>0 && args[0].equals("getData")){
            ArrayList<String> dates = monitor.getStartedClusters();
            String stateID = dates.get(dates.size()-1);
            monitor.getStateFromDB(stateID);
        }else{
              monitor.startMonitoring();
        }

    }

    public ClusterStateMonitor(String masterHost, CommonMongoClient mongoClient){
        this.masterHost = masterHost;
        this.mongoClient = mongoClient;
        MongoRepository mongoRepo = new MongoRepository(mongoClient.getDefaultDB());
        javers = JaversBuilder.javers().registerJaversRepository(mongoRepo).build();
    }

    public void startMonitoring(){
        log.info("Monitoring stated");
        while(1==1){
            try {
                ClusterState state = getActualClusterState();
                UpdateStateInDB(state);
                Thread.sleep(sleepInterval);
                log.trace("Wainting " + sleepInterval+"ms");
            } catch (InterruptedException e){
                log.error(e);
            }
        }
    }


    public ClusterState getActualClusterState(){
        Long operationStarted = System.currentTimeMillis();
        String[] frameworkKeys = new String[]{"frameworks", "completed_frameworks"};
        String[] taskKeys = new String[]{     "tasks", "completed_tasks"};

        JSONObject json = Utils.getJsonFromUrl("http://" + this.masterHost + ":5050/master/state.json");
        HashMap state = Utils.getHashMapFromJSONObject(json);
        Double started = (Double) state.get("start_time");

        ClusterState clusterState = null;
        if (started != null){
            clusterState = new ClusterState("Mesos@"+this.masterHost, started);

            List<Slave> slaves = new ArrayList<Slave>();
            for (HashMap slavemap : (ArrayList<HashMap>) state.get("slaves")){
                String slid= slavemap.get("pid").toString();
                Slave slave = new Slave(slavemap.get("id").toString(), slid, slavemap.get("hostname").toString());
                //ArrayList<Pair> resources = (HashMap)slavemap.get("resources");

                HashMap resourceMap = (HashMap)slavemap.get("resources");
                List <Pair> resources = new ArrayList<Pair>();
                for (String key : (Set<String>) resourceMap.keySet())
                    if(!resourceMap.get(key).toString().equals("0"))
                        resources.add(new Pair(key, resourceMap.get(key), slid+"-resource-"+key));
                slave.setResources(resources);
                slaves.add(slave);
            }
            clusterState.setSlaves(slaves);

            List<Framework> frameworks = new ArrayList<Framework>();
            for (String frkey : frameworkKeys){
                int frameworkI=0;
                ArrayList<HashMap> frameworkList = (ArrayList<HashMap>) state.get(frkey);
                for (frameworkI=0; frameworkI<frameworkList.size() /*&& frameworkI<2*/; frameworkI++){
                    HashMap frameworkmap = frameworkList.get(frameworkI);
                    String status = (frkey.contains("completed") ? "Completed" : "Active");
                    String frid = frameworkmap.get("id").toString();
                    Framework framework = new Framework(frid,frameworkmap.get("name").toString(), clusterState.getId(),status);
                    HashMap resourceMap = (HashMap)frameworkmap.get("resources");
                    List <Pair> resources = new ArrayList<Pair>();
                    for (String key : (Set<String>) resourceMap.keySet())
                        if(!resourceMap.get(key).toString().equals("0"))
                            resources.add(new Pair(key, resourceMap.get(key), frid+"-resource-"+key));
                    framework.setResources(resources);

                    List<Executor> executors = new ArrayList<Executor>();
                    for (HashMap executormap : (ArrayList<HashMap>) frameworkmap.get("executors")){

                        String execid = executormap.get("id").toString();
                        String slaveId = executormap.get("slave_id").toString();
                        HashMap command = (HashMap)executormap.get("command");
                        //resources = (HashMap)executormap.get("resources");
                        Executor executor = new Executor(execid, slaveId, new TreeMap<>(command), null);
                        executors.add(executor);
                    }
                    framework.setExecutors(executors);

                    List<Task> tasks = new ArrayList<Task>();
                    for (String taskKey : taskKeys){
                        int taskI=0;
                        ArrayList<HashMap> taskList = (ArrayList<HashMap>)frameworkmap.get(taskKey);
                        for (taskI=0; taskI<taskList.size() /*&& taskI<1*/; taskI++){
                            HashMap taskmap = taskList.get(taskI);
                            String slaveId = taskmap.get("slave_id").toString();
                            String tid = frid+"."+taskmap.get("id").toString();
                            Task task = new Task(tid, framework.getId(), slaveId);

                            resourceMap = (HashMap)taskmap.get("resources");
                            resources = new ArrayList<Pair>();
                            for (String key : (Set<String>) resourceMap.keySet())
                                if(!resourceMap.get(key).toString().equals("0"))
                                    resources.add(new Pair(key, resourceMap.get(key), tid+"-resource-"+key));
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
        System.out.println("GetActualClusterState took: "+ (System.currentTimeMillis()-operationStarted)/1000+" seconds");
        return clusterState;
    }

    public ArrayList<String> getStartedClusters(){
        ArrayList<String> ret = new ArrayList<String>();
        FindIterable<Document> res = mongoClient.getDocumentsFromDB("startedClusters", new Document("name", "Mesos"+this.masterHost));
        Iterator keysIter = res.iterator();
        while (keysIter.hasNext()){
            Document resultmap = (Document)keysIter.next();
            ret.add(resultmap.get("started").toString());
        }
        return ret;
    }

    public ClusterState getClusterStateFromDB(){
        List<String> stateIDs = getStartedClusters();
        return getStateFromDB(stateIDs.get(stateIDs.size()-1));
    }

    public ClusterState getStateFromDB(String id /*,time*/){
        System.out.println("Getting cluster state from DB");
        Long operationStarted = System.currentTimeMillis();
        ClusterState ret = null;
        if(useVersioning){
            List<CdoSnapshot> snapshots = javers.findSnapshots(QueryBuilder.byInstanceId(id, ClusterState.class).build());
            if (snapshots.size() > 0){
                CdoSnapshot first = snapshots.get(0);
                MyJaversShapshotsCompiler snapCompiler = new MyJaversShapshotsCompiler(javers);
                ret = (ClusterState) snapCompiler.compileEntityStateFromSnapshot(first);
                System.out.println("Compiling cluster state from DB took: "+ (System.currentTimeMillis()-operationStarted)/1000+" seconds");
                return ret;
            }
        }
        //FindIterable<Document> res = mongoClient.getDocumentsFromDB(new Document("name", "Mesos@"+this.masterHost), "clusterStates");
        List<ClusterState> res = mongoClient.getObjectsFromDB("clusterStates", new BasicDBObject(){{ put("name", "Mesos@"+masterHost); }}, 0, ClusterState.class);
        if(res.size()>0)
            ret = res.get(0);
        System.out.println("Getting cluster state from DB took: "+ (System.currentTimeMillis()-operationStarted)/1000+" seconds");
        return ret;
    }

    public void UpdateStateInDB(ClusterState state){
        System.out.println("Updating state in DB");
        Long operationStarted = System.currentTimeMillis();
        ClusterState prevState = getStateFromDB(state.getStarted().toString());

        if (prevState!=null){
            System.out.println("Comparing prev & curr states");
            Diff diff = javers.compare(prevState, state);
            Diff diff1 = javers.compareCollections(prevState.getSlaves(), state.getSlaves(), Slave.class);
            Diff diff2 = javers.compareCollections(prevState.getAllTasks(), state.getAllTasks(), Task.class);
            if(!diff.hasChanges())
                return;

//            if(useVersioning){
//                saveStateToDB(state);
//                return;
//            }else{  //merge executors & delete state from DB
//
//            }
        }
        saveStateToDB(state);
        System.out.println("Updating state in DB took: "+ (System.currentTimeMillis()-operationStarted)/1000+" seconds");
    }

    public void saveStateToDB(ClusterState state){
        Long operationStarted = System.currentTimeMillis();
        System.out.println("Saving state to DB");


        BasicDBObject find = new BasicDBObject(){{ put("name","Mesos@"+masterHost); put("started", state.getStartedToDate().toString());  }};
        BasicDBObject update = new BasicDBObject(find){{ put("updated", new Date().toString()); }};
        mongoClient.saveObjectToDB("startedClusters", find, update);

        String id = state.getId();
        mongoClient.saveObjectToDB("clusterStates", new BasicDBObject(){{ put("id", id); }}, state);

        //if(useVersioning)
        System.out.println("Commiting to javers");
        javers.commit(id, state);

        System.out.println("Saving state to DB took: "+ (System.currentTimeMillis()-operationStarted)/1000+" seconds");
    }

    public List<ifmo.escience.dapris.common.entities.Node> getNodes(ClusterState state){
        ArrayList<ifmo.escience.dapris.common.entities.Node> ret = new ArrayList<ifmo.escience.dapris.common.entities.Node>();
        for(Slave slave : state.getSlaves()){
            String id = slave.getId();
            String name =  slave.getPid();
            String ip = slave.getHostname();
            String parentNodeId = null;
            String networkId = "networkID";
            Map<String, Object> resourceMap = slave.getResourceMap();
            Double cpuTotal = Double.parseDouble(resourceMap.get("cpus").toString());
            Double memoryTotal = Double.parseDouble(resourceMap.get("mem").toString());
            Double gpuTotal = 0.0;
            ifmo.escience.dapris.common.entities.Node node = new ifmo.escience.dapris.common.entities.Node(id, name, ip, parentNodeId, cpuTotal, memoryTotal, gpuTotal, networkId);
            ret.add(node);
        }
        return ret;
    }

    public List<ifmo.escience.dapris.common.entities.Task> getTasks(ClusterState state){
        List<ifmo.escience.dapris.common.entities.Task> ret = new ArrayList<ifmo.escience.dapris.common.entities.Task>();
        int i=0;
        for (StateStructures.Framework framework : state.getFrameworks()){
            String typeId = framework.getName();
            for (StateStructures.Task task : framework.getTasks()){

                Random rand = new Random(i);
                HashSet<String> parentTaskIds = null;

                Hashtable<String, Double> parameters = new Hashtable<>();
                LocalDateTime started = null;
                LocalDateTime finished = null;
                try {
                    started = LocalDateTime.ofInstant(task.getStarted().toInstant(), ZoneId.systemDefault());
                }
                catch (Exception e){
                    String test="123";
                }
                try {
                    finished = LocalDateTime.ofInstant(task.getFinished().toInstant(), ZoneId.systemDefault());
                }
                catch (Exception e){
                    String test="123";
                }

                String nodeId = task.getSlaveId();
                TaskStatus status = null;
                HashSet<Data> inData = new HashSet<>();
                HashSet<Data> outData = new HashSet<>();

                ifmo.escience.dapris.common.entities.Task addTask = new ifmo.escience.dapris.common.entities.Task(task.getId(), parentTaskIds, typeId, parameters, nodeId, status, started, finished, inData, outData);
                ret.add(addTask);
                i++;
            }
        }
        return ret;
    }

    public void TestJavers(){

//        CustomTreeMap map = new CustomTreeMap("1"){{
//            put("mem", 4480.0);
//            put("cpus", 10.0);
//            put("disk", 0);
//        }};

        List<Pair> list = new ArrayList<Pair>();
        list.add(new Pair("mem", 4480,"mem"));
        list.add(new Pair("cpus",(double)10,"cpus"));
        list.add(new Pair("disk",(double)0,"disk"));

//            HashSet<IdentifiedObject> map = new HashSet<IdentifiedObject>(){{
//                add(new IdentifiedObject("mem", (double) 4480));
//                add(new IdentifiedObject("cpus",(double)10));
//                add(new IdentifiedObject("disk",(double)0));
//        }};

//        javers.commit("1", list);
//        List<CdoSnapshot> snapshots = javers.findSnapshots(QueryBuilder.byInstanceId("1", list.getClass()).build());
//        if (snapshots.size()>0){
//            CdoSnapshot first = snapshots.get(0);
//            JaversShapshotsCompiler snapCompiler = new JaversShapshotsCompiler(javers);
//            List<Pair> ret = (List<Pair>)snapCompiler.compileEntityStateFromSnapshot(first);
//            Diff diff = javers.compareCollections(list, ret, Pair.class);
//            String test ="123";
//        }

//        while(1==1) {
//
//            try {
//                javers.commit("1", map);
//                Thread.sleep(3000);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//        }

    }

    public void TestJavers2(){
        //javers = JaversBuilder.javers().build();

        //javers.commit("author", new Employee("bob", 31) );

        //javers.commit("author", new Employee("john",25) );


        //Diff changes = javers.findChanges( QueryBuilder.byInstanceId("bob", Employee.class).build() );
        //Object obj =  javers.findSnapshots(QueryBuilder.byInstanceId("bob", Employee.class).build());
        //String str = "123";

    }

}
