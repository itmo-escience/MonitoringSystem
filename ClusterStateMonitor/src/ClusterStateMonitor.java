import StateStructures.*;
import StateStructures.Task;
import com.mongodb.BasicDBObject;
import com.mongodb.client.FindIterable;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import org.bson.Document;

import org.javers.core.Javers;
import org.javers.core.JaversBuilder;
import org.javers.core.diff.Diff;
import org.javers.core.metamodel.object.CdoSnapshot;
import org.javers.repository.jql.QueryBuilder;
import org.javers.repository.mongo.MongoRepository;

import java.util.*;
/**
 * Created by Pavel Smirnov
 */
public class ClusterStateMonitor {

    private static Logger log = LogManager.getLogger(ClusterStateMonitor.class);
    private Javers javers;
    private int sleepInterval = 3000;
    public boolean useVersioning = false;
    private CommonMongoClient mongoClient;
    private IStateDataProvider dataProvider;

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
        this.dataProvider = new MesosRestClient(masterHost);
        this.mongoClient = mongoClient;
        javers = JaversBuilder.javers().build();
        if(useVersioning){
            MongoRepository mongoRepo = new MongoRepository(mongoClient.getDefaultDB());
            JaversBuilder.javers().registerJaversRepository(mongoRepo).build();
        }
    }

    public void startMonitoring(){
        log.info("Monitoring stated");
        while(1==1){
            try {
                ClusterState state = dataProvider.GetData();
                updateStateInDB(state);
                Thread.sleep(sleepInterval);
                log.trace("Sleeping " + sleepInterval+"ms");
            } catch (InterruptedException e){
                log.error(e);
            }
        }
    }

    public ArrayList<String> getStartedClusters(){
        ArrayList<String> ret = new ArrayList<String>();
        FindIterable<Document> res = mongoClient.getDocumentsFromDB("clusterStates", new Document(), new Document("_id","-1"), 1);
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

    public ClusterState getStateFromDB(String id){
        log.trace("Getting cluster state from DB");
        Long operationStarted = System.currentTimeMillis();
        ClusterState ret = null;
        if(useVersioning){
            List<CdoSnapshot> snapshots = javers.findSnapshots(QueryBuilder.byInstanceId(id, ClusterState.class).build());
            if (snapshots.size() > 0){
                CdoSnapshot first = snapshots.get(0);
                MyJaversShapshotsCompiler snapCompiler = new MyJaversShapshotsCompiler(javers);
                ret = (ClusterState) snapCompiler.compileEntityStateFromSnapshot(first);
                log.trace("Compiling cluster state from DB took: " + (System.currentTimeMillis() - operationStarted) / 1000 + " seconds");
                return ret;
            }
        }
        //FindIterable<Document> res = mongoClient.getDocumentsFromDB(new Document("name", "Mesos@"+this.masterHost), "clusterStates");
        BasicDBObject query = new BasicDBObject(){{ put("id",id); }};
        List<ClusterState> res = mongoClient.getObjectsFromDB("clusterStates", query , 0, ClusterState.class);
        if(res.size()>0)
            ret = res.get(0);
        log.trace("Getting cluster state from DB took: " + (System.currentTimeMillis() - operationStarted) / 1000 + " seconds");
        return ret;
    }

    public void updateStateInDB(ClusterState state){
        log.trace("Updating state in DB");
        mongoClient.open();

        Long operationStarted = System.currentTimeMillis();
        ClusterState prevState = getStateFromDB(state.getId());

        if (prevState!=null){
            log.trace("Comparing prev & curr states");
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
        log.trace("Updating state in DB took: " + (System.currentTimeMillis() - operationStarted) / 1000 + " seconds");
        mongoClient.close();
    }

    public void saveStateToDB(ClusterState state){
        mongoClient.open();
        log.trace("Saving state to DB");
        Long operationStarted = System.currentTimeMillis();

//        BasicDBObject find = new BasicDBObject(){{
//            put("id",state.getId());
//            put("started", state.getStartedAsDate().toString());
//        }};
//        BasicDBObject update = new BasicDBObject(find){{
//            put("updated", new Date().toString());
//        }};
//        mongoClient.saveObjectToDB("startedClusters", find, update);

        mongoClient.saveObjectToDB("clusterStates", new BasicDBObject(){{ put("id", state.getId()); }}, state);

        //if(useVersioning)
        log.trace("Commiting to javers");
        //javers.commit(id, state);
        mongoClient.close();
        log.trace("Saving state to DB took: " + (System.currentTimeMillis() - operationStarted) / 1000 + " seconds");
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
//            List<StateStructures.Pair> ret = (List<StateStructures.Pair>)snapCompiler.compileEntityStateFromSnapshot(first);
//            Diff diff = javers.compareCollections(list, ret, StateStructures.Pair.class);
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
