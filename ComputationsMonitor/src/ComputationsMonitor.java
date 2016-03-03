package ifmo.escience.dapris.monitoring.computationsMonitor;

import com.mongodb.BasicDBObject;
import ifmo.escience.dapris.monitoring.computationsMonitor.StateStructures.*;
import ifmo.escience.dapris.monitoring.common.CommonMongoClient;
import ifmo.escience.dapris.monitoring.common.Utils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
public class ComputationsMonitor {

    public boolean useVersioning = false;
    private static Logger log = LogManager.getLogger(ComputationsMonitor.class);
    private Javers javers;
    private CommonMongoClient mongoClient;
    private String configFileName = "ComputationsMonitor.config";
    private String masterHost = "192.168.13.133";
    private IStateDataProvider dataProvider;

    public static void main(String[] args){
        //Log log2 = LogFactory.getLog(ClusterStateMonitor.class);
        ComputationsMonitor monitor = new ComputationsMonitor(new CommonMongoClient());

//        if (args!=null  && args.length>0 && args[0].equals("getData")){
//            ArrayList<String> dates = monitor.getStartedClusters();
//            String stateID = dates.get(dates.size()-1);
//            monitor.getStateFromDB(stateID);
//        }else{
          int sleepInterval = 3000;
          //if (args!=null  && args.length>2 && args[0].equals("getData"))
          HashMap<String, String> argsList = new HashMap<String, String>();
          for(int i=0; i<args.length; i++){
              if(args[i].startsWith(("-"))){
                  argsList.put(args[i], args[i+1]);
                  i++;
              }
              i++;
          }
        if(argsList.containsKey("-sleepInterval"))
            sleepInterval = Integer.parseInt(argsList.get("-sleepInterval"));
        monitor.startMonitoring(sleepInterval);


    }

    public ComputationsMonitor(CommonMongoClient mongoClient){
        readConfigFile();
        this.dataProvider = new MesosRestClient(masterHost);
        this.mongoClient = mongoClient;
        javers = JaversBuilder.javers().build();
        if(useVersioning){
            MongoRepository mongoRepo = new MongoRepository(mongoClient.getDefaultDB());
            JaversBuilder.javers().registerJaversRepository(mongoRepo).build();
        }
    }

    private void readConfigFile(){
        try{
            List<String> lines = Utils.ReadConfigFile(configFileName);
            masterHost = lines.get(0);
        }
        catch (Exception e){
            log.info("Cannot read external config. Using default values");
        }
    }

    public void startMonitoring(int sleepInterval){
        log.info("Monitoring starting");
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
        List<Document> res = mongoClient.getDocumentsFromDB("clusterStates", new Document(), new Document("_id", -1), 1);

        for(Document resultmap : res){

            ret.add(resultmap.get("id").toString());
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
//                MyJaversShapshotsCompiler snapCompiler = new MyJaversShapshotsCompiler(javers);
//                ret = (ClusterState) snapCompiler.compileEntityStateFromSnapshot(first);
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
        mongoClient.saveObjectToDB("clusterStates", new BasicDBObject(){{ put("id", state.getId()); }}, state);

        if(useVersioning){
            log.trace("Commiting to javers");
            javers.commit(state.getId(), state);
        }
        mongoClient.close();
        log.trace("Saving state to DB took: " + (System.currentTimeMillis() - operationStarted) / 1000 + " seconds");
    }




}
