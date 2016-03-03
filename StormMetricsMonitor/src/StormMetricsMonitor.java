//package ifmo.escience.dapris.monitoring;
import ifmo.escience.dapris.monitoring.common.CommonMongoClient;
import ifmo.escience.dapris.monitoring.common.Utils;
import org.bson.Document;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.*;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

/**
 * Created by Pavel Smirnov
 */
public class StormMetricsMonitor {
    private static Logger log = LogManager.getLogger(StormMetricsMonitor.class);
    private CommonMongoClient mongoClient;
    private StormAPIClient stormAPIClient;

    public static void main(String[] args){
        StormMetricsMonitor stormMetricsMonitor = new StormMetricsMonitor("http://192.168.92.11:8080");
        //stormMetricsMonitor.getActualData();
        //stormMetricsMonitor.analyzeData();
        if(args.length>0 && args[0].equals("aggregate")) {
            List<Document> stats = stormMetricsMonitor.getActualData();
            stormMetricsMonitor.insertStatsIntoDb(stats);
        }else
            stormMetricsMonitor.startMonitoring(10000);
        //stormMetricsMonitor.InsertDataToDb();
    }

    public StormMetricsMonitor(String base_Url){
        mongoClient = new CommonMongoClient();
        stormAPIClient = new StormAPIClient(base_Url);
    }

    public List<Document> getActualData(){
        log.info("Getting actual data");
        List<Document> ret = new ArrayList<Document>();

        JSONArray topologies = stormAPIClient.getTopologies();
        for(Object topology : topologies){
            String topoID = ((JSONObject) topology).get("id").toString();
            Document doc = mongoClient.getDocumentFromDB("storm.Supervisors", new Document(){{ put("topoID", topoID); }});
            if (doc==null){
                JSONArray supervisors = stormAPIClient.getSupervisors();
                if(supervisors.length()>0){
                    List<Document> supervisorsDocs = Utils.JSON2Doc(supervisors);
                    Document insert = new Document();
                    insert.put("topoID", topoID);
                    insert.put("supervisors", supervisorsDocs);
                    mongoClient.insertDocumentToDB("storm.Supervisors", insert);
                }
             }
            log.info("Getting tupleStats for "+topoID);
            Document topostat = stormAPIClient.getTupleStatsByID(topoID);
            if(topostat!=null)
                ret.add(topostat);
        }
        return ret;
    }


    public void insertStatsIntoDb(List<Document> topoStats){
        log.info("insertStatsIntoDb");
        String collectionName = "storm.statistics";
        mongoClient.open();
        for(Document topoStat : topoStats)
            mongoClient.insertDocumentToDB(collectionName, topoStat);
        mongoClient.close();
    }



    public void startMonitoring(int sleepInterval){
        log.info("Start monitoring");
        while(1==1){
            List<Document> topoStats = getActualData();
            if(topoStats.size()>0)
                insertStatsIntoDb(topoStats);
            log.trace("Sleeping " + sleepInterval+"ms");
            Utils.Wait(sleepInterval);
        }
    }



}

