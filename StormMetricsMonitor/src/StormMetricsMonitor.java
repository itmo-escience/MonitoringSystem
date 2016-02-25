//package ifmo.escience.dapris.monitoring;
import ifmo.escience.dapris.monitoring.common.CommonMongoClient;
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
        if(args.length>0 && args[0].equals("aggregate"))
            stormMetricsMonitor.getActualData();
        else
            stormMetricsMonitor.startMonitoring(10000);
        //stormMetricsMonitor.InsertDataToDb();
    }

    public StormMetricsMonitor(String base_Url){
        mongoClient = new CommonMongoClient();
        stormAPIClient = new StormAPIClient(base_Url);
    }

    public List<Document> getActualData(){
        log.trace("Getting actual data");
        List<Document> ret = new ArrayList<Document>();

        JSONArray topologies = stormAPIClient.getTopologies();
        for(Object topology : topologies){
            String topoID = ((JSONObject) topology).get("id").toString();
            Document doc = mongoClient.getDocumentFromDB("storm.Supervisors", new Document(){{ put("topoID", topoID); }});
            if (doc==null){
                JSONArray supervisors = stormAPIClient.getSupervisors();
                if(supervisors.length()>0){
                    ArrayList<Document> supervisorsDocs = new ArrayList<Document>();
                    for(Object supervisor : supervisors){
                        Document  supervisorsDoc = new Document();
                        for(String key : ((JSONObject)supervisor).keySet()){
                            supervisorsDoc.put(key,((JSONObject)supervisor).get(key));
                        }
                        supervisorsDocs.add(supervisorsDoc);
                    }
                    Document insert = new Document();
                    insert.put("topoID", topoID);
                    insert.put("supervisors", supervisorsDocs);
                    mongoClient.insertDocumentToDB("storm.Supervisors", insert);
                }

            }
            Document topostat = GetTopoStatsByID(topoID);
            if(topostat!=null)
                ret.add(topostat);
        }
        return ret;
    }

    public Document GetTopoStatsByID(String topoID){

        JSONObject json = stormAPIClient.getTopologyStats(topoID);
        String uptimeStr = json.get("uptime").toString();

        JSONArray topologyStats = ((JSONArray) json.get("topologyStats"));
        JSONObject topologyStatsAlltime = (JSONObject) topologyStats.get(topologyStats.length() - 1);
        Document topoStats = ReturnValuesByKeys(topologyStatsAlltime);
        if(!topoStats.isEmpty()){
            List<Document> components = new ArrayList<Document>();
            for (String compType : new String[]{"spouts", "bolts"})
                for (Object component : (JSONArray) json.get(compType)){
                    Document compStat = ReturnValuesByKeys(component);
                    if(!compStat.isEmpty())
                        components.add(compStat);
                }
            if(!components.isEmpty()){
                topoStats.put("name", "Topology");
                topoStats.put("topoID", topoID);
                topoStats.put("uptime", uptimeStr);
                topoStats.put("components", components);
            }
            return topoStats;
        }
        return null;
    }

    public void InsertStatsIntoDb(List<Document> topoStats){
        String collectionName = "storm.statistics";
        mongoClient.open();
        for(Document topoStat : topoStats)
            mongoClient.insertDocumentToDB(collectionName, topoStat);
        mongoClient.close();
    }

    public static Document ReturnValuesByKeys(Object item){
        String[] keysTransfer = {"emitted","executed","transferred","acked"};
        String[] keysOther = {"boltId","spoutId", "completeLatency", "processLatency", "executeLatency", "capacity"};

        Date timestamp = new Date();
        Document comp = new Document();
        for (String key : keysTransfer)
            if (((JSONObject) item).keySet().contains(key)) {
                Object value = ((JSONObject) item).get(key);
                if (value != null && !value.toString().equals("null"))
                    comp.put(key, value);
            }
        if(comp.keySet().size()>0){
            for (String key2 : keysOther)
                if (((JSONObject) item).keySet().contains(key2)) {
                    String key = key2;
                    Object value = ((JSONObject) item).get(key);
                    if (key == "spoutId" || key == "boltId" ) key = "name";
                    if (value != null)
                        comp.put(key, value);
                }
            comp.put("timestamp", timestamp);
        }
        return comp;
    }


    public void InsertDataToDb(){

        Random rand = new Random();
        Date timestamp = new Date();
        int prevRiskLevel=0;
        mongoClient.open();
        int i=0;
        while(i<50) {
            int riskLevel = rand.nextInt(30 + 1) + 30;
            Document ins = new Document();
            ins.put("DataID", "Patient_1_Fragment_"+String.valueOf(i));

            ins.put("ModelName", (prevRiskLevel>50? "pp_w-75s_fft_mag" : "pp_w-75s_freq-corr-1-None"));

            ins.put("Parameters", new ArrayList());
            ins.put("RiskLevel", String.valueOf(riskLevel));
            ins.put("DateStart", timestamp);
            ins.put("DateFinish", timestamp);
            prevRiskLevel = riskLevel;
            mongoClient.insertDocumentToDB("seizurePredictionResults", ins);
            if(prevRiskLevel<50)
                i++;
        }
        mongoClient.close();
    }

    public void startMonitoring(int sleepInterval){
        log.info("Monitoring starting");
        while(1==1){
            try {
                getActualData();
                log.trace("Sleeping " + sleepInterval+"ms");
                Thread.sleep(sleepInterval);
            } catch (InterruptedException e){
                log.error(e);
            }
        }
    }



}

