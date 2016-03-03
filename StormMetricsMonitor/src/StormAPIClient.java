import ifmo.escience.dapris.monitoring.common.Utils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.json.JSONArray;
import org.json.JSONObject;
import org.omg.CORBA.portable.Delegate;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * Created by Pavel Smirnov
 */
public class StormAPIClient {
    private static Logger log = LogManager.getLogger(StormAPIClient.class);
    public String baseUrl = "http://192.168.92.11:8080";

    public StormAPIClient(String base_Url){
        baseUrl = base_Url.replace("/api/v1","")+"/api/v1/";
    }

    public List<String> getSupervisorIDs(){

        List<String> ret = new ArrayList<String>();
        for(Object topology : getSupervisors()){
            String id = ((JSONObject) topology).get("id").toString();
            ret.add(id);
        }

        return ret;
    }

    public JSONArray getSupervisors(){
        log.info("Waiting for supervisors list");
        JSONArray supervisors = Utils.WaitForJSON(baseUrl + "supervisor/summary", "supervisors");
        return supervisors;
    }


    public JSONArray getTopologies(){
        log.trace("Waiting for topologies list");
        JSONArray topologies = Utils.WaitForJSON(baseUrl + "topology/summary", "topologies");
        return topologies;
    }

    public JSONObject getTopologyByName(String topoName, Boolean resubmit){
        log.trace("Getting topology by name");
        JSONObject ret=null;
        Integer wait = 5000;
        int i = 0;
        while(ret==null){
            try{

                JSONArray topologies = getTopologies();
                for (Object topology : topologies){
                    if(((JSONObject)topology).get("name").toString().equals(topoName) && ((JSONObject)topology).get("status").toString().equals("ACTIVE")){
                        ret =  (JSONObject)topology;
                        wait=1;
                        break;
                    }
                }
//
//                if(i>0 && i%18==i/18 && topologies.length()==0 && resubmit){
//                    log.info("Try to resubmit");
//                    submitTopology(topoName, paramString);
//                }

            }
            catch (Exception e){

            }
            Utils.Wait(wait);
            i++;
        }
        return ret;
    }

    public JSONObject getTopologyStats(String topoID){
        JSONObject json = Utils.getJsonFromUrl(baseUrl + "topology/"+topoID);
        return json;
    }

    public Document getTupleStatsByID(String topoID){
        log.trace("Getting tupleStats for "+topoID);
        JSONObject json = getTopologyStats(topoID);
        if(json!=null) {
            String uptimeStr = json.get("uptime").toString();

            JSONArray topologyStats = ((JSONArray) json.get("topologyStats"));
            JSONObject topologyStatsAlltime = (JSONObject) topologyStats.get(topologyStats.length() - 1);
            Document topoStats = ReturnValuesByKeys(topologyStatsAlltime);
            if (!topoStats.isEmpty()) {
                List<Document> components = new ArrayList<Document>();
                for (String compType : new String[]{"spouts", "bolts"})
                    for (Object component : (JSONArray) json.get(compType)) {
                        Document compStat = ReturnValuesByKeys(component);
                        if (!compStat.isEmpty())
                            components.add(compStat);
                    }
                if (!components.isEmpty()) {
                    topoStats.put("name", "Topology");
                    topoStats.put("topoID", topoID);
                    topoStats.put("uptime", uptimeStr);
                    topoStats.put("components", components);
                }
                return topoStats;
            }
        }
        return null;
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


}
