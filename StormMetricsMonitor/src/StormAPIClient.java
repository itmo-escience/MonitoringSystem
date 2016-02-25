import ifmo.escience.dapris.monitoring.common.Utils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.json.JSONArray;
import org.json.JSONObject;
import org.omg.CORBA.portable.Delegate;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Pavel Smirnov
 */
public class StormAPIClient {
    private static Logger log = LogManager.getLogger(StormAPIClient.class);
    public String baseUrl = "http://192.168.92.11:8080";

    public StormAPIClient(String base_Url){
        baseUrl = base_Url.replace("/api/v1","")+"/api/v1";
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
        JSONArray supervisors = Utils.WaitForJSON(baseUrl + "/supervisor/summary", "supervisors");
        return supervisors;
    }


    public JSONArray getTopologies(){
        log.trace("Waiting for topologies list");
        JSONArray topologies = Utils.WaitForJSON(baseUrl + "/topology/summary", "topologies");
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
            Wait(wait);
            i++;
        }
        return ret;
    }

    public JSONObject getTopologyStats(String topoID){
        JSONObject json = Utils.getJsonFromUrl(baseUrl + "/topology/"+topoID);
        return json;
    }


    public void Wait(int milliseconds){
        try {
            Thread.sleep(milliseconds);
        }
        catch (InterruptedException e2){

        }
    }
}
