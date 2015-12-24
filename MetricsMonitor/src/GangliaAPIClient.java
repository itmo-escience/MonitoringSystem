import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.*;

/**
 * Created by Pavel Smirnov
 */
public class GangliaAPIClient {

    private static Log log = LogFactory.getLog(GangliaAPIClient.class);
    private int sleepInterval = 3000;
    private String[] desirableMetrics;
    private String serverurl;
    public CommonMongoClient mongoClient;

    private String getbaseurl(){
        return "http://"+serverurl+"/ganglia/api/v1/metrics";
    }

    private HashMap<String,String> baseparams = new HashMap<String, String>(){{
        put("environment","servers");
        put("service","default");
    }};

    public GangliaAPIClient(String ApiServerUrl, String[] desirableMetrics){
        serverurl = ApiServerUrl;
        if(desirableMetrics !=null)
            this.desirableMetrics = desirableMetrics;

        //coll = db.getCollection("desirableMetrics");
    }

    public TreeMap<String, TreeMap<String, Object>> GetActualState(){
        TreeMap<String, TreeMap<String, Object>> ret = new TreeMap<String, TreeMap<String, Object>>();
        List<String> clusterNames = GetClusterNames();
        for (String clusterName : clusterNames) {
            List<String> hostNames = GetClusterHosts(clusterName);
            for (String hostName : hostNames){
                TreeMap<String, Object> hostMetrics = GetMetricsByHost(hostName, desirableMetrics);
                if (hostMetrics.keySet().size() > 0)
                    ret.put(hostName, hostMetrics);
            }
        }
        return ret;
    }

    public List<String> requestItems(HashMap<String, String> params, String propName){
          log.debug("requestItems()");
          HashMap<String,String> mergedparams = new HashMap<String,String>(baseparams);
          mergedparams.putAll(params);
          String paramString = "";
          for (String key : mergedparams.keySet()){
             if(paramString!="")paramString+="&";
              paramString+=key+"="+mergedparams.get(key);
          }
          String url = getbaseurl() +"?"+paramString;
          JSONObject json = Utils.getJsonFromUrl(url);

          List<String> ret = new ArrayList<String>();
          try{
                JSONArray items =  ((JSONArray)((JSONObject) json.get("response")).get("metrics"));
                for (int i = 0; i < items.length(); i++) {
                    JSONObject ob = items.getJSONObject(i);
                    ret.add(ob.get(propName).toString());
                }
          }
          catch (Exception e){
              log.error("Some error with json reading", e);
          }
        return ret;
    }

    public TreeMap<String, Object> requestMetrics(HashMap<String, String> params, Set<String> requiredMetricNames) {
        log.debug("requestMetrics()");
        TreeMap<String, Object> ret = new TreeMap<String, Object>();
        HashMap<String,String> mergedparams = new HashMap<String,String>(baseparams);
        mergedparams.putAll(params);
        String paramString = "";
        for (String key : mergedparams.keySet()){
            if(paramString!="")paramString+="&";
            paramString+=key+"="+mergedparams.get(key);
        }
        String url = getbaseurl()+"?"+paramString;
        JSONObject json = Utils.getJsonFromUrl(url);
        try{
            JSONArray metrics =  ((JSONArray)((JSONObject) json.get("response")).get("metrics"));
            for (int i = 0; i < metrics.length(); i++){
                JSONObject metric = metrics.getJSONObject(i);
                String name = metric.get("metric").toString();
                String value = metric.get("value").toString();
                if (requiredMetricNames!=null){
                   if (requiredMetricNames.contains(name))
                       ret.put(name, value);
                }else
                    ret.put(name, value);
             }
        }
        catch (Exception e){
            log.error("Some error with json reading", e);
        }
        return ret;
    }

    public List<String> GetClusterNames(){
        log.debug("Getting cluster names");
        HashMap<String, String> requestParams = new HashMap<String, String>(){{
            put("metric", "cpu_system");  //get any
        }};
        List<String> ret = requestItems(requestParams, "cluster");
        return ret;
    }

    public List<String> GetClusterHosts(String clusterName){
        log.debug("Getting hosts for cluster " + clusterName);
        HashMap<String, String> requestParams = new HashMap<String, String>(){{
            put("metric", "heartbeat");
            put("cluster", clusterName);
        }};
        List<String> ret = requestItems(requestParams, "host");
        return ret;
    }

    public TreeMap<String, Object> GetMetricsByHost(String hostName, String[] metricNames){
        log.debug("Getting desirableMetrics for " + hostName);

        HashMap<String, String> requestParams = new HashMap<String, String>(){{
            put("host", hostName);
        }};
        Set<String> requiredNames = null;
        if (metricNames!=null)
            requiredNames = new HashSet<String>(Arrays.asList(metricNames));
        TreeMap<String, Object> ret = requestMetrics(requestParams, requiredNames);
        return ret;
    }

}
