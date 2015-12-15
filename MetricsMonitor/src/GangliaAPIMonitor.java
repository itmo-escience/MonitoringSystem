import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.bson.Document;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.*;

/**
 * Created by Pavel Smirnov
 */
public class GangliaAPIMonitor{

    private static Log logger = LogFactory.getLog(GangliaAPIMonitor.class);
    private int sleepInterval = 3000;
    private String[] metrics;
    private String serverurl;
    public CommonMongoClient mongoClient;

    private String getbaseurl(){
        return "http://"+serverurl+":8080/ganglia/api/v1/metrics";
    }

    private HashMap<String,String> baseparams = new HashMap<String, String>(){{
        put("environment","servers");
        put("service","default");
    }};

    public GangliaAPIMonitor(String ApiServerUrl, String[] Metrics){
        serverurl = ApiServerUrl;
        if(Metrics!=null)
            metrics=Metrics;

        //coll = db.getCollection("metrics");
    }

    public void StartMonitoring(){
        while(1==1){
            try {
                List<String> clusterNames = GetClusterNames();
                for (String clusterName : clusterNames) {
                    List<String> hostNames = GetClusterHosts(clusterName);
                    for (String hostName : hostNames){
                        TreeMap<String, String> hostMetrics = GetMetricsByHost(hostName, metrics);

                        if(hostMetrics.keySet().size()>0){
                            Document metricsEntry = new Document();
                            metricsEntry.append("timestamp", new Date());
                            metricsEntry.append("hostname",  hostName);
                            metricsEntry.append("metrics",  hostMetrics);
                            mongoClient.saveDocumentToDB(metricsEntry, "metrics");
                        }
                    }
                }
                Thread.sleep(sleepInterval);
            } catch (InterruptedException e) {
                logger.error(e);
            }
        }
    }

    public List<String> requestItems(HashMap<String, String> params, String propName){
          logger.debug("requestItems()");
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
              logger.error("Some error with json reading", e);
          }
        return ret;
    }

    public TreeMap<String, String> requestMetrics(HashMap<String, String> params, Set<String> requiredMetricNames) {
        logger.debug("requestMetrics()");
        HashMap<String,String> mergedparams = new HashMap<String,String>(baseparams);
        mergedparams.putAll(params);
        String paramString = "";
        for (String key : mergedparams.keySet()){
            if(paramString!="")paramString+="&";
            paramString+=key+"="+mergedparams.get(key);
        }
        String url = getbaseurl()+"?"+paramString;
        JSONObject json = Utils.getJsonFromUrl(url);
        TreeMap<String, String> ret = new TreeMap<String, String>();
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
            logger.error("Some error with json reading", e);
        }
        return ret;
    }

    public List<String> GetClusterNames(){
        logger.debug("Getting cluster names");
        HashMap<String, String> requestParams = new HashMap<String, String>(){{
            put("metric", "cpu_system");  //get any
        }};
        List<String> ret = requestItems(requestParams, "cluster");
        return ret;
    }

    public List<String> GetClusterHosts(String clusterName){
        logger.debug("Getting hosts for cluster "+clusterName);
        HashMap<String, String> requestParams = new HashMap<String, String>(){{
            put("metric", "heartbeat");
            put("cluster", clusterName);
        }};
        List<String> ret = requestItems(requestParams, "host");
        return ret;
    }

    public TreeMap<String, String> GetMetricsByHost(String hostName, String[] metricNames){
        logger.debug("Getting metrics for "+hostName);

        HashMap<String, String> requestParams = new HashMap<String, String>(){{
            put("host", hostName);
        }};
        Set<String> requiredNames = null;
        if (metricNames!=null)
            requiredNames = new HashSet<String>(Arrays.asList(metricNames));
        TreeMap<String, String> ret = requestMetrics(requestParams, requiredNames);
        return ret;
    }

}
