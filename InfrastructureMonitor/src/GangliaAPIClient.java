package ifmo.escience.dapris.monitoring.infrastructureMonitor;
import ifmo.escience.dapris.monitoring.common.CommonMongoClient;
import ifmo.escience.dapris.monitoring.common.Utils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.*;

/**
 * Created by Pavel Smirnov
 */
public class GangliaAPIClient implements IMetricsDataProvider{

    private static Log log = LogFactory.getLog(GangliaAPIClient.class);
    private String serverurl;
    public CommonMongoClient mongoClient;



    private HashMap<String,String[]> baseparams = new HashMap<String, String[]>(){{
        put("environment",new String[]{"default"});
        put("service", new String[]{"default"});
    }};

    public GangliaAPIClient(String ApiServerUrl, String[] desirableMetrics){
        serverurl = ApiServerUrl;
//        if(desirableMetrics !=null)
//            this.desirableMetrics = desirableMetrics;
        //hostNames = GetClusterHosts(clusterName);
        //coll = db.getCollection("desirableMetrics");
    }

    private String getbaseurl(){
        return "http://"+serverurl+"/ganglia/api/v1/metrics";
    }
    public String createUrl(HashMap<String, String[]> params){
        HashMap<String,String[]> mergedparams = new HashMap<String,String[]>(baseparams);
        mergedparams.putAll(params);
        String paramString = "";
        for (String key : mergedparams.keySet())
            for (String value : mergedparams.get(key)){
                if(paramString!="")paramString+="&";
                paramString+=key+"="+value;
            }
        String url = getbaseurl() +"?"+paramString;
        return url;
    }

    private List<String> requestItems(HashMap<String, String[]> params, String propName){
        log.debug("requestItems()");
        List<String> ret = new ArrayList<String>();

        String url = createUrl(params);
        JSONObject json = Utils.getJsonFromUrl(url);
        try{
                JSONArray items =  ((JSONArray)((JSONObject) json.get("response")).get("metrics"));
                for (int i = 0; i < items.length(); i++) {
                    JSONObject ob = items.getJSONObject(i);
                    if(!ret.contains(ob.get(propName).toString()))
                        ret.add(ob.get(propName).toString());
                }
        }
        catch (Exception e){
              log.error("Some error with json reading", e);
        }
        return ret;
    }

    public TreeMap<String, TreeMap<String, Object>> requestMetrics(HashMap<String, String[]> params, Set<String> requiredMetricNames){
        log.debug("requestMetrics()");
        TreeMap<String, TreeMap<String, Object>> ret = new TreeMap<String, TreeMap<String, Object>>();
        String url = createUrl(params);
        JSONObject json = Utils.getJsonFromUrl(url);
        try{
            JSONArray metrics =  ((JSONArray)((JSONObject) json.get("response")).get("metrics"));
            for (int i = 0; i < metrics.length(); i++){
                JSONObject metric = metrics.getJSONObject(i);
                String hostname = metric.get("host").toString();
                String name = metric.get("metric").toString();
                String value = metric.get("value").toString();

                if (requiredMetricNames==null || (requiredMetricNames!=null && requiredMetricNames.contains(name))){
                    if(!ret.keySet().contains(hostname))
                        ret.put(hostname, new TreeMap<String, Object>());
                    TreeMap<String, Object> metricsPerHost = ret.get(hostname);
                    metricsPerHost.put(name, value);
                }
            }
        }
        catch (Exception e){
            log.error("Some error with json reading", e);
        }
        return ret;
    }

//    public TreeMap<String, Object> requestMetrics0(HashMap<String, String> params, Set<String> requiredMetricNames){
//        log.debug("requestMetrics()");
//        TreeMap<String, Object> ret = new TreeMap<String, Object>();
////        HashMap<String,String> mergedparams = new HashMap<String,String>(baseparams);
////        mergedparams.putAll(params);
////        String paramString = "";
////        for (String key : mergedparams.keySet()){
////            if(paramString!="")paramString+="&";
////            paramString+=key+"="+mergedparams.get(key);
////        }
////        String url = getbaseurl()+"?"+paramString;
////        JSONObject json = Utils.getJsonFromUrl(url);
////        try{
////            JSONArray metrics =  ((JSONArray)((JSONObject) json.get("response")).get("metrics"));
////            for (int i = 0; i < metrics.length(); i++){
////                JSONObject metric = metrics.getJSONObject(i);
////                String name = metric.get("metric").toString();
////                String value = metric.get("value").toString();
////                if (requiredMetricNames!=null){
////                   if (requiredMetricNames.contains(name))
////                       ret.put(name, value);
////                }else
////                    ret.put(name, value);
////             }
////        }
////        catch (Exception e){
////            log.error("Some error with json reading", e);
////        }
//        return ret;
//    }

    public List<String> GetClusterNames(){
        log.debug("Getting cluster names");
        HashMap<String, String[]> requestParams = new HashMap<String, String[]>(){{
            put("metric", new String[]{"heartbeat"});  //get any
        }};
        List<String> ret = requestItems(requestParams, "cluster");
        return ret;
    }

    public List<String> GetClusterHosts(String clusterName){
        log.debug("Getting hosts for cluster " + clusterName);
        HashMap<String, String[]> requestParams = new HashMap<String, String[]>(){{
            put("metric", new String[]{ "heartbeat"});
            put("cluster",new String[]{ clusterName});
        }};
        List<String> ret = requestItems(requestParams, "host");
        return ret;
    }

    public TreeMap<String, TreeMap<String, Object>> GetData(String clusterName, String[] metricNames){
        log.debug("Getting desirableMetrics for " + clusterName);

        HashMap<String, String[]> requestParams = new HashMap<String, String[]>(){{
            put("cluster", new String[]{ clusterName });
            put("metric", metricNames);
        }};

        Set<String> requiredNames = null;
        if (metricNames!=null)
            requiredNames = new HashSet<String>(Arrays.asList(metricNames));
        TreeMap<String, TreeMap<String, Object>> ret = requestMetrics(requestParams, requiredNames);
        return ret;
    }

//    public TreeMap<String, Object> GetMetricsByHost(String hostName, String[] metricNames){
//        log.debug("Getting desirableMetrics for " + hostName);
//
//        HashMap<String, String> requestParams = new HashMap<String, String>(){{
//            put("host", hostName);
//        }};
//        Set<String> requiredNames = null;
//        if (metricNames!=null)
//            requiredNames = new HashSet<String>(Arrays.asList(metricNames));
//        TreeMap<String, Object> ret = requestMetrics(requestParams, requiredNames);
//        return ret;
//    }

}
