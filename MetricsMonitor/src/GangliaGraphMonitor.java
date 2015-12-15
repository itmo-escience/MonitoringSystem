import org.json.JSONArray;
import org.json.JSONObject;

import java.util.*;

/**
 *
 * @author Pavel Smirnov
 */
public class GangliaGraphMonitor {

    //private LOG = LogFactory.getLog(class$org$apache$commons$httpclient$HttpClient == null?(class$org$apache$commons$httpclient$HttpClient = class$("org.apache.commons.httpclient.HttpClient")):class$org$apache$commons$httpclient$HttpClient);
    //private String baseurl = "http://192.168.92.11:8080/ganglia/api/v1/metrics";
    private String serverurl;
    private String getbaseurl(){
        return "http://"+serverurl+"/ganglia/graph.php";
    }
    private HashMap<String,String> baseparams = new HashMap<String, String>(){{
        //put("environment","servers");
        //put("service","default");
    }};

    public GangliaGraphMonitor(String serverUrl){
        serverurl = serverUrl;
        //c=ds1&m=load_one
    }

    public List<String> requestItems(HashMap<String, String> params, String propName){

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
            System.out.println("Some error with json reading");
        }
        return ret;
    }

    public HashMap<String, String> requestMetrics(HashMap<String, String> params, Set<String> requiredMetricNames){
        HashMap<String,String> mergedparams = new HashMap<String,String>(baseparams);
        mergedparams.putAll(params);
        String paramString = "";
        for (String key : mergedparams.keySet()){
            if(paramString!="")paramString+="&";
            paramString+=key+"="+mergedparams.get(key);
        }
        String url = getbaseurl()+"?"+paramString;

        JSONObject json = Utils.getJsonFromUrl(url);

        HashMap<String, String> ret = new HashMap<String, String>();
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
            System.out.println("Some error with json reading");
        }
        return ret;
    }

    public List<String> GetClusterNames(){
        HashMap<String, String> requestParams = new HashMap<String, String>(){{
            put("metric", "cpu_system");  //get any
        }};
        List<String> ret = requestItems(requestParams, "cluster");
        return ret;

    }

    public List<String> GetClusterHosts(String clusterName){
        HashMap<String, String> requestParams = new HashMap<String, String>(){{
            put("metric", "cpu_system");  //get any
            put("cluster", clusterName);
        }};
        List<String> ret = requestItems(requestParams, "host");
        return ret;
    }

    public HashMap<String, String> GetMetricsByHost(String hostName, String[] metricNames){
        HashMap<String, String> requestParams = new HashMap<String, String>(){{
            put("host", hostName);
        }};
        Set<String> requiredNames = null;
        if (metricNames!=null)
            requiredNames = new HashSet<String>(Arrays.asList(metricNames));
        HashMap<String, String> ret = requestMetrics(requestParams, requiredNames);
        return ret;
    }
}
