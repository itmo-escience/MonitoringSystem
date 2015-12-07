import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.apache.commons.httpclient.*;
import org.apache.commons.httpclient.methods.*;
import org.apache.commons.httpclient.params.HttpMethodParams;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.bson.Document;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.util.*;


public class GangliaAPIMonitor{
    private static Log logger = LogFactory.getLog(GangliaAPIMonitor.class);

    private int sleepInterval=3000;
    private String[] metrics;
    private String serverurl;
    private MongoDatabase db;
    private MongoCollection coll;

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
        MongoClient mongoClient = new MongoClient( "192.168.13.133" , 27017 );
        db = mongoClient.getDatabase("logging");
        coll = db.getCollection("metrics");
    }

    public void StartMonitoring(){
        while(1==1){
            try {
                List<String> clusterNames = GetClusterNames();
                for (String clusterName : clusterNames) {
                    List<String> hostNames = GetClusterHosts(clusterName);
                    for (String hostName : hostNames){
                        HashMap<String, String> hostMetrics = GetMetricsByHost(hostName, metrics);
                        if(hostMetrics.keySet().size()>0){
                            Document doc = new Document("host", hostName);
                            doc.append("time", new Date());
                            for (String key : hostMetrics.keySet())
                                doc.append(key, hostMetrics.get(key));
                            coll.insertOne(doc);
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
          JSONObject json = queryServer(url);

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

    public HashMap<String, String> requestMetrics(HashMap<String, String> params, Set<String> requiredMetricNames) {
        logger.debug("requestMetrics()");
        HashMap<String,String> mergedparams = new HashMap<String,String>(baseparams);
        mergedparams.putAll(params);
        String paramString = "";
        for (String key : mergedparams.keySet()){
            if(paramString!="")paramString+="&";
            paramString+=key+"="+mergedparams.get(key);
        }
        String url = getbaseurl()+"?"+paramString;
        JSONObject json = queryServer(url);
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
            logger.error("Some error with json reading", e);
        }
        return ret;
    }

    public JSONObject queryServer(String url){
        logger.debug("Requesting "+url);

        JSONObject ret = null;
        HttpClient client = new HttpClient();
        GetMethod method = new GetMethod(url);
        method.getParams().setParameter(HttpMethodParams.RETRY_HANDLER, new DefaultHttpMethodRetryHandler(3, false));

        try {
            int statusCode = client.executeMethod(method);

            if (statusCode != HttpStatus.SC_OK) {
                logger.error("Method failed: " + method.getStatusLine());
            }
            byte[] responseBody = method.getResponseBody();
            String res = new String(responseBody);
            ret = new JSONObject(res);

        } catch (HttpException e) {
            logger.error("HttpException: ", e);
        } catch (IOException e) {
            logger.error("IOException: ", e);
        } catch (JSONException e) {
            logger.error("JSONException: ", e);
        } finally {
            // Release the connection.
            method.releaseConnection();
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

    public HashMap<String, String> GetMetricsByHost(String hostName, String[] metricNames){
        logger.debug("Getting metrics for "+hostName);

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
