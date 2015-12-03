import org.apache.commons.httpclient.*;
import org.apache.commons.httpclient.methods.*;
import org.apache.commons.httpclient.params.HttpMethodParams;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.util.*;


public class GangliaMonitor {
    //private LOG = LogFactory.getLog(class$org$apache$commons$httpclient$HttpClient == null?(class$org$apache$commons$httpclient$HttpClient = class$("org.apache.commons.httpclient.HttpClient")):class$org$apache$commons$httpclient$HttpClient);

    //private String baseurl = "http://192.168.92.11:8080/ganglia/api/v1/metrics";
    private String baseurl = "http://192.168.13.133:8080/ganglia/api/v1/metrics";
    private HashMap<String,String> baseparams = new HashMap<String, String>(){{
        put("environment","servers");
        put("service","default");
    }};


    public List<String> requestNames(HashMap<String, String> params, String propName){

          HashMap<String,String> mergedparams = new HashMap<String,String>(baseparams);
          mergedparams.putAll(params);
          String paramString = "";
          for (String key : mergedparams.keySet()){
             if(paramString!="")paramString+="&";
              paramString+=key+"="+mergedparams.get(key);
          }
          String url = baseurl+"?"+paramString;
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
        String url = baseurl+"?"+paramString;

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
            System.out.println("Some error with json reading");
        }
        return ret;
    }

    public JSONObject queryServer(String url){
        System.out.println(url);
        JSONObject ret = null;
        HttpClient client = new HttpClient();
        GetMethod method = new GetMethod(url);

        // Provide custom retry handler is necessary
        method.getParams().setParameter(HttpMethodParams.RETRY_HANDLER,
                new DefaultHttpMethodRetryHandler(3, false));

        try {
            // Execute the method.
            int statusCode = client.executeMethod(method);

            if (statusCode != HttpStatus.SC_OK) {
                System.err.println("Method failed: " + method.getStatusLine());
            }

            // Read the response body.
            byte[] responseBody = method.getResponseBody();
            String res = new String(responseBody);
            //ret = json2.loads(html)
            ret = new JSONObject(res);

        } catch (HttpException e) {
            System.err.println("Fatal protocol violation: " + e.getMessage());
            e.printStackTrace();
        } catch (IOException e) {
            System.err.println("Fatal transport error: " + e.getMessage());
            e.printStackTrace();
        } catch (JSONException e) {
            e.printStackTrace();
        } finally {
            // Release the connection.
            method.releaseConnection();
        }
        return ret;
    }

    public void GetMetricsByCluster(String clusterName, String[] metricNames){
        HashMap<String, String> requestParams = new HashMap<String, String>(){{
            put("cluster", clusterName);  //get any
        }};
        //JSONObject json = requestNames(requestParams, "cluster");
        System.out.println("123");
        //List<String> clusterNames = json.get("response");
//        clusterNames = [metric['cluster'] for metric in json['response']['metrics']]
//        clusters = {}
//        for metric in metrics:
    //        for cluster in clusterNames:

    //        if not clusters.has_key(cluster):
    //        clusters[cluster] = {}
    //        json = requestNames({'cluster': cluster, 'metric': metric})
    //        #metric = [metric['host'] for host in json['response']['metrics']]
    //        time = json['response']['localTime']
    //        for metric_per_host in json['response']['metrics']:
    //        host = metric_per_host['host']
    //        if not clusters[cluster].has_key(host):
    //        clusters[cluster][host] = {}
    //        clusters[cluster][host][metric] = metric_per_host['value']

    }

    public List<String> GetClusterNames(){
        HashMap<String, String> requestParams = new HashMap<String, String>(){{
            put("metric", "cpu_system");  //get any
        }};
        List<String> ret = requestNames(requestParams, "cluster");
        return ret;

  }
    public List<String> GetClusterHosts(String clusterName){
        HashMap<String, String> requestParams = new HashMap<String, String>(){{
            put("metric", "cpu_system");  //get any
            put("cluster", clusterName);
        }};
        List<String> ret = requestNames(requestParams, "host");
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
