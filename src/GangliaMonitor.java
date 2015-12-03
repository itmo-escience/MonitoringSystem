import org.apache.commons.httpclient.*;
import org.apache.commons.httpclient.methods.*;
import org.apache.commons.httpclient.params.HttpMethodParams;
import org.apache.commons.logging.LogFactory;
import org.json.JSONObject;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;


public class GangliaMonitor {
    //private LOG = LogFactory.getLog(class$org$apache$commons$httpclient$HttpClient == null?(class$org$apache$commons$httpclient$HttpClient = class$("org.apache.commons.httpclient.HttpClient")):class$org$apache$commons$httpclient$HttpClient);

    private String baseurl = "http://192.168.92.11:8080/ganglia/api/v1/metrics";
    private HashMap<String,String> baseparams = new HashMap<String, String>(){{
        put("environment","servers");
        put("service","default");
    }};

    private List<String> metrics = Arrays.asList("cpu_system", "mem_total", "disk_total", "network_total");
    public JSONObject requestJson(HashMap<String, String> params){

          HashMap<String,String> mergedparams = new HashMap<String,String>(baseparams);
          mergedparams.putAll(params);
          String paramString = "";
          for (String key : mergedparams.keySet()){
             if(paramString!="")paramString+="&";
              paramString+=key+"="+mergedparams.get(key);
          }
          String url = baseurl+"?"+paramString;
          queryServer(url);
//        response = urllib2.urlopen(url)
//        html = response.read()
//        ret = json2.loads(html)
          return null;
    }

    public void queryServer(String url){

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

            // Deal with the response.
            // Use caution: ensure correct character encoding and is not binary data
            System.out.println(new String(responseBody));

        } catch (HttpException e) {
            System.err.println("Fatal protocol violation: " + e.getMessage());
            e.printStackTrace();
        } catch (IOException e) {
            System.err.println("Fatal transport error: " + e.getMessage());
            e.printStackTrace();
        } finally {
            // Release the connection.
            method.releaseConnection();
        }
    }

    public void GetCurrentMetrics(){
          HashMap<String, String> defaultMap = new HashMap<String, String>();
          defaultMap.put("metric", metrics.get(0));

          JSONObject json = requestJson(defaultMap);
          System.out.println("123");
          //List<String> clusterNames = json.get("response");
//        clusterNames = [metric['cluster'] for metric in json['response']['metrics']]
//        clusters = {}
//        for metric in metrics:
//        for cluster in clusterNames:
//        if not clusters.has_key(cluster):
//        clusters[cluster] = {}
//        json = requestJson({'cluster': cluster, 'metric': metric})
//        #metric = [metric['host'] for host in json['response']['metrics']]
//        time = json['response']['localTime']
//        for metric_per_host in json['response']['metrics']:
//        host = metric_per_host['host']
//        if not clusters[cluster].has_key(host):
//        clusters[cluster][host] = {}
//        clusters[cluster][host][metric] = metric_per_host['value']

    }
}
