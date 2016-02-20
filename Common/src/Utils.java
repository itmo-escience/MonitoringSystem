package ifmo.escience.dapris.monitoring.common;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.httpclient.params.HttpMethodParams;
import org.apache.commons.httpclient.DefaultHttpMethodRetryHandler;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.io.IOUtils;
//import org.apache.http.client.HttpClient;
//import org.apache.log4j.Logger;
//import org.apache.logging.log4j.LogManager;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.*;
import java.nio.charset.Charset;
import java.util.*;

import org.apache.logging.log4j.LogManager;

/**
 *
 * @author Pavel Smirnov
 */
public class Utils {
    private static org.apache.logging.log4j.Logger log = LogManager.getLogger(Utils.class);

    public static JSONObject getJsonFromUrl(String url){
        //System.out.print("Requesting " + url);
        log.trace("Requesting " + url);

        JSONObject ret = null;

        //RequestConfig requestConfig = RequestConfig.custom().setConnectTimeout(30 * 1000).build();
        //HttpClient client = HttpClientBuilder.create().setDefaultRequestConfig(requestConfig).build();

        HttpClient client = new HttpClient();
        GetMethod method = new GetMethod(url);
        method.getParams().setParameter(HttpMethodParams.RETRY_HANDLER, new DefaultHttpMethodRetryHandler(1, false));
        method.getParams().setParameter(HttpMethodParams.SO_TIMEOUT, 3000);
        //while (ret==null){
            try {
                int statusCode = client.executeMethod(method);
                if (statusCode != HttpStatus.SC_OK){
                    throw new HttpException("Method failed: " + method.getStatusLine());
                }
                InputStream stream = method.getResponseBodyAsStream();
                byte[] responseBody = IOUtils.toByteArray(stream);
                stream.close();
                String res = new String(responseBody);
                ret = new JSONObject(res);
            } catch (HttpException e) {
                //log.error("HttpException: ", e);
                //Sleep(1000);
            } catch (IOException e) {
                //log.error("IOException: ", e.getMessage());
                //Sleep(1000);
            }
            catch (JSONException e) {
                log.error("JSONException: ", e);
                //Sleep(1000);
            }
            catch (Throwable throwable) {
                log.error("JSONException: ", throwable);
            } finally {
                method.releaseConnection();
            }
        //}
        return ret;
    }

    public static void Sleep(int interval){
        try {
            Thread.sleep(interval);
        } catch (InterruptedException e) {
            //log.error(e);
        }
    }

    public static void WriteToFile(String filename, String str){
        try {
            FileWriter fw = new FileWriter(filename, true); //the true will append the new data
            fw.write(str + "\n");
            fw.close();

        } catch (IOException e) {
            log.error(e.getMessage());
        }
    }

    public static HashMap<String, Object> getHashMapFromJSONObject(JSONObject object){
        HashMap<String, Object> ret = new HashMap<String, Object>();
        Iterator keysIter = object.keys();
        while (keysIter.hasNext()){
            //System.out.println(flavoursIter.next());
            String key = (String) keysIter.next();
            Object value = object.opt(key);
            value = ConvertUnknownJSON(value);
            ret.put(key, value);
        }
        return ret;
    }

    public static ArrayList<Object> getArrayListFromJSONObject(Object object){
        ArrayList<Object> ret = new ArrayList<Object>();
        JSONArray jsonList = (JSONArray)object;
        for (int i = 0; i < jsonList.length(); i++){
            Object value = null;
            try {
                value = jsonList.getJSONObject(i);
            } catch (JSONException e) {
                //log.error("Error getting ElementFromJSONArray("+i+"):", e);
            }
            value = ConvertUnknownJSON(value);
            ret.add(value);
        }
        return ret;
    }

    public static Object ConvertUnknownJSON(Object obj){
        if(obj instanceof JSONArray)
            obj = getArrayListFromJSONObject((JSONArray) obj);
        if(obj instanceof JSONObject)
            obj = getHashMapFromJSONObject((JSONObject)obj);
        return obj;
    }

    public static <T> T CreateObjectFromJSON(String json, Class<T> type){
        T ret = null;
        try {
            ret = new ObjectMapper().readValue(json, type);
        } catch (IOException e) {
            e.printStackTrace();
            log.error("IOException: ", e);
        }
        return ret;
    }

    public static Double DateToEpoch(Date date){
        return new Double(date.toInstant().now().getEpochSecond());
    }
    public static Date EpochToDate(Double epoch){  return new Date((long) (epoch * 1000)); }

    public static List<String> ReadConfigFile(String configFileName) throws Exception {
        List<String> ret = new ArrayList<String>();

        String line;
        try (
                InputStream fis = new FileInputStream(configFileName);
                InputStreamReader isr = new InputStreamReader(fis, Charset.forName("UTF-8"));
                BufferedReader br = new BufferedReader(isr);
        ) {
            int i=0;
            while ((line = br.readLine()) != null) {
               ret.add(line);
            }
        } catch (FileNotFoundException e) {
            throw e;
        } catch (IOException e) {
            throw e;
        }
        return ret;
    }
}


