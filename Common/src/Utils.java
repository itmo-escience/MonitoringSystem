//package MonitoringSystem.Common;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mongodb.client.MongoCollection;
import org.apache.commons.httpclient.params.HttpMethodParams;
import org.apache.commons.httpclient.DefaultHttpMethodRetryHandler;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.bson.Document;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

/**
 *
 * @author Pavel Smirnov
 */
public class Utils {

    private static Log logger = LogFactory.getLog(Utils.class);

    public static JSONObject getJsonFromUrl(String url){
        logger.debug("Requesting "+url);

        JSONObject ret = null;
        HttpClient client = new HttpClient();
        GetMethod method = new GetMethod(url);
        method.getParams().setParameter(HttpMethodParams.RETRY_HANDLER, new DefaultHttpMethodRetryHandler(3, false));

        try {
            int statusCode = client.executeMethod(method);
            if (statusCode != HttpStatus.SC_OK){
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
                logger.error("Error getting ElementFromJSONArray("+i+"):", e);
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
            logger.error("IOException: ", e);
        }
        return ret;
    }



}


