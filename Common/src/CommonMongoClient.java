import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.mongodb.*;
import com.mongodb.bulk.*;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.util.JSON;
import com.sun.org.apache.xpath.internal.operations.Bool;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by Pavel Smirnov
 */
public class CommonMongoClient {
    private static Logger log = LogManager.getLogger(CommonMongoClient.class);
    public static String metricsCollection = "metrics";
    public static String stateCollection = "clusterStates";
    private String serverUrl = "192.168.13.133";
    private String defaultDBname = "logging";
    public MongoClient mongoClient;
    private MongoDatabase defaultDB;
    private Mongo mongo;
    private DB db;
    private Boolean isOpened=false;
    private Boolean closeAtFinish=false;

    public CommonMongoClient(){

    }

    public void ReadConfigFile(){
        BufferedReader br = null;

        try {

            String sCurrentLine;
            br = new BufferedReader(new FileReader("MongoClient.conf"));
            while ((sCurrentLine = br.readLine()) != null) {
                System.out.println(sCurrentLine);
            }

        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (br != null)br.close();
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
    }

    public MongoDatabase getDefaultDB(){ return defaultDB; }

    public CommonMongoClient(String serverUrl, String defaultDBname){
        this.serverUrl = serverUrl;
        this.defaultDBname = defaultDBname;
    }

    public void open(){
        if(isOpened)return;
        log.trace("Mongo client open()");
        mongoClient = new MongoClient(serverUrl, 27017 );
        defaultDB = mongoClient.getDatabase(defaultDBname);
        mongo = new Mongo(serverUrl, 27017);
        db = mongo.getDB(defaultDBname);
        isOpened=true;
    }

    public void close(){
        log.trace("Mongo client close()");
        mongoClient.close();
        mongo.close();
        isOpened=false;
        closeAtFinish=false;
    }
//    public void saveObjectToDB(Object obj, String collection){
//        JSONObject stateJSON = new JSONObject(obj);
//        String jsonStr = stateJSON.toString();
//        Document doc = Document.parse(jsonStr);
//        insertDocumentToDB(doc, collection);
//    }

    public DBObject CreateDBObject(Object obj){
        if(!(obj instanceof String)){
            ObjectMapper mapper = new ObjectMapper();
            DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm a z");
            mapper.setDateFormat(df);
            try {
                mapper.writeValue(new File("tmp.json"), obj);
                obj = mapper.writeValueAsString(obj);
            } catch (IOException e){
                log.error(e);
            }
            catch (Exception e) {
                log.error(e);
            }
        }
        obj = JSON.parse((String)obj);
        return (DBObject)obj;
    }

    public void insertDocumentToDB(String collection, Document doc){
        if(!isOpened){ open(); closeAtFinish = true;}
        //log.trace("Inserting document to DB: "+collection);
        try {
            MongoCollection coll = defaultDB.getCollection(collection);
            coll.insertOne(doc);
        }
        catch (Exception e){
            log.error(e);
        }
        if(closeAtFinish)close();
    }

    public void saveObjectToDB(String collection, Object insert){
        saveObjectToDB(collection, null, insert);
    }



    public void saveObjectToDB(String collection, Object find, Object update){

        if(!(find instanceof DBObject))
            find = CreateDBObject(find);

        if(!(update instanceof DBObject))
            update = CreateDBObject(update);
        if(!isOpened){ open(); closeAtFinish = true;}
        DBCollection dbCollection = db.getCollection(collection);
        if (find!=null)
            dbCollection.update((DBObject) find, (DBObject) update, true, false);
        else
            dbCollection.save((DBObject)update);
        if(closeAtFinish)close();
    }

    public FindIterable<Document> getDocumentsFromDB(String collection, Document condition){
        return  getDocumentsFromDB(collection, condition, null, 0);
    }

    public FindIterable<Document> getDocumentsFromDB(String collection, Document condition, Document sort, int limit){

        if(sort==null)
            sort = new Document("_id",1);
         /* Example:
        FindIterable<Document> res = mongoClient.getDocumentsFromDB(new Document(), "clusterStates");
        Iterator keysIter = res.iterator();
        while (keysIter.hasNext()){
            //System.out.println(flavoursIter.next());
            Document resultmap = (Document)keysIter.next();
            ret.add(resultmap.get("started").toString());
        }
       */

        if(!isOpened){ open(); closeAtFinish = true;}
        MongoCollection coll = defaultDB.getCollection(collection);
        FindIterable<Document> ret = coll.find(condition).sort(sort).projection(new Document("_id", 0)).limit(limit);
        if(closeAtFinish)close();
        return ret;
    }

    public <T> List<T> getObjectsFromDB(String collection, DBObject condition, int limit, Class<T> targetClass){
        //Example:  List<ClusterState> res = getObjectsFromDB("clusterStates", new BasicDBObject(){{ put("name", "Mesos"); }}, ClusterState.class);
        List<T> ret = new ArrayList<T>();

        DBCursor cursor = getObjectsFromDB(collection, condition, limit);
        while (cursor.hasNext()){
            BasicDBObject dbObj = (BasicDBObject)cursor.next();
            String jsonStr = dbObj.toJson().toString();
            ObjectMapper mapper = new ObjectMapper();
            mapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
            T retObj = null;
            try {
                retObj = mapper.readValue(jsonStr, targetClass);

                ret.add(retObj);
            } catch (IOException e) {
                log.error(e);
            }

        }
        return ret;
    }

    public DBCursor getObjectsFromDB(String collection, DBObject condition, int limit){
        if(!isOpened){ open(); closeAtFinish = true;}
        DBCollection dbCollection = db.getCollection(collection);
        DBCursor cursor = dbCollection.find(condition, new BasicDBObject("_id", 0)).limit(limit);
        if(closeAtFinish)close();
        return cursor;
    }
}


