package ifmo.escience.dapris.monitoring.common;
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
//import org.apache.commons.configuration2.Configuration;
//import org.apache.commons.configuration2.XMLConfiguration;
//import org.apache.commons.configuration2.builder.fluent.Configurations;
//import org.apache.commons.configuration2.ex.ConfigurationException;
//import org.apache.commons.logging.Log;
//import org.apache.commons.logging.LogFactory;
//import org.apache.logging.log4j.LogManager;
//import org.apache.logging.log4j.Logger;
import org.bson.Document;
import org.json.JSONObject;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by Pavel Smirnov
 */
public class CommonMongoClient {
    //private static Logger log = LogManager.getLogger(CommonMongoClient.class);
    public static String metricsCollection = "metrics";
    public static String stateCollection = "clusterStates";

    private String defaultDBname = "logging";
    public MongoClient mongoClient;
    private MongoDatabase defaultDB;
    private Mongo mongo;
    private String host = "192.168.13.133";
    private String username;
    private String password;
    private String configFileName = "Mongo.conf";
    private DB db;
    private Boolean isOpened=false;
    private Boolean closeAtFinish=false;

    public CommonMongoClient(){
        ReadConfigFile();
    }

    private void ReadConfigFile(){

//        Configurations configs = new Configurations();
//        try
//        {
//            Configuration config = configs.properties(new File(configFileName));
//            host = config.getString("host");
//            username = config.getString("username");
//            password = config.getString("password");
//            defaultDBname = config.getString("dbname");
//
//
//        }
//        catch (ConfigurationException cex)
//        {
//            log.info("Cannot read external config. Using default values");
//        }

    }

    public MongoDatabase getDefaultDB(){ return defaultDB; }

    public CommonMongoClient(String serverUrl, String defaultDBname){
        this.host = serverUrl;
        this.defaultDBname = defaultDBname;
    }

    public void open(){
        if(isOpened)return;
        //log.trace("Mongo client open()");

        if(username!=null && password!=null && !username.equals("") && !password.equals("")){
            List<MongoCredential> credentials = new ArrayList<MongoCredential>();
            credentials.add(
                    MongoCredential.createMongoCRCredential(username, defaultDBname,password.toCharArray())
            );
            mongoClient = new MongoClient(new ServerAddress(host), credentials);
        }else
            mongoClient = new MongoClient(new ServerAddress(host));

        defaultDB = mongoClient.getDatabase(defaultDBname);

        mongo = new Mongo(host);
        db = mongo.getDB(defaultDBname);
        isOpened=true;
    }

    public void close(){
        //log.trace("Mongo client close()");
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
                //log.error(e);
            }
            catch (Exception e) {
                //log.error(e);
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
            //log.error(e);
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

    public List<Document> getDocumentsFromDB(String collection, Document condition){
        return getDocumentsFromDB(collection, condition, null, 0);
    }

    public List<Document> getDocumentsFromDB(String collection, Document condition, Document sort, int limit){
        if(condition==null)
            condition=new Document();

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
        ArrayList<Document> ret = null;
        try {
            MongoCollection coll = defaultDB.getCollection(collection);
            FindIterable<Document> res = coll.find(condition).sort(sort).projection(new Document("_id", 0)).limit(limit);
            Iterator keysIter = res.iterator();
            ret = new ArrayList<Document>();
            while (keysIter.hasNext()){
                Document doc = (Document)keysIter.next();
                ret.add(new Document(doc));
            }
        }
        catch (Exception ex){
            throw ex;
            //log.error(ex);
        }
        if(closeAtFinish)close();
        return ret;
    }

    public <T> List<T> getObjectsFromDB(String collection, DBObject condition, int limit, Class<T> targetClass){
        //Example:  List<ClusterState> res = getObjectsFromDB("clusterStates", new BasicDBObject(){{ put("name", "Mesos"); }}, ClusterState.class);
        List<T> ret = new ArrayList<T>();
        if(!isOpened){ open(); closeAtFinish = true;}
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
                //log.error(e);
            }

        }
        if(closeAtFinish)close();
        return ret;
    }

    private DBCursor getObjectsFromDB(String collection, DBObject condition, int limit){

        DBCollection dbCollection = db.getCollection(collection);
        DBCursor cursor = dbCollection.find(condition, new BasicDBObject("_id", 0)).limit(limit);

        return cursor;
    }
}


