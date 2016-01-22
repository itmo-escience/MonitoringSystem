import com.mongodb.client.FindIterable;
import ifmo.escience.dapris.common.entities.Node;
import ifmo.escience.dapris.common.entities.NodeState;
import ifmo.escience.dapris.common.entities.NodeStatus;

import org.bson.Document;

import java.io.*;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.Charset;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;


/**
 * Created by Pavel Smirnov
 */
public class MetricsMonitor {
    private static Logger log = LogManager.getLogger(MetricsMonitor.class);
    private IMetricsDataProvider dataProvider;
    private CommonMongoClient mongoClient;
    public String monitoringHost = "192.168.92.11:8081"; //"192.168.13.133:8082"
    private String configFileName = "MetricsMonitor.config";
    private String defaultCollection = "metrics";
    private String[] desiredMetrics = new String[]{ "bytes_in", "bytes_out", "cpu_user","mem_free", "part_max_used"};

    private List<String> clusterNames;
    public static void main(String[] args) {

//        String ApiServerUrl = "192.168.13.133:8082";  //"192.168.13.133:8080"
//               ApiServerUrl = "192.168.92.11:8081";
        MetricsMonitor metricsMonitor = new MetricsMonitor(new CommonMongoClient());
//        if(args!=null && args.length>0)
//            ApiServerUrl = args[0];
//        if (args!=null  && args.length>0 && args[0].equals("getData"))
//            metricsMonitor.GetMetricsFromDb("node-92-11", LocalDateTime.now().minusMinutes(1), LocalDateTime.now());
//        else
        int sleepInterval = 3000;
        //if (args!=null  && args.length>2 && args[0].equals("getData"))
        HashMap<String, String> argsList = new HashMap<String, String>();
        for(int i=0; i<args.length; i++){
            if(args[i].startsWith(("-"))){
                argsList.put(args[i], args[i+1]);
                i++;
            }
            i++;
        }
        if(argsList.containsKey("-sleepInterval"))
            sleepInterval = Integer.parseInt(argsList.get("-sleepInterval"));

        metricsMonitor.StartMonitoring(sleepInterval);
    }

    public MetricsMonitor(CommonMongoClient mongoClient){
        readConfigFile();
        this.mongoClient = mongoClient;
        dataProvider = new GangliaAPIClient(monitoringHost, desiredMetrics);
        clusterNames = dataProvider.GetClusterNames();
    }

    private void readConfigFile(){
        try {
            List<String> lines = Utils.ReadConfigFile(configFileName);
            for (int i = 0; i < lines.size(); i++) {
                if (i == 0) monitoringHost = lines.get(i);
                else
                    desiredMetrics = lines.get(i).split(",");
                ;
                i++;
            }
        }
        catch (Exception e){
            log.info("Cannot read external config. Using default values");
        }
    }


    public void StartMonitoring(int sleepInterval){

        String startedAt = "";
        try {
            startedAt = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        log.info("Start monitoring for "+monitoringHost);
        Document startedEntry = new Document();
        startedEntry.append("monitoringHost", monitoringHost);
        startedEntry.append("startedAt", startedAt);
        startedEntry.append("timestamp", new Date());
        mongoClient.insertDocumentToDB("startedMetricsMons", startedEntry);

        while(1==1){
            try {

                for(String clusterName : clusterNames){
                    TreeMap<String, TreeMap<String, Object>> state = dataProvider.GetData(clusterName, desiredMetrics);
                    SaveMetricsToDb(state);
                    state = null;
                }
                log.trace("Sleeping " + sleepInterval + "ms");
                Thread.sleep(sleepInterval);
            } catch (InterruptedException e) {
                log.error(e);
            }
        }
    }

    private void SaveMetricsToDb(TreeMap<String, TreeMap<String, Object>> metricsPerHosts){
        for(String hostname : metricsPerHosts.keySet()){
            TreeMap<String, Object> hostMetrics = metricsPerHosts.get(hostname);
           // MetricsEntry metricsEntry = new MetricsEntry(new Double(Instant.now().getEpochSecond()), hostname, hostMetrics);
            //metricsEntry.date = LocalDateTime.now();
            //metricsEntry.date = new Date();

            mongoClient.open();
            List<Document> res = mongoClient.getDocumentsFromDB(defaultCollection, new Document("hostname",hostname), new Document("_id",-1),1);
            //mongoClient.saveObjectToDB(defaultCollection, metricsEntry);

            if (res.size()==0 || (!res.get(0).get("metrics").toString().contains(hostMetrics.toString()))){
                Document metricsEntry2 = new Document();
                metricsEntry2.append("timestamp", new Date());
                metricsEntry2.append("hostname", hostname);
                metricsEntry2.append("metrics", hostMetrics);
                log.trace("Inserting metrics to DB: "+hostname+" "+(res.get(0)!=null?"Equality:"+res.get(0).get("metrics").toString()+"=="+hostMetrics.toString():"0 found"));
                mongoClient.insertDocumentToDB(defaultCollection, metricsEntry2);
           }
        }
        mongoClient.close();
    }

//    public List<MetricsEntry> GetMetricsFromDb2(String hostname, LocalDateTime starttime, LocalDateTime endtime){
//        Date start = Date.from(starttime.toInstant(starttime.atZone(ZoneId.systemDefault()).getOffset()));
//        Date end = Date.from(endtime.toInstant(starttime.atZone(ZoneId.systemDefault()).getOffset()));
//        DBObject datequery = BasicDBObjectBuilder.start("$gte", start).add("$lte", end).get();
//        BasicDBObject query = new BasicDBObject(){{
//            put("hostname", hostname);
//            put("date", datequery);
//            //put("date", new BasicDBObject("$gte", starttime.toEpochSecond(starttime.atZone(ZoneId.systemDefault()).getOffset()))
////            put("date", new BasicDBObject("$gte", Date.from(starttime.toInstant(starttime.atZone(ZoneId.systemDefault()).getOffset())))
////                            //.append("$lte", endtime.toEpochSecond(endtime.atZone(ZoneId.systemDefault()).getOffset()))
//           // );
//
//
//        }};
//        List<MetricsEntry> ret = mongoClient.getObjectsFromDB(defaultCollection, query, 0, MetricsEntry.class );
//        return ret;
//    }

    public TreeMap<LocalDateTime, TreeMap<String, Object>> GetMetricsFromDb(String hostname, LocalDateTime starttime, LocalDateTime endtime){
        log.debug("Getting GetMetrics from Db from DB "+hostname);
        //log.trace("Getting GetMetrics from Db from DB "+hostname);
        TreeMap<LocalDateTime, TreeMap<String, Object>> ret = new TreeMap<LocalDateTime, TreeMap<String, Object>>();
        Date start = Date.from(starttime.toInstant(starttime.atZone(ZoneId.systemDefault()).getOffset()));
        Document dateFilter = new Document("$gte", start);
        Date end = null;
        try {
            end = Date.from(endtime.toInstant(starttime.atZone(ZoneId.systemDefault()).getOffset()));
            dateFilter.append("$lte", end);
        }
        catch (Exception e){

        }
        List<Document> res = null;
        mongoClient.open();
        res = mongoClient.getDocumentsFromDB(defaultCollection, new Document("hostname", hostname).append("timestamp", dateFilter));
        if(res.size()==0){ //Find monitor, started before task started
            log.debug("No metrics for range. Getting uprange");
            List<Document> lastStartedMonitor = mongoClient.getDocumentsFromDB("startedMetricsMons", new Document("monitoringHost", monitoringHost).append("timestamp", new Document("$lte", start)), new Document("_id", -1), 1);
            Document timespampfilter = new Document("$lte", start);
            if(lastStartedMonitor.size()>0){
                log.debug("Getting uprange only till "+lastStartedMonitor.get(0).get("timestamp"));
                timespampfilter.append("$gte", lastStartedMonitor.get(0).get("timestamp"));
            }
            //find states before task started until the started monitor (if exists)
            res = mongoClient.getDocumentsFromDB(defaultCollection, new Document("hostname", hostname).append("timestamp", timespampfilter), new Document("_id", -1), 1);
        }
        Iterator<Document> iterator = res.iterator();
        int i=0;
        while (iterator.hasNext()){
            Document iter = iterator.next();
            Date date = (Date)iter.get("timestamp");
            Instant instant = Instant.ofEpochMilli(date.getTime());
            LocalDateTime localDateTime = LocalDateTime.ofInstant(instant, ZoneId.systemDefault());
            ret.put(localDateTime, new TreeMap<String, Object>(((Document) iter.get("metrics"))));
            i++;
        }
        mongoClient.close();
        //log.trace("Getting metrics for "+hostname+" "+start.toString()+" "+end.toString()+": "+ i+" found");
        return ret;

    }



}
