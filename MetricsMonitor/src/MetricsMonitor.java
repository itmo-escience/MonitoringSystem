import com.mongodb.client.FindIterable;
import ifmo.escience.dapris.common.entities.Node;
import ifmo.escience.dapris.common.entities.NodeState;
import ifmo.escience.dapris.common.entities.NodeStatus;

import org.bson.Document;

import java.net.InetAddress;
import java.net.UnknownHostException;
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
    private int sleepInterval = 5000;
    private GangliaAPIClient client;
    private CommonMongoClient mongoClient;
    public String monitoringHost = "192.168.92.11:8081"; //"192.168.13.133:8082"
    private String startedMetricsMons = "startedMetricsMons";
    private String defaultCollection = "metrics";
    private String[] desiredMetrics = new String[]{ "bytes_in", "bytes_out", "cpu_user","mem_free", "part_max_used"};
    private List<String> clusterNames;
    public static void main(String[] args) {

//        String ApiServerUrl = "192.168.13.133:8082";  //"192.168.13.133:8080"
//               ApiServerUrl = "192.168.92.11:8081";
        MetricsMonitor metricsMonitor = new MetricsMonitor(new CommonMongoClient());
//        if(args!=null && args.length>0)
//            ApiServerUrl = args[0];
        if (args!=null  && args.length>0 && args[0].equals("getData"))
            metricsMonitor.GetMetricsFromDb("node-92-11", LocalDateTime.now().minusMinutes(1), LocalDateTime.now());
        else
            metricsMonitor.StartMonitoring();
    }

    public MetricsMonitor(CommonMongoClient mongoClient){
        this.mongoClient = mongoClient;
        client = new GangliaAPIClient(monitoringHost, desiredMetrics);
        clusterNames = client.GetClusterNames();
    }

    public void StartMonitoring(){

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
        mongoClient.insertDocumentToDB(startedMetricsMons, startedEntry);
        while(1==1){
            try {

                for(String clusterName : clusterNames){
                    TreeMap<String, TreeMap<String, Object>> state = client.GetActualMetricsOfCluster(clusterName, desiredMetrics);
                    SaveMetricsToDb(state);
                }
                log.trace("Sleeping " + sleepInterval + "ms");
                Thread.sleep(sleepInterval);
            } catch (InterruptedException e) {
                e.printStackTrace();
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
            FindIterable<Document> res = mongoClient.getDocumentsFromDB(defaultCollection, new Document("hostname",hostname), new Document("_id",-1),1);
            //mongoClient.saveObjectToDB(defaultCollection, metricsEntry);

            if (res.first() == null || (res.first() != null && !res.first().get("metrics").toString().contains(hostMetrics.toString()))){
                Document metricsEntry2 = new Document();
                metricsEntry2.append("timestamp", new Date());
                metricsEntry2.append("hostname", hostname);
                metricsEntry2.append("metrics", hostMetrics);
                log.trace("Inserting metrics to DB: "+hostname+" "+(res.first()!=null?"Equality:"+res.first().get("metrics").toString()+"=="+hostMetrics.toString():"0 found"));
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

    private TreeMap<LocalDateTime, TreeMap<String, Object>> GetMetricsFromDb(String hostname, LocalDateTime starttime, LocalDateTime endtime){
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
        FindIterable<Document> res = null;
        mongoClient.open();
        res = mongoClient.getDocumentsFromDB(defaultCollection, new Document("hostname", hostname).append("timestamp", dateFilter));
        if(res.first()==null){ //Find monitor, started before task started
            log.debug("No metrics for range. Getting uprange");
            FindIterable<Document> lastStartedMonitor = mongoClient.getDocumentsFromDB(startedMetricsMons, new Document("monitoringHost", monitoringHost).append("timestamp", new Document("$lte", start)), new Document("_id", -1), 1);
            Document timespampfilter = new Document("$lte", start);
            if(lastStartedMonitor.first()!=null){
                log.debug("Getting uprange only till "+lastStartedMonitor.first().get("timestamp"));
                timespampfilter.append("$gte", lastStartedMonitor.first().get("timestamp"));
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

    public List<NodeState> getNodeStateForPeriod(Node node, LocalDateTime starttime, LocalDateTime endtime){
        ArrayList<NodeState> ret = new ArrayList<>();
        String hostname = node.getIp();
//        hostname = "node-92-16";
//        starttime = LocalDateTime.now().minusDays(4);
//        endtime = LocalDateTime.now();
        TreeMap<LocalDateTime, TreeMap<String, Object>> metrics = GetMetricsFromDb(hostname, starttime, endtime);
        for(LocalDateTime timestamp : metrics.keySet()){
            TreeMap<String, Object> metricsmap = metrics.get(timestamp);
            NodeStatus status = null;
            double cpuUsage = Double.parseDouble(metricsmap.get("cpu_user").toString());
            double memUsage = Double.parseDouble(metricsmap.get("mem_free").toString());
            double gpuUsage = 0.0;
            double netInUsage = Double.parseDouble(metricsmap.get("bytes_in").toString());
            double netOutUsage = Double.parseDouble(metricsmap.get("bytes_out").toString());
            NodeState state = new NodeState(timestamp.toString(), node.getId(), timestamp, status, cpuUsage, memUsage, gpuUsage, netInUsage, netOutUsage);
            ret.add(state);
        }
        return ret;
    }

}
