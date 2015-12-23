import com.mongodb.client.FindIterable;
import ifmo.escience.dapris.common.entities.Node;
import ifmo.escience.dapris.common.entities.NodeState;
import ifmo.escience.dapris.common.entities.NodeStatus;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.bson.Document;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;

/**
 * Created by Pavel Smirnov
 */
public class MetricsMonitor {

    private static Log logger = LogFactory.getLog(GangliaAPIClient.class);
    private int sleepInterval = 3000;
    private GangliaAPIClient client;
    private CommonMongoClient mongoClient;
    private String defaultCollection = "metrics";
    private String[] desiredMetrics = new String[]{ "bytes_in", "bytes_out", "cpu_user","mem_free", "part_max_used"};
    public static void main(String[] args) {
        String ApiServerUrl = "192.168.13.133:8082";  //"192.168.13.133:8080"
               ApiServerUrl = "192.168.92.11:8081";

        if(args!=null && args.length>0)
            ApiServerUrl = args[0];

        MetricsMonitor metricsMonitor = new MetricsMonitor(new CommonMongoClient());
        metricsMonitor.StartMonitoring();
    }

    public MetricsMonitor(CommonMongoClient mongoClient){
        client = new GangliaAPIClient("192.168.92.11:8081", desiredMetrics);
        this.mongoClient = mongoClient;
    }

    public void StartMonitoring(){
        while(1==1){
            try {
                TreeMap<String, TreeMap<String, Object>> state = client.GetActualState();
                SaveMetricsToDb(state);
                Thread.sleep(sleepInterval);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    public TreeMap<String, TreeMap<String, Object>> GetActualState(){
        return client.GetActualState();
    }

    public void SaveMetricsToDb(TreeMap<String, TreeMap<String, Object>> metricsPerHosts){
        for(String hostName : metricsPerHosts.keySet()){
            TreeMap<String, Object> hostMetrics = metricsPerHosts.get(hostName);
            FindIterable<Document> res = mongoClient.getDocumentsFromDB(new Document("hostname", hostName).append("metrics", hostMetrics), defaultCollection);
            if (res.first() == null) {
                Document metricsEntry = new Document();
                metricsEntry.append("timestamp", new Date());
                metricsEntry.append("hostname", hostName);
                metricsEntry.append("metrics", hostMetrics);
                mongoClient.saveDocumentToDB(metricsEntry, defaultCollection);
            }

        }
    }

    public TreeMap<LocalDateTime, TreeMap<String, Object>> GetMetricsFromDb(String hostname, LocalDateTime starttime, LocalDateTime endtime){
        System.out.println("Getting GetMetrics from Db from DB "+hostname);
        TreeMap<LocalDateTime, TreeMap<String, Object>> ret = new TreeMap<LocalDateTime, TreeMap<String, Object>>();
        Date start = Date.from(starttime.toInstant(starttime.atZone(ZoneId.systemDefault()).getOffset()));
        Date end = Date.from(endtime.toInstant(starttime.atZone(ZoneId.systemDefault()).getOffset()));

        FindIterable<Document> res = mongoClient.getDocumentsFromDB(new Document("hostname", hostname).append("timestamp", new Document("$gte", start).append("$lte", end)), defaultCollection);
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
        //System.out.println("Getting metrics for "+hostname+" "+start.toString()+" "+end.toString()+": "+ i+" found");
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
