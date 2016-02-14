package ifmo.escience.dapris.monitoring;
import com.mongodb.BasicDBObject;
import com.mongodb.util.JSONCallback;
import com.panayotis.gnuplot.JavaPlot;
import com.panayotis.gnuplot.plot.AbstractPlot;
import com.panayotis.gnuplot.plot.DataSetPlot;
import com.panayotis.gnuplot.style.NamedPlotColor;
import com.panayotis.gnuplot.style.PlotStyle;
import com.panayotis.gnuplot.style.Style;
import ifmo.escience.dapris.monitoring.common.CommonMongoClient;
import ifmo.escience.dapris.monitoring.common.Utils;
import org.bson.Document;
import org.json.JSONArray;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import javax.print.Doc;

/**
 * Created by Pavel Smirnov
 */
public class StormMetricsMonitor {
    private static Logger log = LogManager.getLogger(StormMetricsMonitor.class);
    String baseUrl = "";

    private CommonMongoClient mongoClient;

    public static void main(String[] args){
        StormMetricsMonitor stormMetricsMonitor = new StormMetricsMonitor("http://192.168.92.11:8080/api/v1/topology/");
        //stormMetricsMonitor.getActualData();
        //stormMetricsMonitor.analyzeData();
        stormMetricsMonitor.startMonitoring(10000);
    }

    public StormMetricsMonitor(String baseUrl){
        this.baseUrl = baseUrl;
        String[] splitted = baseUrl.split("/");
        mongoClient = new CommonMongoClient();
    }

    public void getActualData(){
        log.trace("Getting actual data");
        Date timestamp = new Date();
        String[] keys0 = {"emitted","transferred", "completeLatency"};
        String[] keys1 = {"spoutId", "emitted","transferred","acked", "completeLatency"};
        String[] keys2 = {"boltId", "emitted","executed","transferred","acked", "completeLatency", "processLatency", "executeLatency", "capacity"};

        JSONObject summaryJson = Utils.getJsonFromUrl(baseUrl+"summary");
        mongoClient.open();
        for(Object topology : (JSONArray)summaryJson.get("topologies")) {
            String topoName = ((JSONObject) topology).get("id").toString();
            JSONObject json = Utils.getJsonFromUrl(baseUrl+topoName);
            String uptimeStr = json.get("uptime").toString();

            JSONArray topologyStats = ((JSONArray) json.get("topologyStats"));

            JSONObject topologyStatsAlltime = (JSONObject) topologyStats.get(topologyStats.length() - 1);
            Document topoDoc = new Document();
            topoDoc.put("timestamp", timestamp);
            topoDoc.put("name", "Topology");
            topoDoc.put("uptime", uptimeStr);
            for (String key : keys0)
                topoDoc.put(key, ((JSONObject) topologyStatsAlltime).get(key));
            log.trace("Opening client");
            List<Document> components = new ArrayList<Document>();
            for (Object component : (JSONArray) json.get("spouts")) {
                Document ins = new Document();
                ins.put("timestamp", timestamp);
                for (String key2 : keys1) {
                    String key = key2;
                    Object value = ((JSONObject) component).get(key);
                    if (key == "spoutId") key = "name";
                    ins.put(key, value);
                }
                components.add(ins);
                //mongoClient.insertDocumentToDB("storm."+topoName, ins);
            }

            for (Object component : (JSONArray) json.get("bolts")) {
                Document ins = new Document();
                ins.put("timestamp", timestamp);
                Double processLatency = Double.parseDouble(((JSONObject) component).get("processLatency").toString());
                Double executeLatency = Double.parseDouble(((JSONObject) component).get("executeLatency").toString());
                for (String key2 : keys2) {
                    String key = key2;
                    if (key == "completeLatency") {
                        ins.put(key, processLatency + executeLatency);
                    } else {
                        Object val = ((JSONObject) component).get(key);
                        if (key == "boltId") key = "name";
                        if (key == "processLatency") val = processLatency;
                        if (key == "executeLatency") val = executeLatency;
                        ins.put(key, val);
                    }
                }
                components.add(ins);
                //mongoClient.insertDocumentToDB("storm."+topoName, ins);
            }
            topoDoc.put("components", components);

            mongoClient.insertDocumentToDB("storm." + topoName, topoDoc);
        }
        mongoClient.close();
    }

    public void analyzeData(){
        String topoName = "";
        log.trace("Analysing data from DB");
        String[] keys = {"processLatency", "executeLatency"/*, "completeLatency"*/ };

        JavaPlot p = new JavaPlot();
        p.setTitle("Default Terminal Title");
        p.getAxis("x").setLabel("Processing time, s");
        p.getAxis("y").setLabel("Averige latency, ms");

        List<Document> topoStates = mongoClient.getDocumentsFromDB("storm."+topoName, new Document());
        //double[][] dataset = {{1, 1.1}, {2, 2.2}, {3, 3.3}, {4, 4.3}};
        HashMap<String, ArrayList<double[]>> dataSet = new HashMap<String, ArrayList<double[]>>();
        for (String key : keys){
            int i=0;
            if(!dataSet.containsKey(key))
                dataSet.put(key, new ArrayList<double[]>());
            for (Document topoState : topoStates){
                //"completeLatency", new Document("$gt", 0)
                Double totalKeyValue = 0.0;
               for(Document compDoc: ((ArrayList<Document>)topoState.get("components"))){
                    if(compDoc.containsKey(key))
                        totalKeyValue += Double.parseDouble(compDoc.get(key).toString());
                }
                dataSet.get(key).add(new double[]{  i*1.0 , totalKeyValue});
                i++;
            }
            DataSetPlot plot = new DataSetPlot(dataSet.get(key).toArray(new double[0][]));
            plot.setTitle(key);
            PlotStyle plotStyle = plot.getPlotStyle();
            plotStyle.setStyle(Style.LINES);
            p.addPlot(plot);
        }


        //stl.setPointType(5);
        //stl.setPointSize(5);
        //p.addPlot("sin(x)");
        p.plot();

        String test = topoStates.toString();


    }

    public void startMonitoring(int sleepInterval){
        log.info("Monitoring starting");
        while(1==1){
            try {
                getActualData();
                log.trace("Sleeping " + sleepInterval+"ms");
                Thread.sleep(sleepInterval);
            } catch (InterruptedException e){
                log.error(e);
            }
        }
    }


}

