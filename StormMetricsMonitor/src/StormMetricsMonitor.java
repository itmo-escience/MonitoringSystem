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

import java.util.*;

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
        stormMetricsMonitor.getActualData();
        //stormMetricsMonitor.analyzeData();
        //stormMetricsMonitor.startMonitoring(10000);
        //stormMetricsMonitor.InsertDataToDb();
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
            JSONObject json = Utils.getJsonFromUrl(baseUrl + topoName);
            String uptimeStr = json.get("uptime").toString();

            JSONArray topologyStats = ((JSONArray) json.get("topologyStats"));

            JSONObject topologyStatsAlltime = (JSONObject) topologyStats.get(topologyStats.length() - 1);
            Document topoDoc = new Document();
            topoDoc.put("timestamp", timestamp);
            topoDoc.put("name", "Topology");
            topoDoc.put("uptime", uptimeStr);
            for (String key : keys0) {
                Object value = ((JSONObject) topologyStatsAlltime).get(key);
                if(value!=null && !value.toString().equals("null"))
                    topoDoc.put(key, value);
            }

            log.trace("Opening client");
            List<Document> components = new ArrayList<Document>();
            for (Object component : (JSONArray) json.get("spouts")) {
                Document ins = new Document();
                ins.put("timestamp", timestamp);
                for (String key2 : keys1)
                    if(((JSONObject) component).keySet().contains(key2)){
                        String key = key2;
                        Object value = ((JSONObject) component).get(key);
                        if (key == "spoutId") key = "name";
                        if(value!=null)
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
                    } else if(((JSONObject) component).keySet().contains(key)) {
                        Object val = ((JSONObject) component).get(key);
                        if (key == "boltId") key = "name";
                        if (key == "processLatency") val = processLatency;
                        if (key == "executeLatency") val = executeLatency;
                        if(val!=null)
                            ins.put(key, val);
                    }
                }
                components.add(ins);
                //mongoClient.insertDocumentToDB("storm."+topoName, ins);
            }
            topoDoc.put("components", components);
            String collectionName = "storm." + topoName;
            mongoClient.insertDocumentToDB(collectionName, topoDoc);
        }
        mongoClient.close();
    }

    public void analyzeData() {
        String defaultTopoName = "patient_1-6-1455438719";
        String annealingTopoName ="";
        //String topoName = "patient_2-7-1455455748";
        //String topoName = "patient_3-8-1455457123";
        //topoName = "benchmark2-12-1455460437";
        //topoName = "storm.patient_1-1-1455491540";
        //topoName = "storm.patient_1_5242880_3_1_3-1-1455518716";
        //topoName = "patient_1_5242880_3_1_3-2-1455520043";
        //topoName = "patient_our_5242880_3_1_3-1-1455525736";

        //String[] experiment = {"patient_default_5242880_3_1_3-1-1455528852", "patient_our_5242880_3_1_3-1-1455526957"};
        //String[] experiment = {"patient_default_5242880_10_3_9-2-1455531141", "patient_our_5242880_10_3_9-1-1455532448"};
        String[] experiment = {"patient_default_5242880_5_1_3-1-1455556218", "patient_our_5_5_1_3-1-1455636684"};



        String[] metrics = {"emitted" /*, "transferred", "executed" */};
        Boolean average = true;

        HashMap<String, List<Document>> topoStates = new HashMap<String, List<Document>>();
        topoStates.put("Default", mongoClient.getDocumentsFromDB("storm." + experiment[0].replace("storm.", ""), null, null, 100));
        topoStates.put("Annealing", mongoClient.getDocumentsFromDB("storm." + experiment[1].replace("storm.", ""), null, null, 100));


        tuplesPerSecond(topoStates, 6, "("+experiment[0].split("-")[0].split("default_")[1]+")");
        //averageLatency(topoStates);
    }

    public void InsertDataToDb(){

        Random rand = new Random();
        Date timestamp = new Date();
        int prevRiskLevel=0;
        mongoClient.open();
        int i=0;
        while(i<50) {
            int riskLevel = rand.nextInt(30 + 1) + 30;
            Document ins = new Document();
            ins.put("DataID", "Patient_1_Fragment_"+String.valueOf(i));

            ins.put("ModelName", (prevRiskLevel>50? "pp_w-75s_fft_mag" : "pp_w-75s_freq-corr-1-None"));

            ins.put("Parameters", new ArrayList());
            ins.put("RiskLevel", String.valueOf(riskLevel));
            ins.put("DateStart", timestamp);
            ins.put("DateFinish", timestamp);
            prevRiskLevel = riskLevel;
            mongoClient.insertDocumentToDB("seizurePredictionResults", ins);
            if(prevRiskLevel<50)
                i++;
        }
        mongoClient.close();
    }

    public void tuplesPerSecond(HashMap<String, List<Document>> topoStates, int averagePeriod, String additionalTitle) {

        String[] metricNames = {"emitted", "transferred", "executed"};

        log.trace("Analysing data from DB");
        JavaPlot p = new JavaPlot();
        p.setTitle("Tuples per second "+additionalTitle);
        p.getAxis("x").setLabel("Processing time, s");
        p.getAxis("y").setLabel("Tuples per second");

        for (String schedulerName : topoStates.keySet()){
            HashMap<String, ArrayList<double[]>> dataSet = new HashMap<String, ArrayList<double[]>>();
            for (String metricName : metricNames){
                int i = 0;
                if (!dataSet.containsKey(metricName))
                    dataSet.put(metricName, new ArrayList<double[]>());
                for (Document topoState : topoStates.get(schedulerName)){
                    //"completeLatency", new Document("$gt", 0)
                    String[] uptime = topoState.get("uptime").toString().split(" ");
                    Integer seconds = Integer.parseInt(uptime[uptime.length - 1].replace("s", ""));
                    if (uptime.length > 1)
                        seconds += 60 * Integer.parseInt(uptime[uptime.length - 2].replace("m", ""));
                    if (uptime.length > 2)
                        seconds += 3600 * Integer.parseInt(uptime[uptime.length - 3].replace("h", ""));

                    Double totalKeyValue = 0.0;
                    for (Document compDoc : ((ArrayList<Document>) topoState.get("components"))) {
                        if (compDoc.containsKey(metricName))
                            totalKeyValue += Double.parseDouble(compDoc.get(metricName).toString());
                    }
                    //if (average)
                    totalKeyValue /= seconds;
                    dataSet.get(metricName).add(new double[]{i*10.0, totalKeyValue});
                    i++;
                }

                ArrayList<double[]> setToDisplay = dataSet.get(metricName);
                if(setToDisplay.size()>0) {
                    if (averagePeriod > 1) {
                        ArrayList<double[]> averigedDataSet = new ArrayList<double[]>();
                        int j = 1;
                        int period = 3;
                        Double valueToAverage = 0.0;
                        for (double[] pair : dataSet.get(metricName)) {
                            valueToAverage += pair[1];
                            if (j == period) {
                                averigedDataSet.add(new double[]{pair[0], valueToAverage / period});
                                valueToAverage = 0.0;
                                j = 1;
                            }

                            j++;
                        }
                        setToDisplay = averigedDataSet;

                    }

                    DataSetPlot plot = new DataSetPlot(setToDisplay.toArray(new double[0][]));
                    plot.setTitle(schedulerName + "_" + metricName);
                    PlotStyle plotStyle = plot.getPlotStyle();
                    plotStyle.setStyle(Style.LINES);
                    if (schedulerName == "Annealing")
                        plotStyle.setLineWidth(4);
                    else
                        plotStyle.setLineWidth(2);
                    p.addPlot(plot);
                }
            }
    }

        //stl.setPointType(5);
        //stl.setPointSize(5);
        //p.addPlot("sin(x)");
        p.plot();
    }

    public void averageLatency(HashMap<String, List<Document>> topoStates){

        String[] metricsNames = {"processLatency", "executeLatency"/*, "completeLatency"*/ };
        JavaPlot p = new JavaPlot();
        p.setTitle("Averige latency (ms)");
        p.getAxis("x").setLabel("Processing time, s");
        p.getAxis("y").setLabel("Averige latency (ms)");


        for (String schedulerName : topoStates.keySet()) {
            HashMap<String, ArrayList<double[]>> dataSet = new HashMap<String, ArrayList<double[]>>();
            for (String metricName : metricsNames) {

                if (!dataSet.containsKey(metricName))
                    dataSet.put(metricName, new ArrayList<double[]>());
                int i = 0;
                for (Document topoState : topoStates.get(schedulerName)) {
//                    String[] uptime = topoState.get("uptime").toString().split(" ");
//                    Integer seconds = Integer.parseInt(uptime[uptime.length - 1].replace("s", ""));
//                    if (uptime.length > 1)
//                        seconds += 60 * Integer.parseInt(uptime[uptime.length - 2].replace("m", ""));
//                    if (uptime.length > 2)
//                        seconds += 3600 * Integer.parseInt(uptime[uptime.length - 3].replace("h", ""));
                    Double metricValue = 0.0;
                    for (Document compDoc : ((ArrayList<Document>) topoState.get("components"))) {
                        if (compDoc.containsKey(metricName))
                            metricValue += Double.parseDouble(compDoc.get(metricName).toString());
                    }
                    dataSet.get(metricName).add(new double[]{i * 10.0, metricValue});

                    i++;
                }

                for (String compName : dataSet.keySet()) {
                    DataSetPlot plot = new DataSetPlot(dataSet.get(compName).toArray(new double[0][]));
                    plot.setTitle(schedulerName+"_"+compName);
                    PlotStyle plotStyle = plot.getPlotStyle();
                    plotStyle.setStyle(Style.LINES);
                    if(schedulerName=="Annealing")
                        plotStyle.setLineWidth(4);
                    else
                        plotStyle.setLineWidth(2);
                    p.addPlot(plot);
                }
            }
        }
        p.plot();
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

