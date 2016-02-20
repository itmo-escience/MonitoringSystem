//package ifmo.escience.dapris.monitoring;
import com.mongodb.BasicDBObject;
import com.mongodb.util.JSONCallback;
import com.panayotis.gnuplot.JavaPlot;
import com.panayotis.gnuplot.plot.AbstractPlot;
import com.panayotis.gnuplot.plot.DataSetPlot;
import com.panayotis.gnuplot.style.NamedPlotColor;
import com.panayotis.gnuplot.style.PlotStyle;
import com.panayotis.gnuplot.style.Style;
import com.panayotis.gnuplot.terminal.GNUPlotTerminal;
import com.panayotis.gnuplot.terminal.ImageTerminal;
import ifmo.escience.dapris.monitoring.common.CommonMongoClient;
import ifmo.escience.dapris.monitoring.common.Utils;
import org.bson.Document;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import javax.imageio.ImageIO;
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
        if(args.length>0 && args[0].equals("aggregate"))
            stormMetricsMonitor.getExperimentsData();
        else
            stormMetricsMonitor.startMonitoring(10000);
        //stormMetricsMonitor.InsertDataToDb();
    }

    public StormMetricsMonitor(String baseUrl){
        this.baseUrl = baseUrl;
        String[] splitted = baseUrl.split("/");
        mongoClient = new CommonMongoClient();
    }

    public void getActualData(){
        log.trace("Getting actual data");

        JSONObject summaryJson = Utils.getJsonFromUrl(baseUrl+"summary");
        mongoClient.open();
        for(Object topology : (JSONArray)summaryJson.get("topologies")) {
            String topoID = ((JSONObject) topology).get("id").toString();
            JSONObject json = Utils.getJsonFromUrl(baseUrl + topoID);
            String uptimeStr = json.get("uptime").toString();

            JSONArray topologyStats = ((JSONArray) json.get("topologyStats"));
            JSONObject topologyStatsAlltime = (JSONObject) topologyStats.get(topologyStats.length() - 1);
            Document topoStats = ReturnValuesByKeys(topologyStatsAlltime);
            if(!topoStats.isEmpty()){
                List<Document> components = new ArrayList<Document>();
                for (String compType : new String[]{"spouts", "bolts"})
                    for (Object component : (JSONArray) json.get(compType)){
                        Document compStat = ReturnValuesByKeys(component);
                        if(!compStat.isEmpty())
                            components.add(compStat);
                    }
                if(!components.isEmpty()){
                    topoStats.put("name", "Topology");
                    topoStats.put("topoID", topoID);
                    topoStats.put("uptime", uptimeStr);
                    topoStats.put("components", components);

                    //String experimentsCollection = "storm." + topoName;
                    String collectionName = "storm.statistics";
                    mongoClient.insertDocumentToDB(collectionName, topoStats);
                }
            }
        }
        mongoClient.close();
    }

    public Document ReturnValuesByKeys(Object item){
        String[] keysTransfer = {"emitted","executed","transferred","acked"};
        String[] keysOther = {"boltId","spoutId", "completeLatency", "processLatency", "executeLatency", "capacity"};

        Date timestamp = new Date();
        Document comp = new Document();
        for (String key : keysTransfer)
            if (((JSONObject) item).keySet().contains(key)) {
                Object value = ((JSONObject) item).get(key);
                if (value != null && !value.toString().equals("null"))
                    comp.put(key, value);
            }
        if(comp.keySet().size()>0){
            for (String key2 : keysOther)
                if (((JSONObject) item).keySet().contains(key2)) {
                    String key = key2;
                    Object value = ((JSONObject) item).get(key);
                    if (key == "spoutId" || key == "boltId" ) key = "name";
                    if (value != null)
                        comp.put(key, value);
                }
            comp.put("timestamp", timestamp);
        }
        return comp;
    }

    public void getExperimentsData(){
        String path = "d:/Projects/MonitoringSystem/StormMetricsMonitor/target/";
        StringBuilder output = new StringBuilder();
        mongoClient.open();
        List<Document> experiments = mongoClient.getDocumentsFromDB("storm.experiments", null, new Document(){{ put("_id", -1); }}, 100);
        for(Document experiment : experiments){

            String name = experiment.get("name").toString();
            List<Document> runs = (List<Document>)experiment.get("runs");
            if(runs.size()>1){
                //output.append("<H1>"+experiment.getString("name")+"</H1>\n");
                HashMap<String, List<Document>> topoStates = new HashMap<String, List<Document>>();
                for(Document run : runs){
                    String scheduler = run.get("scheduler").toString();
                    if(run.containsKey("topoID")){
                        String topoID = run.get("topoID").toString();
                        Document find = new Document() {{   put("topoID", topoID);  }};
                        List<Document> runStats = mongoClient.getDocumentsFromDB("storm.statistics", find);
                        List<Document> factAssignments = mongoClient.getDocumentsFromDB("storm.FactAssignments", find);
                        //output.append("Fact assignments:"+factAssignments+"</H1>");
                        Document structure = mongoClient.getDocumentFromDB("storm.Structure", find);
                        //output.append("Structure:"+structure+"<br>");
                        List<Document> schedules = mongoClient.getDocumentsFromDB("storm.Schedules", find);
                        if(runStats.size()>0)
                            topoStates.put(scheduler, runStats);
                        else{
                           // String abc = topoStates.toString();
                        }
                    }
                }

                if(topoStates.size()>0){
                    String relPath = Paths.get("img",experiment.get("name").toString().toString()+".png").toString();
                    String absPath =  Paths.get(path,relPath).toString();
                    tuplesPerSecond(topoStates, 1, "("+ name +")",absPath);
                    output.append("<img src='"+relPath+"'>\n");
                }
            }

        }
        mongoClient.close();
        String filename = Paths.get(path,"results.html").toString();

        try {
            Files.delete(Paths.get(filename));
        } catch (IOException e) {
            e.printStackTrace();
        }

        Utils.WriteToFile(filename, output.toString());
//        HashMap<String, List<Document>> topoStates = new HashMap<String, List<Document>>();
//        for(String exp : experiment)
//            topoStates.put(exp.split("_")[1], mongoClient.getDocumentsFromDB("storm." + exp.replace("storm.", ""), null, null, 100));
//
//
//        //topoStates.put("Annealing", mongoClient.getDocumentsFromDB("storm." + experiment[1].replace("storm.", ""), null, null, 100));
//
//        tuplesPerSecond(topoStates, 1, "("+String.join(" vs ", topoStates.keySet())+")");

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
        //String[] experiment = {"patient_default_5242880_5_1_3-1-1455556218", "patient_our_1048576_6_1_3-2-1455803874"};
        //String[] experiment = {"patient_default_5242880_5_1_3-1-1455556218", "patient_our_1048576_10_1_3-3-1455804269"};
        //String[] experiment = {"patient_default_1048576_10_1_3-2-1455806915" , "patient_resource_1048576_10_1_3-1-1455810282" };
        //String[] experiment = {"patient_default_1048576_10_1_3-2-1455806915" , "patient_resource_5242880_10_1_3-2-1455811482" };
        String[] experiment = { "patient_default_1048576_5_1_3-1-1455815137", "patient_resource_1048576_5_1_3-1-1455813782"};


        String[] metrics = {"emitted" , "transferred", "executed" };
        Boolean average = true;

        HashMap<String, List<Document>> topoStates = new HashMap<String, List<Document>>();
        for(String exp : experiment)
            topoStates.put(exp.split("_")[1], mongoClient.getDocumentsFromDB("storm." + exp.replace("storm.", ""), null, null, 100));


        //topoStates.put("Annealing", mongoClient.getDocumentsFromDB("storm." + experiment[1].replace("storm.", ""), null, null, 100));

        tuplesPerSecond(topoStates, 1, "("+String.join(" vs ", topoStates.keySet())+")", "1.png");
        //averageLatency(topoStates);
    }

    public void tuplesPerSecond(HashMap<String, List<Document>> topoStates, int averagePeriod, String additionalTitle, String filename) {

        String[] metricNames = {/* "emitted" , "transferred",*/ "executed" };

        log.trace("Analysing data from DB");
        JavaPlot p = new JavaPlot();
        p.setTitle("Tuples per second "+additionalTitle);
        p.getAxis("x").setLabel("Processing time, s");
        p.getAxis("y").setLabel("Tuples per second");

        int topoCount=0;
        for (String schedulerName : topoStates.keySet()){
            topoCount++;
            HashMap<String, ArrayList<double[]>> dataSet = new HashMap<String, ArrayList<double[]>>();
            Double prevValue = 0.0;
            for (String metricName : metricNames){
                int i = 0;
                int j = 0;
                if (!dataSet.containsKey(metricName))
                    dataSet.put(metricName, new ArrayList<double[]>());
                Double oneMinuteDelta = 0.0;
                for (Document topoState : topoStates.get(schedulerName)){
                    Double tenSecondsDelta = 0.0;
                    //"completeLatency", new Document("$gt", 0)
                    String[] uptime = topoState.get("uptime").toString().split(" ");
                    Integer seconds = Integer.parseInt(uptime[uptime.length - 1].replace("s", ""));
                    if (uptime.length > 1)
                        seconds += 60 * Integer.parseInt(uptime[uptime.length - 2].replace("m", ""));
                    if (uptime.length > 2)
                        seconds += 3600 * Integer.parseInt(uptime[uptime.length - 3].replace("h", ""));

                    Double totalKeyValue = 0.0;
                    for (Document compDoc : ((ArrayList<Document>) topoState.get("components"))){
                        if(compDoc.containsKey(metricName)){
                            if (compDoc.containsKey("name") && compDoc.get("name").toString().contains("evaluate"))
                                totalKeyValue += Double.parseDouble(compDoc.get(metricName).toString());
                            if (compDoc.containsKey("boltId") && compDoc.get("boltId").toString().contains("evaluate"))
                                totalKeyValue += Double.parseDouble(compDoc.get(metricName).toString());
                        }

                    }
                    if(totalKeyValue>0) {
                        tenSecondsDelta = (totalKeyValue - prevValue);
                        oneMinuteDelta += (totalKeyValue - prevValue);
                    }
                    //if (average)
                    //totalKeyValue/= i*10.0;
                    dataSet.get(metricName).add(new double[]{i*10.0, tenSecondsDelta/10});
//                    if(i%5==i/5){
//                        dataSet.get(metricName).add(new double[]{j, oneMinuteDelta});
//                        oneMinuteDelta = 0.0;
//                        j++;
//                    }
                    prevValue = totalKeyValue;
                    i++;
                }

                ArrayList<double[]> setToDisplay = dataSet.get(metricName);
                if(setToDisplay.size()>0) {
                    if (averagePeriod > 1) {
                        ArrayList<double[]> averigedDataSet = new ArrayList<double[]>();
                        int k = 1;
                        int period = 3;
                        Double valueToAverage = 0.0;
                        for (double[] pair : dataSet.get(metricName)) {
                            valueToAverage += pair[1];
                            if (k == period) {
                                averigedDataSet.add(new double[]{pair[0], valueToAverage / period});
                                valueToAverage = 0.0;
                                k = 1;
                            }

                            k++;
                        }
                        setToDisplay = averigedDataSet;

                    }

                    DataSetPlot plot = new DataSetPlot(setToDisplay.toArray(new double[0][]));
                    plot.setTitle(schedulerName + "_" + metricName);
                    PlotStyle plotStyle = plot.getPlotStyle();
                    plotStyle.setStyle(Style.LINES);
                    plotStyle.setLineWidth(topoCount*2);
                    p.addPlot(plot);
                }
            }
    }

        //stl.setPointType(5);
        //stl.setPointSize(5);
        //p.addPlot("sin(x)");
        ImageTerminal png = new ImageTerminal();
        File file = new File(filename);
        try {
            file.createNewFile();
            png.processOutput(new FileInputStream(file));
        } catch (FileNotFoundException ex) {
            System.err.print(ex);
        } catch (IOException ex) {
            System.err.print(ex);
        }

        p.setTerminal(png);
        p.setPersist(false);
        p.plot();

        try {
            ImageIO.write(png.getImage(), "png", file);
        } catch (IOException ex) {
            System.err.print(ex);
        }
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

