import com.panayotis.gnuplot.JavaPlot;
import com.panayotis.gnuplot.plot.DataSetPlot;
import com.panayotis.gnuplot.style.PlotStyle;
import com.panayotis.gnuplot.style.Style;
import com.panayotis.gnuplot.terminal.ImageTerminal;
import ifmo.escience.dapris.monitoring.common.CommonMongoClient;
import ifmo.escience.dapris.monitoring.common.Utils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;

import javax.imageio.ImageIO;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

/**
 * Created by Pavel Smirnov
 */
public class StormMetricsAnalyzer {
    private static Logger log = LogManager.getLogger(StormMetricsAnalyzer.class);
    String baseUrl = "";
    private CommonMongoClient mongoClient;

    public static void main(String[] args){
        StormMetricsAnalyzer stormMetricsAnalyzer = new StormMetricsAnalyzer("http://192.168.92.11:8080/api/v1/topology/");
        //stormMetricsMonitor.getActualData();
        //stormMetricsMonitor.analyzeData();
        if(args.length>0 && args[0].equals("test"))
            stormMetricsAnalyzer.getExperimentsData();
        else
            stormMetricsAnalyzer.startAnalysis(5000);
        //stormMetricsMonitor.InsertDataToDb();
    }

    public StormMetricsAnalyzer(String baseUrl){
        this.baseUrl = baseUrl;
        String[] splitted = baseUrl.split("/");
        mongoClient = new CommonMongoClient();
    }

    public void getExperimentsData(){
        String path = "d:/Projects/MonitoringSystem/StormMetricsMonitor/target/";
        StringBuilder output = new StringBuilder();
        output.append("<style>\n");
        output.append("table {background: silver; cellspacing:1px; cellpadding:1px;}\n");
        output.append("td {background: white; white-space:nowrap;}\n");
        output.append("</style>\n");
        mongoClient.open();
        List<Document> experiments = mongoClient.getDocumentsFromDB("storm.experiments", new Document(){{ /*put("_id", "patient_3072_5_1_1");*/ }}, null, 100);
        for(Document experiment : experiments){
            StringBuilder tuplesPerSecond = new StringBuilder();
            String expId = experiment.get("_id").toString();
            //List<Document> runs = (List<Document>)experiment.get("runs");
            List<Document> runs = mongoClient.getDocumentsFromDB("storm.runs", new Document(){{
                put("expID", expId);
                //put("Finished", new Document(){{ put( "$exists:", true); }} );
            }}, new Document(){{ put("started", 1); }}, 100);

            if(runs.size()>0){
                output.append("<H1>"+experiment.getString("_id")+"</H1>\n");

                HashMap<String, List<Document>> topoStates = new HashMap<String, List<Document>>();
                int runI=0;

                ArrayList<String> runKeys = new ArrayList<String>();
                runKeys.add("_id");
                runKeys.add("started");
                runKeys.add("scheduler");
                runKeys.add("statSize");
                runKeys.add("tasksTotal");
                runKeys.add("workersTotal");
                runKeys.add("executorsTotal");
                runKeys.add("finished");
                runKeys.add("requestedMemOffHeap");
                runKeys.add("requestedCpu");
                runKeys.add("requestedTotalMem");
                runKeys.add("assignedCpu");
                runKeys.add("assignedMemOffHeap");
                runKeys.add("assignedTotalMem");
                int i=0;

                for(Document run : runs){
                    i++;
                    String scheduler = run.get("scheduler").toString();
                    String runID = run.get("_id").toString();
                    Document find = new Document() {{   put("topoID", runID);  }};

                    List<Document> runStats = mongoClient.getDocumentsFromDB("storm.statistics", find);
                    run.put("statSize", runStats.size());
                    if(runStats.size()>0){
                        String relPath = Paths.get("img", runID+".png").toString();
                        String absPath =  Paths.get(path,relPath).toString();
                        HashMap<String, ArrayList<double[]>> dataSets = prepareTuplesPerSecond(runStats, true);
                        CreatePlot(dataSets, absPath, "(# "+String.valueOf(i)+")");
                        tuplesPerSecond.append("<td><img src='"+relPath+"' style='display:inline; float:left;'></td>\n");
                    }

                    List<Document> factAssignments = mongoClient.getDocumentsFromDB("storm.FactAssignments", find);
                    Document structure = mongoClient.getDocumentFromDB("storm.Structure", find);
                    Document schedule = mongoClient.getDocumentFromDB("storm.Schedules", find);

                    String scheduleStr = "";
                    if(schedule!=null)
                        if(schedule.containsKey("nodes")){
                            for (Document node : (List<Document>) schedule.get("nodes")) {
                                scheduleStr += node.get("nodeId") + " => [";
                                if (node.get("tasks") != null)
                                    for (Object task : (List<Object>) node.get("tasks")){
                                        scheduleStr += ", "+((ArrayList<Object>) task).get(0);
                                    }
                                scheduleStr += "]<br>";
                            }
                        }


                    String factAssignmentsStr = "";
                    for (Document factAssignment : factAssignments){
                        if(factAssignment.get("assignment")!=null) {
                            for (Document assignment : (List<Document>) factAssignment.get("assignment")) {
                                factAssignmentsStr += assignment.get("nodeId") + " => [";
                                if (assignment.get("tasks") != null)
                                    for (Object task : (List<Document>) assignment.get("tasks")) {
                                        factAssignmentsStr += ", " + ((ArrayList<String>) task).get(0);
                                    }
                                factAssignmentsStr += "]<br>";
                            }
                            factAssignmentsStr += "<hr>";
                        }
                    }

//                    if(runStats.size()>0){
//                        //String key = scheduler;
//                        String key = run.get("_id").toString();
//                        if(topoStates.containsKey(key)){
//                            key+="_"+String.valueOf(runI);
//                            runI++;
//                        }
//                        topoStates.put(key, runStats);
//                    }

                    String th="";
                    String td="";

                    for(String key : run.keySet()){
                        if(!runKeys.contains(key))
                            runKeys.add(key);
                    }

                    //runsTable.append("<tr>"+th+"</tr>");
                    //runsTable.append("<tr>"+td+"</tr>");
                    //runsTable.append("<td>"+String.valueOf(runStats.size())+"</td>");
                    //runsTable.append("</tr>");
                    //runsTable.append("<tr><td>Schedule:</td><td colspan="+String.valueOf(run.keySet())+">"+ scheduleStr+"</td></tr>");
                    //runsTable.append("<tr><td>Fact assignment:</td><td colspan="+String.valueOf(run.keySet())+">"+factAssignmentsStr+"</td></tr>");
                }

                output.append("<h3>Runs</h3>");
                StringBuilder runsTable = new StringBuilder();
                runsTable.append("<table cellpadding=3 cellspacing=3>");
                runsTable.append("<th>#</th>");
                for(String key : runKeys)
                    runsTable.append("<th>"+key+"</th>");
                runsTable.append("</tr>");
                i=0;
                for(Document run : runs){
                    i++;
                    runsTable.append("<tr>");
                    runsTable.append("<td># "+String.valueOf(i)+"</td>");
                    for(String key : runKeys)
                        runsTable.append("<td>"+ (run.containsKey(key)?run.get(key):"")+"</td>");
                    runsTable.append("</tr>");
                }
                runsTable.append("</table>");

                output.append(runsTable);
                if(tuplesPerSecond.length()>0){
                    output.append("<h3>Tuples per second</h3>");
                    output.append("<table><tr>"+tuplesPerSecond+"</tr></table>");
                }




//                output.append("<h3>Run stats</h3>");
//                output.append("<table cellpadding=3 cellspacing=3>");
//                output.append("<tr>");
//                for(String key : runKeys)
//                    output.append("<th>"+key+"</th>");
//                output.append("</tr>");
//                for(Document run : runs){
//                    output.append("<tr>");
//                    for(String key : runKeys)
//                        output.append("<td>"+ (run.containsKey(key)?run.get(key):"")+"</td>");
//                    output.append("</tr>");
//                }
//                output.append("</table>");


                output.append("<hr>\n");
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
//        prepareTuplesPerSecond(topoStates, 1, "("+String.join(" vs ", topoStates.keySet())+")");

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

        //prepareTuplesPerSecond(topoStates, 1, "("+String.join(" vs ", topoStates.keySet())+")", "1.png");
        //averageLatency(topoStates);
    }

    public HashMap<String, ArrayList<double[]>> prepareTuplesPerSecond(List<Document>runStates, boolean componentsSeparately){

        String[] metricNames = { "emitted" , "transferred", "executed" };

        log.trace("Analysing data from DB");

        HashMap<String, ArrayList<double[]>> dataSets = new HashMap<String, ArrayList<double[]>>();

        for (String metricName : metricNames){
            String dataSetKey = null;
            Double prevValue = 0.0;
            //for (String metricName : metricNames){
            int i = 0;
            int j = 0;

            Double oneMinuteDelta = 0.0;
            for (Document runState : runStates){
                Double tenSecondsDelta = 0.0;
                //"completeLatency", new Document("$gt", 0)
                String[] uptime = runState.get("uptime").toString().split(" ");
                Integer seconds = Integer.parseInt(uptime[uptime.length - 1].replace("s", ""));
                if (uptime.length > 1)
                    seconds += 60 * Integer.parseInt(uptime[uptime.length - 2].replace("m", ""));
                if (uptime.length > 2)
                    seconds += 3600 * Integer.parseInt(uptime[uptime.length - 3].replace("h", ""));

                if(!componentsSeparately){
                    dataSetKey = metricName;
                    Double totalKeyValue = 0.0;
                    for (Document component : ((ArrayList<Document>) runState.get("components"))){

                        dataSetKey = component.get("name").toString()+ "_"+metricName;

                        if (!dataSets.containsKey(dataSetKey))
                            dataSets.put(dataSetKey, new ArrayList<double[]>());
                        ArrayList<double[]> dataSet = dataSets.get(dataSetKey);

                        if(component.containsKey(metricName)){
                            if (component.containsKey("name") && component.get("name").toString().contains("evaluate"))
                                totalKeyValue += Double.parseDouble(component.get(metricName).toString());
                        }
                    }
                    if(totalKeyValue>0) {
                        tenSecondsDelta = (totalKeyValue - prevValue);
                        oneMinuteDelta += (totalKeyValue - prevValue);
                    }
                    prevValue = totalKeyValue;
                    if (!dataSets.containsKey(dataSetKey))
                        dataSets.put(dataSetKey, new ArrayList<double[]>());
                    ArrayList<double[]> dataSet = dataSets.get(dataSetKey);

                    dataSet.add(new double[]{i*10.0, tenSecondsDelta/10});
                }else{  // per component
                    Double totalKeyValue = 0.0;
                    for (Document component : ((ArrayList<Document>) runState.get("components"))){
                        String name = component.get("name").toString();
                        if(name.contains("_")){
                            String [] splitted = name.split("_");
                            name = splitted[splitted.length-1];
                        }

                        if(component.containsKey(metricName)){
                            dataSetKey = name+ "_"+metricName;
                            if (!dataSets.containsKey(dataSetKey))
                                dataSets.put(dataSetKey, new ArrayList<double[]>());
                            ArrayList<double[]> dataSet = dataSets.get(dataSetKey);
                            if(dataSet.size()>0)
                                prevValue = dataSet.get(dataSet.size()-1)[1];
                            totalKeyValue = Double.parseDouble(component.get(metricName).toString());
                            tenSecondsDelta = (totalKeyValue - prevValue);
                            dataSet.add(new double[]{i*10.0, tenSecondsDelta/10});
                      }
                    }

                }
                //dataSet.add(new double[]{i*10.0, totalKeyValue/i/10});

                //if (average)
                //totalKeyValue/= i*10.0;

//                    if(i%5==i/5){
//                        dataSet.get(metricName).add(new double[]{j, oneMinuteDelta});
//                        oneMinuteDelta = 0.0;
//                        j++;
//                    }
                //prevValue = totalKeyValue;
                i++;
            }



//            ArrayList<double[]> setToDisplay = dataSet.get(metricName);
//            if(setToDisplay.size()>0){
//                if (averagePeriod > 1){
//                    ArrayList<double[]> averigedDataSet = new ArrayList<double[]>();
//                    int k = 1;
//                    int period = 3;
//                    Double valueToAverage = 0.0;
//                    for (double[] pair : dataSet.get(metricName)) {
//                        valueToAverage += pair[1];
//                        if (k == period) {
//                            averigedDataSet.add(new double[]{pair[0], valueToAverage / period});
//                            valueToAverage = 0.0;
//                            k = 1;
//                        }
//
//                        k++;
//                    }
//                    setToDisplay = averigedDataSet;
//
//                }
//            }
        }
        return dataSets;
    }

    public void CreatePlot(HashMap<String, ArrayList<double[]>> dataSets, String filename,  String additionalTitle){

        JavaPlot p = new JavaPlot();
        p.setTitle("Tuples per second "+additionalTitle);
        p.getAxis("x").setLabel("Processing time, s");
        p.getAxis("y").setLabel("Tuples per second");
        String [] sorted = dataSets.keySet().toArray(new String[0]);
        Arrays.sort(sorted);
        for(String key : sorted) {
            ArrayList<double[]> dataSet = dataSets.get(key);
            DataSetPlot plot = new DataSetPlot(dataSet.toArray(new double[0][]));
            plot.setTitle(key);
            PlotStyle plotStyle = plot.getPlotStyle();
            plotStyle.setStyle(Style.LINES);
            //plotStyle.setLineWidth(topoCount*2);
            p.addPlot(plot);
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

    public void startAnalysis(int sleepInterval){
        log.info("Monitoring starting");
        while(1==1){
            try {
                getExperimentsData();
                log.trace("Sleeping " + sleepInterval+"ms");
                Thread.sleep(sleepInterval);
            } catch (InterruptedException e){
                log.error(e);
            }
        }
    }
}
