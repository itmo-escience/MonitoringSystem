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
import org.apache.logging.log4j.core.config.Scheduled;
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
            stormMetricsMonitor.getActualData();
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

