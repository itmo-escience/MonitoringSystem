import ifmo.escience.dapris.monitoring.common.CommonMongoClient;
import com.jcabi.ssh.Shell;
import com.jcabi.ssh.SSH;
import ifmo.escience.dapris.monitoring.common.Utils;
import org.apache.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ObjectArrayMessage;
import org.bson.Document;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.UnknownHostException;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;

/**
 * Created by Pavel Smirnov
 */
public class StormSchedulerExperiments {
    private static Logger log = LogManager.getLogger(StormSchedulerExperiments.class);
    private CommonMongoClient mongoClient;
    private String sshKey = "";
    String experimentsCollection = "storm.experiments";
    String runsCollection = "storm.runs";
    String masterHost = "192.168.92.11";
    String baseUrl = "http://192.168.92.11:8080/api/v1/";
    String[] slaveHosts = new String[]{"192.168.92.13", "192.168.92.14", "192.168.92.15", "192.168.92.16"};
    private String[] runStatKeys = new String[]{"tasksTotal","workersTotal", "executorsTotal", "requestedMemOffHeap" ,"requestedCpu", "requestedTotalMem", "assignedCpu","assignedMemOffHeap", "assignedTotalMem","schedulerInfo" };
    private HashMap<String, Shell.Plain> shellPlains = new HashMap<String, Shell.Plain>();
    private HashMap<String, String> schedulers = new HashMap<String, String>();

    public static void main(String[] args){
        org.apache.log4j.Logger mongoLogger = org.apache.log4j.Logger.getLogger("org.mongodb.driver.cluster");
        mongoLogger.setLevel(Level.OFF);

        StormSchedulerExperiments stormMetricsMonitor = new StormSchedulerExperiments("http://192.168.92.11:8080/api/v1/topology/");
        //stormMetricsMonitor.killAllProcesses();
        stormMetricsMonitor.startExperiments(180);
     }

    public StormSchedulerExperiments (String baseUrl){
        sshKey = ReadSShKey();
//        this.baseUrl = baseUrl;
//        String[] splitted = baseUrl.split("/");
        mongoClient = new CommonMongoClient();
        mongoClient.open();
        schedulers.put("default", "org.apache.storm.scheduler.annealing.DefaultSchedulerEmulator");
        schedulers.put("resource", "org.apache.storm.scheduler.resource.ResourceAwareScheduler");
        schedulers.put("annealing", "org.apache.storm.scheduler.annealing.AnnealingScheduler");
    }

    public void startExperiments(int seconds) {

        String debugTopoID = null;//
        if (debugTopoID == null)
            killTopologies();

        log.info("Starting experiments");
        int i = 0;
        Document experiment;


        for (String scheduler : new String[]{
                //"default"
                "resource"
                //"annealing"
        }){
            Double[] onheapMBs = new Double[]{32.0 /*, 64.0*/};
            Double[] offheapMBs = new Double[]{/*5.0,*/ 10.0};
            int[] cpuPerComps = new int[]{5 /*, 10 , 20, 30*/};
            int[] maxHeapSizeMbs = new int[]{9000};


            for (Double onheapMB : (scheduler.equals("resource")? onheapMBs: new Double[]{null}))
                for (Double offheapMB : (scheduler.equals("resource")? offheapMBs: new Double[]{null}))
                    for (int cpuPerComp : (scheduler.equals("resource")? cpuPerComps: new int[]{ 0 }))
                        for (int maxHeapSizeMb : (scheduler.equals("resource")? maxHeapSizeMbs: new int[]{ 0 })){

                            String additionalRAparams = "";
                            HashMap<String, Object> configParams = new HashMap<String, Object>();
                            configParams.put("storm.scheduler", schedulers.get(scheduler));
                            if(scheduler.equals("resource")){
                                configParams.put("topology.component.resources.onheap.memory.mb", onheapMB.toString());
                                configParams.put("topology.component.resources.offheap.memory.mb", offheapMB.toString());
                                configParams.put("topology.component.cpu.pcore.percent", cpuPerComp);
                                configParams.put("topology.worker.max.heap.size.mb", maxHeapSizeMb);
                                additionalRAparams = "-" + String.format("%.0f", onheapMB) + "_" + String.format("%.0f", offheapMB) + "_" + cpuPerComp + "_" + maxHeapSizeMb;
                            }
                            SetConfigParams(configParams);

                            for (int workers : new int[]{5, 10 /*, 15*/})
                                for (int emitters : new int[]{1})
                                    for (int processors : new int[]{1 /*, 3, 5, 8*/})
                                        for (int megabytes : new int[]{3 /*, 5, 10 */})
                                            for (int cpuKoef : new int[]{10, 20 })
                                                for (int memKoef : new int[]{128, 256}){


                            Integer kbsize = 1024 * megabytes;
                            String paramString = kbsize + " " + workers + " " + emitters + " " + processors+ " "+cpuKoef+" "+memKoef;

                            String originalTopoName = "patient";
                            String expId = originalTopoName + "_" + paramString.replace(" ", "_");
                            String topoName = originalTopoName + String.valueOf(i) + "_" + paramString.replace(" ", "_") + additionalRAparams;

                            Document expFind = new Document() {{
                                put("_id", expId);
                            }};

                            experiment = mongoClient.getDocumentFromDB(experimentsCollection, expFind);

                            if (experiment == null) {
                                experiment = new Document();
                                experiment.put("_id", expId);
                                experiment.put("kbSize", kbsize);
                                experiment.put("workers", workers);
                                experiment.put("emitters", emitters);
                                experiment.put("processors", processors);
                                experiment.put("cpuKoef", emitters);
                                experiment.put("memKoef", memKoef);
                                if(scheduler.equals("resource")) {
                                    experiment.put("onheapMb", String.format("%.1f", onheapMB));
                                    experiment.put("offheapMb", String.format("%.1f", offheapMB));
                                    experiment.put("corePercent", cpuPerComp);
                                    experiment.put("maxHeapSizeMb", maxHeapSizeMb);
                                }
                                experiment.put("updated", new Date());
                                experiment.putAll(expFind);
                                experiment.put("runs", new ArrayList<Document>());
                                mongoClient.insertDocumentToDB(experimentsCollection, experiment);
                            }

                            ArrayList<Document> expRuns = ((ArrayList<Document>) experiment.get("runs"));

                            Document run = new Document();
                            expRuns.add(run);

                            Date runStarted = new Date();
                            experiment.put("updated", runStarted);
                            run.put("started", runStarted);
                            run.put("expID", expId);
                            //run.put("topoName", topoName);
                            run.put("scheduler", scheduler);

                            String topoId;

                            if (debugTopoID == null)
                                submitTopology(topoName, paramString);

                            log.info("Waiting for topology info");
                            JSONObject topologyInfo = getTopologyById(topoName);
                            if (debugTopoID != null)
                                topoId = debugTopoID;
                            else
                                topoId = topologyInfo.get("id").toString();

                            run.put("_id", topoId);

                            mongoClient.insertDocumentToDB(runsCollection, run);
                            mongoClient.updateDocumentInDB(experimentsCollection, expFind, experiment);

                            Document runFind = new Document() {{
                                put("_id", topoId);
                            }};

                            log.info("Exp(" + String.valueOf(i) + "): running " + topoId + " with " + scheduler + " scheduler for " + String.valueOf(seconds) + " seconds");

                            String schedInfo = "";
                            int wait = 5000;
                            while (new Date().getTime() - runStarted.getTime() < seconds * 1000) {

                                topologyInfo = getTopologyById(topoName);
                                int changes = 0;
                                for (String statKey : runStatKeys) {
                                    Object newValue = topologyInfo.get(statKey);
                                    if (newValue != null && !newValue.toString().toString().equals("null")) {

                                        Object runValue = null;
                                        if (run.containsKey(statKey))
                                            runValue = run.get(statKey);
                                        if (runValue == null || !runValue.toString().equals(newValue.toString())) {
                                            run.put(statKey, newValue);
                                            changes++;
                                        }
                                    }
                                }
                                if (changes > 0)
                                    mongoClient.updateDocumentInDB(runsCollection, runFind, run);

//                                if (topologyInfo.get("schedulerInfo").toString().contains("Not enough resources")){
//                                    log.info(schedInfo);
//                                    wait=1;
//                                    break;
//                                }
                                Wait(wait);
                            }

                            Date runFinished = new Date();
                            run.put("finished", runFinished);

                            experiment.put("updated", runFinished);
                            mongoClient.updateDocumentInDB(runsCollection, runFind, run);
                            killTopology(topoName);
                            killAllProcesses();
                            capturePicture(topoId);
                            i++;
                                        }


                        }
            }
        killTopologies();
        log.info(String.valueOf(i)+" experiments finished");
    }


    public JSONObject getTopologyById(String topoName){
        log.trace("Getting topology by name");
        JSONObject ret=null;
        Integer wait = 5000;
        while(ret==null){
            try{

                JSONArray topologies = getTopologies();
                for (Object topology : topologies){
                    if(((JSONObject)topology).get("name").toString().equals(topoName) && ((JSONObject)topology).get("status").toString().equals("ACTIVE")){
                        ret =  (JSONObject)topology;
                        wait=1;
                        break;
                    }
                }

            }
            catch (Exception e){

            }
            Wait(wait);
        }
        return ret;
    }


    public String ReadSShKey(){
        String ret = "";
        BufferedReader br = null;
        try {

            String sCurrentLine;

            br = new BufferedReader(new FileReader("d:\\Projects\\MonitoringSystem\\StormMetricsMonitor\\resources\\id_rsa"));
            while ((sCurrentLine = br.readLine()) != null) {
                if(ret!="")ret+="\n";
                ret+=sCurrentLine;
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
        return ret;
    }

    public String executeCommand(String host, String command){
        String ret = "";
        log.trace(host+" -> "+command);

        Shell.Plain shellPlain=null;
        if(shellPlains.containsKey(host))
            shellPlain = shellPlains.get(host);

        log.trace("Waiting for ssh conection to "+host);
        Integer wait = 2000;
        while (shellPlain==null){
            try {
                SSH hostShell = new SSH(host, 22, "vagrant", sshKey);
                shellPlain = new Shell.Plain(hostShell);
                if(!shellPlains.containsKey(shellPlain))
                    shellPlains.put(host,shellPlain);
                wait=1;
            } catch (UnknownHostException e) {
                //e.printStackTrace();

            }
            Wait(wait);
        }
        Boolean executed=false;
        log.trace("Waiting for command executon on "+host);
        wait = 2000;
        while(!executed) {
            try {
                ret = shellPlain.exec(command);
                executed = true;
                wait = 1;
            } catch (IOException e) {

            }
            Wait(wait);
        }
        return ret;
    }

    public void switchScheduler(String name){
        log.info("Switching scheduler to "+name);

        String oldConfig = executeCommand(masterHost,"sudo cat /opt/storm/defaults.yaml");
        for (String schedName: schedulers.keySet())
            if(!schedName.equals(name)){
                executeCommand(masterHost, "sudo sed -i \"s/"+schedulers.get(schedName)+"/"+schedulers.get(name)+"/g\" /opt/storm/defaults.yaml");
            }
        //Wait(1000);
        String newConfig = executeCommand(masterHost,"sudo cat /opt/storm/defaults.yaml");
        if(!newConfig.equals(oldConfig))
            executeCommand(masterHost,"sudo kill `ps -aux | grep nimbus | awk '{print $2}'`");
        //executeCommand(masterHost,"sudo service supervisor restart`");
        getSupervisorIDs();
    }

    public void SetConfigParams (HashMap<String, Object> newParams){
        log.info("Setting config parameters");
        String[] oldConfig = executeCommand(masterHost,"sudo cat /opt/storm/defaults.yaml").split("\r\n");

        HashMap<String, String> linesToReplace = new HashMap<String, String>();
        for(String line : oldConfig){
            for(String param : newParams.keySet()) {
                String value = newParams.get(param).toString();
                if(line.contains("\""))
                    value = "\""+value+"\"";
                String newline=param + ": " + value;
                if (line.startsWith(param+":") && !line.equals(newline))
                    linesToReplace.put(line, newline);
            }
        }

        if(linesToReplace.size()>0){
            for(String lineToReplace : linesToReplace.keySet()){
                String command = "sudo sed -i \'s/"+lineToReplace+"/"+linesToReplace.get(lineToReplace)+"/g\' /opt/storm/defaults.yaml";
                executeCommand(masterHost, command);
            }
            log.info("Restarting nimbus");
            executeCommand(masterHost,"sudo kill `ps -aux | grep nimbus | awk '{print $2}'`");
        }
        //executeCommand(masterHost,"sudo service supervisor restart`");
        getSupervisorIDs();
    }

    public void submitTopology(String topoName, String paramString){
        log.info("Submitting topology "+topoName);
        String command = "/opt/storm/bin/storm jar /opt/storm-examples/seizure-light-2.0.0-SNAPSHOT-jar-with-dependencies.jar seizurelight.SeizurePredictionTopology "+ topoName+" "+paramString;
        executeCommand(masterHost, command);

    }

    public JSONArray getTopologies(){
        log.trace("Waiting for topologies list");
        JSONArray topologies = WaitForJSON(baseUrl + "topology/summary", "topologies");

        return topologies;
    }

    public List<String> getSupervisorIDs(){
        log.info("Waiting for supervisors list");
        List<String> ret = new ArrayList<String>();
        JSONArray topologies = WaitForJSON(baseUrl + "supervisor/summary", "supervisors");
        for(Object topology : topologies){
            String id = ((JSONObject) topology).get("id").toString();
            ret.add(id);
        }
        return ret;
    }

    public void capturePicture(String topoId){
        String path = "d:/Projects/MonitoringSystem/StormMetricsMonitor/target/img/load";
        try {
            URL website = new URL("http://192.168.92.11/ganglia/stacked.php?m=load_one&c=ds1&r=hour&st=1455921590");
            ReadableByteChannel rbc = Channels.newChannel(website.openStream());
            FileOutputStream fos = new FileOutputStream(path+"/"+topoId+".png");
            fos.getChannel().transferFrom(rbc, 0, Long.MAX_VALUE);
        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void killTopologies(){
        log.info("Killing existing topologies");
        int i=0;
        for(Object topology : getTopologies()){
            if(!((JSONObject)topology).get("status").toString().equals("KILLED")){
                killTopology(((JSONObject)topology).get("name").toString());
                i++;
            }
        }
        if(i>0){
            killAllProcesses();
            log.info("Waiting for empty topologies list");
            while (getTopologies().length()>0)
                Wait(5000);
        }
    }
    public void killTopology(String topologyName){
        log.info("Killing topology "+topologyName);
        executeCommand(masterHost,"/opt/storm/bin/storm kill "+topologyName);
    }

    public void killAllProcesses(){
        log.info("Killing worker processes");
        for (String slaveHost: slaveHosts) {
            executeCommand(slaveHost, "sudo killall java && sudo rm -rf /opt/storm/storm-local");
            //executeCommand(slaveHost,"sudo rm -rf /opt/storm/storm-local");
        }
//        Integer topos = 1;
//        Integer wait = 5000;
//        while(topos>0){
//            try {
//                JSONArray topologies = WaitForJSON(baseUrl + "topology/summary", "topologies");
//                topos = topologies.length();
//                if(topos==0)
//                    wait=1;
//            } catch (Exception e) {
//
//            }
//            Wait(wait);
//        }
    }


    public JSONArray WaitForJSON(String url, String property){
        //log.trace("Waiting for "+property);
        int i=0;
        int wait=1000;
        Boolean restartExecuted = false;
        JSONArray topologies = null;
        while(topologies==null) {
            try{
                JSONObject json = Utils.getJsonFromUrl(url);
                if(json!=null){
                    topologies = (JSONArray) json.get(property);
                    wait = 1;
                }
            }
            catch (Exception e){

            }
            if(i>0 && i%60==i/60 && wait>1 && !restartExecuted){
                log.info("Trying to restart nimbus");
                executeCommand(masterHost,"sudo service supervisor restart");
                restartExecuted = true;
            }
                //executeCommand(masterHost,"cd - && sudo kill `ps -aux | grep nimbus | awk '{print $2}'`");
            i++;
            Wait(wait);
        }
        return topologies;
    }

    public void Wait(int milliseconds){
        try {
            Thread.sleep(milliseconds);
        }
        catch (InterruptedException e2){

        }
    }
}
