import ifmo.escience.dapris.monitoring.common.CommonMongoClient;
import ifmo.escience.dapris.monitoring.common.CommonSSHClient;
import ifmo.escience.dapris.monitoring.common.Utils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.bson.Document;
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

/**
 * Created by Pavel Smirnov
 */
public class StormSchedulerExperiments {
    private static Logger log = LogManager.getLogger(StormSchedulerExperiments.class);
    private CommonMongoClient mongoClient;

    String experimentsCollection = "storm.experiments";
    String runsCollection = "storm.runs";
    String masterHost = "192.168.92.11";
    private StormAPIClient stormAPIClient;
    String[] slaveHosts = new String[]{"192.168.92.13", "192.168.92.14", "192.168.92.15", "192.168.92.16"};
    private String[] runStatKeys = new String[]{"tasksTotal","workersTotal", "executorsTotal", "requestedMemOffHeap" ,"requestedCpu", "requestedTotalMem", "assignedCpu","assignedMemOffHeap", "assignedTotalMem","schedulerInfo" };
    private HashMap<String, CommonSSHClient> sshClients = new HashMap<String, CommonSSHClient>();
    private HashMap<String, String> schedulers = new HashMap<String, String>();
    private String paramString;
    private String sshKey = "";
    private String sshPath = "d:\\Projects\\MonitoringSystem\\StormMetricsMonitor\\resources\\id_rsa";

    public static void main(String[] args){
        StormSchedulerExperiments stormMetricsMonitor = new StormSchedulerExperiments("http://192.168.92.11:8080");
        //stormMetricsMonitor.killAllProcesses();
        stormMetricsMonitor.startExperiments(60*5);
     }

    public StormSchedulerExperiments (String base_Url){
        sshKey = CommonSSHClient.ReadSShKey(sshPath);
//        String[] splitted = baseUrl.split("/");
        mongoClient = new CommonMongoClient();
        stormAPIClient = new StormAPIClient(base_Url);
        schedulers.put("default", "org.apache.storm.scheduler.annealing.DefaultSchedulerEmulator");
        schedulers.put("resource", "org.apache.storm.scheduler.resource.ResourceAwareScheduler");
        schedulers.put("annealing", "org.apache.storm.scheduler.annealing.AnnealingScheduler");
    }

    public void startExperiments(int seconds) {

        String debugTopoID = null; // "patient0_10240_10_1_3_10_128-64_10_20_1024-1-1456421934";//
        if (debugTopoID == null)
            killTopologies();

        log.info("Starting experiments");
        int i = 0;
        Document experiment;


        for (String scheduler : new String[]{

                "annealing",
                "resource",
                "default"
        }){
            Double[] onheapMBs = new Double[]{ /*32.0 */ 64.0 };
            Double[] offheapMBs = new Double[]{/*5.0,*/ 10.0};
            int[] cpuPerComps = new int[]{/*10 20 */ 30};
            int[] workerMaxHeapSizeMbs = new int[]{ 1024 };


            for (Double onheapMB : (scheduler.equals("resource")? onheapMBs: new Double[]{null}))
                for (Double offheapMB : (scheduler.equals("resource")? offheapMBs: new Double[]{null}))
                    for (int cpuPerComp : (scheduler.equals("resource")? cpuPerComps: new int[]{ 0 }))
                        for (int maxHeapSizeMb : (scheduler.equals("resource")? workerMaxHeapSizeMbs: new int[]{ 0 })){

                            String additionalRAparams = "";
                            HashMap<String, Object> configParams = new HashMap<String, Object>();
                            configParams.put("storm.scheduler", schedulers.get(scheduler));
                            if(scheduler.equals("resource")){
                                configParams.put("topology.component.resources.onheap.memory.mb", onheapMB.toString());
                                configParams.put("topology.component.resources.offheap.memory.mb", offheapMB.toString());
                                configParams.put("topology.component.cpu.pcore.percent", cpuPerComp);
                                configParams.put("topology.worker.max.heap.size.mb", maxHeapSizeMb);
                                configParams.put("worker.heap.memory.mb", maxHeapSizeMb);
                                additionalRAparams = "-" + String.format("%.0f", onheapMB) + "_" + String.format("%.0f", offheapMB) + "_" + cpuPerComp + "_" + maxHeapSizeMb;
                            }
                            SetConfigParams(configParams);

                            for (int workers : new int[]{10 /*, 15*/})
                                for (int emitters : new int[]{1})
                                    for (int processors : new int[]{/*1 */ /*3*/ /*, 5,*/ 5 })
                                        for (int megabytes : new int[]{/*3*/ 5 })
                                            for (int cpuKoef : new int[]{ 10 /*, 40*/ })
                                                for (int memKoef : new int[]{ 128 }){


                            Integer kbsize = 1024 * megabytes;
                            paramString = kbsize + " " + workers + " " + emitters + " " + processors+ " "+cpuKoef+" "+memKoef;

                            String originalTopoName = "patient";
                            String expId = originalTopoName + "_" + paramString.replace(" ", "_");
                            String topoName = originalTopoName + String.valueOf(i) + "_" + paramString.replace(" ", "_")+additionalRAparams;

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
                            JSONObject topologyInfo = stormAPIClient.getTopologyByName(topoName,true);
                            if (debugTopoID != null)
                                topoId = debugTopoID;
                            else
                                topoId = topologyInfo.get("id").toString();

                            Document runFind = new Document() {{ put("_id", topoId); }};
                            run.put("_id", topoId);
                            if (debugTopoID == null) {
                                mongoClient.insertDocumentToDB(runsCollection, run);
                                mongoClient.updateDocumentInDB(experimentsCollection, expFind, experiment);
                            }

                            log.info("Waiting for run statistics become not empty");
                            Document topoStat = null;
                            while(topoStat==null) {
                                topoStat =  stormAPIClient.getTupleStatsByID(topoId);
                                Utils.Wait(5000);
                            }




                            log.info("Exp(" + String.valueOf(i) + "): running " + topoId + " with " + scheduler + " scheduler for " + String.valueOf(seconds) + " seconds");

                            String schedInfo = "";
                            int wait = 5000;
                            while (new Date().getTime() - runStarted.getTime() < seconds * 1000) {

                                topologyInfo = stormAPIClient.getTopologyByName(topoName, false);
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
                                Utils.Wait(wait);
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





//    public void switchScheduler(String name){
//        log.info("Switching scheduler to "+name);
//
//        String oldConfig = executeCommand(masterHost,"sudo cat /opt/storm/defaults.yaml");
//        for (String schedName: schedulers.keySet())
//            if(!schedName.equals(name)){
//                executeCommand(masterHost, "sudo sed -i \"s/"+schedulers.get(schedName)+"/"+schedulers.get(name)+"/g\" /opt/storm/defaults.yaml");
//            }
//        //Wait(1000);
//        String newConfig = executeCommand(masterHost,"sudo cat /opt/storm/defaults.yaml");
//        if(!newConfig.equals(oldConfig))
//            executeCommand(masterHost,"sudo kill `ps -aux | grep nimbus | awk '{print $2}'`");
//        //executeCommand(masterHost,"sudo service supervisor restart`");
//        getSupervisorIDs();
//    }
    public String executeCommand(String host, String command){
        CommonSSHClient client;

        if(!sshClients.containsKey(host)){
            client = new CommonSSHClient(host,"vagrant",sshKey);
            sshClients.put(host, client);
        }else
            client = sshClients.get(host);
        return client.executeCommand(command);
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
        stormAPIClient.getSupervisorIDs();
    }

    public void submitTopology(String topoName, String paramString){
        log.info("Submitting topology "+topoName);
        String command = "/opt/storm/bin/storm jar /opt/storm-examples/seizure-light-2.0.0-SNAPSHOT-jar-with-dependencies.jar seizurelight.SeizurePredictionTopology "+ topoName+" "+paramString;
        log.info(command);
        executeCommand(masterHost, command);

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
        for(Object topology : stormAPIClient.getTopologies()){
            if(!((JSONObject)topology).get("status").toString().equals("KILLED")){
                killTopology(((JSONObject)topology).get("name").toString());
                i++;
            }
        }
        if(i>0){
            killAllProcesses();
            log.info("Waiting for empty topologies list");
            while (stormAPIClient.getTopologies().length()>0)
                Utils.Wait(5000);
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



}
