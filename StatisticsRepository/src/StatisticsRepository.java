import StateStructures.ClusterState;
import StateStructures.Slave;
import com.sun.org.apache.xml.internal.security.Init;
import ifmo.escience.dapris.common.data.IRepository;
import ifmo.escience.dapris.common.data.Uow;
import ifmo.escience.dapris.common.entities.*;
import ifmo.escience.dapris.common.helpers.NodeStateDateComparator;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.*;


public class StatisticsRepository implements IRepository {

    private List<NetworkType> networkTypes = new ArrayList<NetworkType>();
    private List<Network> networks = new ArrayList<Network>();
    private List<Node> nodes = new ArrayList<Node>();
    private List<NodeState> nodeStates = new ArrayList<NodeState>();
    private List<DataLayer> layers = new ArrayList<DataLayer>();
    private List<Data> data = new ArrayList<Data>();
    private List<TaskType> taskTypes = new ArrayList<TaskType>();
    private List<Task> tasks = new ArrayList<Task>();
    private int nodesNumber = 0;
    private CommonMongoClient mongoClient;
    private ClusterStateMonitor clusterStateMonitor;
    private DstorageMonitor dstorageMonitor;
    private MetricsMonitor metricsMonitor;
    private ClusterState clusterState;

    public static void main(String[] args){
        StatisticsRepository repo = new StatisticsRepository(new CommonMongoClient());
        Uow.instance.repo = repo;

        List<Task> tasks = new ArrayList<Task>(repo.getAllTasks());
        List<Node> nodes = new ArrayList<Node>(repo.getAllNodes());

        for (Task task : tasks){
            List<NodeState> nodeStates = task.getNodeStates();
        }
        String test = "123";

    }

    public StatisticsRepository(){

    }
    public StatisticsRepository(CommonMongoClient mongoClient){
        this.mongoClient = mongoClient;
        this.clusterStateMonitor = new ClusterStateMonitor("192.168.92.11", mongoClient);
        this.metricsMonitor = new MetricsMonitor(mongoClient);
        this.dstorageMonitor = new DstorageMonitor(mongoClient);

        ClusterState state = clusterStateMonitor.getClusterStateFromDB();
        nodes = getNodes(state);
        tasks = getTasks(state);
        InitDataLayers();
    }

    public void InitDataLayers(){
        layers = new ArrayList<DataLayer>();
        double readSpeed = 0;
        double writeSpeed = 0;
        double totalSize = 0;

        String nodeId = null;
        layers.add(new DataLayer("0", "HDD", nodeId, readSpeed, writeSpeed, totalSize));
        layers.add(new DataLayer("1", "SSD", nodeId, readSpeed, writeSpeed, totalSize));
        layers.add(new DataLayer("2", "RAM", nodeId, readSpeed, writeSpeed, totalSize));

        //dstorageMonitor.GetLeveledRequests();
    }

    public List<ifmo.escience.dapris.common.entities.Node> getNodes(ClusterState state){
        ArrayList<ifmo.escience.dapris.common.entities.Node> ret = new ArrayList<ifmo.escience.dapris.common.entities.Node>();
        for(Slave slave : state.getSlaves()){
            String id = slave.getId();
            String name =  slave.getPid();
            String ip = slave.getHostname();
            String parentNodeId = null;
            String networkId = "networkID";
            Map<String, Object> resourceMap = slave.getResourceMap();
            Double cpuTotal = Double.parseDouble(resourceMap.get("cpus").toString());
            Double memoryTotal = Double.parseDouble(resourceMap.get("mem").toString());
            Double gpuTotal = 0.0;
            ifmo.escience.dapris.common.entities.Node node = new ifmo.escience.dapris.common.entities.Node(id, name, ip, parentNodeId, cpuTotal, memoryTotal, gpuTotal, networkId);
            ret.add(node);
        }
        return ret;
    }

    public List<Task> getTasks(ClusterState state){
        List<Task> ret = new ArrayList<ifmo.escience.dapris.common.entities.Task>();
        int i=0;
        for (StateStructures.Framework framework : state.getFrameworks()){
            String typeId = framework.getName();
            for (StateStructures.Task task : framework.getTasks()){

                Random rand = new Random(i);
                HashSet<String> parentTaskIds = null;

                Hashtable<String, Double> parameters = new Hashtable<>();
                LocalDateTime started = null;
                LocalDateTime finished = null;
                try {
                    started = LocalDateTime.ofInstant(task.getStarted().toInstant(), ZoneId.systemDefault());
                }
                catch (Exception e){
                    String test="123";
                }
                try {
                    finished = LocalDateTime.ofInstant(task.getFinished().toInstant(), ZoneId.systemDefault());
                }
                catch (Exception e){
                    String test="123";
                }

                String nodeId = task.getSlaveId();
                TaskStatus status = null;
                HashSet<Data> inData = new HashSet<>();
                HashSet<Data> outData = new HashSet<>();

                Task addTask = new Task(task.getId(), parentTaskIds, typeId, parameters, nodeId, status, started, finished, inData, outData);
                ret.add(addTask);
                i++;
            }
        }
        return ret;
    }

    @Override
    public Set<Data> getAllData() {
        return new HashSet<>(data);
    }

    @Override
    public Set<Data> getDataByLayer(String layerId) {
        HashSet<Data> result = new HashSet<>();
        for (Data item : data){
            if(item.getLayerId() == layerId){
                result.add(item);
            }
        }
        return result;
    }

    @Override
    public Set<Data> getDataByLayer(DataLayer layer) {
        HashSet<Data> result = new HashSet<>();
        for (Data item : data){
            if(item.getLayer().equals(layer)){
                result.add(item);
            }
        }
        return result;
    }

    @Override
    public Set<Data> getDataByNode(int nodeId) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Set<Data> getDataByNode(Node node) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Data getDataById(String id) {
        for (Data item : data){
            if(item.getId() == id){
                return item;
            }
        }
        return null;
    }

    @Override
    public Data getDataByName(String name) {
        for (Data item : data){
            if(item.getName() == name){
                return item;
            }
        }
        return null;
    }

    @Override
    public Set<DataLayer> getAllDataLayers(){
        return new HashSet<>(layers);
    }

    @Override
    public DataLayer getLayerById(String id) {
        for (DataLayer item : layers){
            if(item.getId() == id){
                return item;
            }
        }
        return null;
    }

    @Override
    public Set<DataLayer> getLayersByNode(String nodeId) {
        HashSet<DataLayer> result = new HashSet<>();
        for (DataLayer item : layers){
            if(item.getNodeId() == nodeId){
                result.add(item);
            }
        }
        return result;
    }

    @Override
    public Set<DataLayer> getLayersByNode(Node node) {
        HashSet<DataLayer> result = new HashSet<>();
        for (DataLayer item : layers){
            if(item.getNode().equals(node)){
                result.add(item);
            }
        }
        return result;
    }

    @Override
    public Set<Node> getAllNodes() {
       return new HashSet<>(nodes);
    }

    @Override
    public Set<Node> getChildNodes(String parentId) {
        HashSet<Node> result = new HashSet<>();
        for(Node node : nodes){
            if(node.getParentId().equals(parentId))
                result.add(node);
        }
        return result;
    }

    @Override
    public Node getNodeById(String id) {
        for(Node node : nodes){
            if(node.getId().equals(id))
                return node;
        }
        return null;
    }

    @Override
    public Node getNodeByName(String name) {
        for(Node node : nodes){
            if(node.getName().equals(name))
                return node;
        }
        return null;
    }

    @Override
    public List<NodeState> getNodeStates(String name) {
        ArrayList<NodeState> result = new ArrayList<>();
        for(NodeState state : nodeStates){
            if(state.getNode().getName() == name)
                result.add(state);
        }
        Collections.sort(result, new NodeStateDateComparator());
        return result;
    }


    @Override
    public List<NodeState> getNodeStateForPeriod(String nodeId, LocalDateTime start, LocalDateTime finish){
        Node node = getNodeById(nodeId);
        ArrayList<NodeState> ret = new ArrayList<>();
        String hostname = node.getIp();
//        hostname = "node-92-16";
//        starttime = LocalDateTime.now().minusDays(4);
//        endtime = LocalDateTime.now();
        TreeMap<LocalDateTime, TreeMap<String, Object>> metrics = metricsMonitor.GetMetricsFromDb(hostname, start, finish);
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


    @Override
    public Set<Task> getAllTasks(){
        return new HashSet<>(tasks);
    }

    @Override
    public Task getTaskById(String id){
        for(Task task : tasks){
            if(task.getId() == id)
                return task;
        }
        return null;
    }

    @Override
    public Set<Task> getTasksByType(TaskType type) {
        HashSet<Task> result = new HashSet<>();
        for(Task task : tasks){
            if(task.getType().equals(type))
                result.add(task);
        }
        return result;
    }

    @Override
    public Set<Task> getTasksByType(String typeId) {
        HashSet<Task> result = new HashSet<>();
        for(Task task : tasks){
            if(task.getTypeId() == typeId)
                result.add(task);
        }
        return result;
    }

    @Override
    public Set<TaskType> getAllTaskTypes() {
        return new HashSet<>(taskTypes);
    }

    @Override
    public TaskType getTaskTypeById(String id) {
        for(TaskType type : taskTypes){
            if(type.getId() == id)
                return type;
        }
        return null;
    }

    @Override
    public Set<NetworkType> getAllNeworkTypes() {
        return new HashSet<>(networkTypes);
    }

    @Override
    public NetworkType getNetworkTypeById(String id) {
        for(NetworkType type : networkTypes){
            if(type.getId() == id)
                return type;
        }
        return null;
    }

    @Override
    public Set<Network> getAllNeworks() {
        return new HashSet<>(networks);
    }

    @Override
    public Network getNetworkById(String networkId) {
        for(Network network : networks){
            if(network.getId() == networkId)
                return network;
        }
        return null;
    }

    @Override
    public Set<Task> getTasksByType(String typeId, String nodeName) {
        HashSet<Task> result = new HashSet<>();
        for(Task task : tasks){
            if(task.getTypeId() == typeId && task.getNode().getName() == nodeName)
                result.add(task);
        }
        return result;
    }

    @Override
    public Set<Task> getTasksByType(TaskType type, String nodeName) {
        HashSet<Task> result = new HashSet<>();
        for(Task task : tasks){
            if(task.getType().equals(type) && task.getNode().getName() == nodeName)
                result.add(task);
        }
        return result;
    }

}