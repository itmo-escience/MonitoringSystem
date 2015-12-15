import ifmo.escience.dapris.common.data.IRepository;
import ifmo.escience.dapris.common.entities.*;
import ifmo.escience.dapris.common.helpers.NodeStateDateComparator;

import java.net.InetAddress;
import java.rmi.UnknownHostException;
import java.time.LocalDateTime;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Alexander Visheratin
 */
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

    public StatisticsRepository() {
        initNetworkTypes();
        initNetworks();
        try {
            initNodes();
        } catch (UnknownHostException ex) {
            Logger.getLogger(StatisticsRepository.class.getName()).log(Level.SEVERE, null, ex);
        }
        initLayers();
        initData();
        initTaskTypes();
        initTasks();
    }

    private void initNodes() throws UnknownHostException{


        nodesNumber = 20;
        for (int i = 1; i <= nodesNumber; i++) {
            Random rand = new Random(i);
            String idString = String.valueOf(i);
            String name = "Node " + idString;
            String ip = "192.168.0." + idString;
            int parentId = 0;
            if(i%4 != 0)
                parentId = i / 4;
            NodeStatus status = NodeStatus.Runnung;
            double cpuTotal = rand.nextInt(16);
            double memTotal = rand.nextInt(16000);
            double gpuTotal = rand.nextInt(4086);
            int networkId = rand.nextInt(networks.size()-1);
            LocalDateTime initTime = LocalDateTime.now();
            Node node = new Node(i, name, ip, parentId,
                    cpuTotal, memTotal, gpuTotal, networkId);
            nodes.add(node);
            for (int j = 0; j < 40; j++) {
                LocalDateTime time = initTime.plusMinutes(i);
                double cpuUsage = rand.nextInt(4);
                double memUsage = rand.nextInt(4000);
                double gpuUsage = rand.nextInt(512);
                double netInUsage = rand.nextInt(1000);
                double netOutUsage = rand.nextInt(1000);
                NodeState state = new NodeState(j, i, time, status,
                        cpuUsage, memUsage, gpuUsage, netInUsage, netOutUsage);
                nodeStates.add(state);
            }
        }
    }

    private void initNetworkTypes(){
        for (int i = 1; i < 3; i++) {
            Random rand = new Random(i);
            String name = "Type " + String.valueOf(i);
            NetworkType type = new NetworkType(i, name, rand.nextInt(1000),
                    rand.nextInt(1000), rand.nextInt(15));
            networkTypes.add(type);
        }
    }

    private void initNetworks(){

        //Network net = new Network("1", name,rand.nextInt(1200),rand.nextInt(1200),rand.nextInt(300));
        //networks.add(net);

    }

    private void initLayers(){
        for (int i = 1; i < 55; i++) {
            Random rand = new Random(i);
            String name = "Layer " + String.valueOf(i);
            DataLayer layer = new DataLayer(i, name, rand.nextInt(nodes.size()-1),
                    rand.nextInt(5000), rand.nextInt(5000), rand.nextInt(100000));
            layers.add(layer);
        }
    }

    private void initData(){
        Random rand = new Random();
        for (int i = 1; i < 155; i++) {
            String name = "Data " + String.valueOf(i);
            Data data = new Data(i, rand.nextInt(layers.size()-1),
                    name, rand.nextInt(5000));
            this.data.add(data);
        }
    }

    private void initTaskTypes(){
        Random rand = new Random();
        for (int i = 1; i < 6; i++) {
            String name = "Task type " + String.valueOf(i);
            TaskType type = new TaskType(i, name);
            taskTypes.add(type);
        }
    }

    private void initTasks(){
        for (int i = 1; i < 500; i++) {
            Random rand = new Random(i);
            String name = "Task " + String.valueOf(i);
            // int typeId = rand.nextInt(taskTypes.size()-1)+1;
            int typeId = 3;
            Hashtable<String, Double> parameters = new Hashtable<>();
            double sum = 0.0;
            for (int j = 0; j < typeId; j++) {
                String key = "key" + String.valueOf(j);
                double value = rand.nextDouble() * rand.nextInt(30);
                parameters.put(key, value);
                sum += value;
            }
            int nodeId = rand.nextInt(nodesNumber-1);
            LocalDateTime start = LocalDateTime.now().plusMinutes(rand.nextInt(10));
            LocalDateTime finish = start.plusMinutes((int) sum);
            HashSet<Data> inData = new HashSet<>();
            for (int j = 0; j < rand.nextInt(20); j++) {
                inData.add(this.data.get(rand.nextInt(this.data.size()-1)));
            }
            HashSet<Data> outData = new HashSet<>();
            for (int j = 0; j < rand.nextInt(20); j++) {
                outData.add(this.data.get(rand.nextInt(this.data.size()-1)));
            }
            Task task = new Task(i, null, typeId, parameters, nodeId+1,
                    TaskStatus.Finished, start, finish, inData, outData);
            tasks.add(task);
        }
    }

    @Override
    public Set<Data> getAllData() {
        return new HashSet<>(data);
    }

    @Override
    public Set<Data> getDataByLayer(int layerId) {
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
    public Data getDataById(int id) {
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
    public Set<DataLayer> getAllDataLayers() {
        return new HashSet<>(layers);
    }

    @Override
    public DataLayer getLayerById(int id) {
        for (DataLayer item : layers){
            if(item.getId() == id){
                return item;
            }
        }
        return null;
    }

    @Override
    public Set<DataLayer> getLayersByNode(int nodeId) {
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
    public Set<Node> getChildNodes(int parentId) {
        HashSet<Node> result = new HashSet<>();
        for(Node node : nodes){
            if(node.getParentId() == parentId)
                result.add(node);
        }
        return result;
    }

    @Override
    public Node getNodeById(int id) {
        for(Node node : nodes){
            if(node.getId() == id)
                return node;
        }
        return null;
    }

    @Override
    public Node getNodeByName(String name) {
        for(Node node : nodes){
            if(node.getName() == name)
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
    public List<NodeState> getNodeStateForPeriod(String name, LocalDateTime start, LocalDateTime finish) {
        ArrayList<NodeState> result = new ArrayList<>();
        for(NodeState state : nodeStates){
            if(state.getNode().getName() == name && state.getTimestamp().isAfter(start) &&
                    state.getTimestamp().isBefore(finish))
                result.add(state);
        }
        Collections.sort(result, new NodeStateDateComparator());
        return result;
    }

    @Override
    public List<NodeState> getNodeStateForPeriod(int nodeId, LocalDateTime start, LocalDateTime finish) {
        ArrayList<NodeState> result = new ArrayList<>();
        for(NodeState state : nodeStates){
            if(state.getNode().getId() == nodeId && state.getTimestamp().isAfter(start) &&
                    state.getTimestamp().isBefore(finish))
                result.add(state);
        }
        Collections.sort(result, new NodeStateDateComparator());
        return result;
    }

    @Override
    public Set<Task> getAllTasks() {
        return new HashSet<>(tasks);
    }

    @Override
    public Task getTaskById(int id) {
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
    public Set<Task> getTasksByType(int typeId) {
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
    public TaskType getTaskTypeById(int id) {
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
    public NetworkType getNetworkTypeById(int id) {
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
    public Network getNetworkById(int networkId) {
        for(Network network : networks){
            if(network.getId() == networkId)
                return network;
        }
        return null;
    }

    @Override
    public Set<Task> getTasksByType(int typeId, String nodeName) {
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