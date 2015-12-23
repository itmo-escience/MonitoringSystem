import StateStructures.ClusterState;
import ifmo.escience.dapris.common.data.IRepository;
import ifmo.escience.dapris.common.data.Uow;
import ifmo.escience.dapris.common.entities.*;
import ifmo.escience.dapris.common.helpers.NodeStateDateComparator;

import java.time.LocalDateTime;
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
        //ClusterState state = clusterStateMonitor.getClusterStateFromDB();
        ClusterState state = clusterStateMonitor.getActualClusterState();
        nodes = clusterStateMonitor.getNodes(state);
        tasks = clusterStateMonitor.getTasks(state);

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
    public Set<DataLayer> getAllDataLayers() {
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
        return metricsMonitor.getNodeStateForPeriod(node, start, finish);
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