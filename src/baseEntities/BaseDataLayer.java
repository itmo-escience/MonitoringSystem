package baseEntities;

import entities.Node;
import entities.Data;
import java.util.HashSet;
import java.util.Set;
import exceptions.LayerSizeOverflowException;

/**
 *
 * @author Alexander Visheratin
 */
public abstract class BaseDataLayer {
    private int id;
    private String name;
    private int nodeId;
    private Node node;
    private double readSpeed;
    private double writeSpeed;
    private double totalSize;
    private double usedSize;
    private Set<Data> dataContent;
    
    public BaseDataLayer(){
        dataContent = new HashSet<>();
    }
    
    public BaseDataLayer(String name, int nodeId, double readSpeed, 
            double writeSpeed, double totalSize){
        dataContent = new HashSet<>();
        this.name = name;
        this.nodeId = nodeId;
        this.readSpeed = readSpeed;
        this.writeSpeed = writeSpeed;
        this.totalSize = totalSize;
    }
    
    public BaseDataLayer(String name, int nodeId, double readSpeed, 
            double writeSpeed, double totalSize, Set<Data> content) throws LayerSizeOverflowException{
        dataContent = new HashSet<>();
        this.name = name;
        this.nodeId = nodeId;
        this.readSpeed = readSpeed;
        this.writeSpeed = writeSpeed;
        this.totalSize = totalSize;
        for (Data dataElement : content) {
            if(dataElement.getSize() + this.usedSize <= this.totalSize){
                dataContent.add(dataElement);
                this.usedSize += dataElement.getSize();
            }
            else {
                dataContent = new HashSet<>();
                throw new LayerSizeOverflowException();
            }
        }
    }
    
    public boolean addData(Data data){
        if(data.getSize() + usedSize <= totalSize){
            dataContent.add(data);
            usedSize += data.getSize();
            return true;
        }
        else{
            return false;
        }
    }
    
    public boolean removeData(String name){
        for (Data f : dataContent) {
            if (f.getName() == name){
                dataContent.remove(f);
                return true;
            }
        }
        return false;
    }
    
    public Data getData(String name){
        for (Data f : dataContent) {
            if (f.getName() == name){
                return f;
            }
        }
        return null;
    }
}
