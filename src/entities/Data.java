package entities;

import java.util.HashSet;

/**
 *
 * @author Alexander Visheratin
 */
public class Data {
    private int id;
    private String name;
    private int layerId;
    private double size;
    private HashSet<Task> tasks;
    
    public String getName(){
        return name;
    }
    
    public double getSize(){
        return size;
    }
}
