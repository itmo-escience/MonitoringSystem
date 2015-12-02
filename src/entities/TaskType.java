/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package entities;

/**
 *
 * @author user
 */
public class TaskType {
    private int id;
    private String name;
    private String performanceModel;
    
    public TaskType(int id, String name, String performanceModel){
        this.id = id;
        this.name = name;
        this.performanceModel = performanceModel;
    }
    
    public TaskType(){ }
    
    public int getId(){
        return id;
    }
    
    public String getName(){
        return name;
    }
    
    public String getPerformanceModel(){
        return performanceModel;
    }
}
