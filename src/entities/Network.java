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
public class Network {
    private int id;
    private String name;
    private int networkTypeId;
    private NetworkType type;
    
    public Network(){
        
    }
        
    public int getId(){
        return id;
    }
    
    public String getName(){
        return name;
    }
    
    public NetworkType getType(){
        return type;
    }
}