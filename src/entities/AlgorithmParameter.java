/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package entities;

import java.io.Serializable;

/**
 *
 * @author Alexander Visheratin
 */
public class AlgorithmParameter implements Serializable {
    private String name;
    private double currentValue;
    private double minValue;
    private double maxValue;
    
    public AlgorithmParameter(String name, double value, 
            double minValue, double maxValue){
        this.name = name;
        this.currentValue = value;
        this.minValue = minValue;
        this.maxValue = maxValue;
    }
    
    public AlgorithmParameter(){ }
    
    public String getName(){
        return name;
    }
    
    public double getCurrentValue(){
        return currentValue;
    }
    
    public void setValue(double newValue){
        if(newValue >= minValue && newValue <= maxValue)
            currentValue = newValue;
    }
    
    public double getMinValue(){
        return minValue;
    }
    
    public double getMaxValue(){
        return maxValue;
    }
    
    public boolean equals(AlgorithmParameter param) {
        return this.name == param.name && 
                this.currentValue == param.currentValue && 
                this.minValue == param.minValue &&
                this.maxValue == param.maxValue; 
    }
    
    public AlgorithmParameter clone(){
        return new AlgorithmParameter(name, currentValue, minValue, maxValue);
    }
}
