/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package entities;

import entities.TaskStatus;
import entities.TaskType;
import entities.Data;
import org.bson.Document;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.Period;
import java.time.ZoneId;
import java.time.temporal.TemporalUnit;
import java.util.Date;
import java.util.Dictionary;
import java.util.HashSet;
import java.util.Set;

/**
 *
 * @author user
 */
public class Task {
    //private int id;

    private int parentTaskId;
    private Task ParentTask;
    private int typeId;
    private TaskType type;
    private Dictionary<String, String> parameters;
    private int nodeId;
    private TaskStatus status;
    private LocalDateTime startTimestamp;
    private LocalDateTime finishTimestamp;
    private HashSet<Data> data;

    private String id;
    private String packagename;
    private String resource;

    public Task(){
        
    }

    public Task(Object jsonItem){
        Document item = (Document)jsonItem;
        id = item.get("taskID").toString();
        packagename = item.get("package").toString();
        resource = item.get("resource").toString();
        status = TaskStatus.valueOf(item.get("status").toString());
        Date date = (Date)item.get("time");
        finishTimestamp = LocalDateTime.ofInstant(date.toInstant(), ZoneId.systemDefault());
        String timestr = item.get("T_All").toString();
        try{
            Duration allTime = Duration.parse("PT"+timestr.substring(0,2)+"H"+timestr.substring(3,5)+"M"+timestr.substring(6,8)+"S");
            startTimestamp = finishTimestamp.minusNanos(allTime.toNanos());
            System.out.println("123");
        }
        catch (Exception e){
            System.out.println("err");
        }



    }
}
