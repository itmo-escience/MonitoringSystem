/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package entities;

import entities.NodeStatus;
import java.net.InetAddress;
import java.time.LocalDateTime;

/**
 *
 * @author user
 */
public class Node {
    private int id;
    private String name;
    private InetAddress ip;
    private LocalDateTime timestamp;
    private int parentNodeId;
    private Node parentNode;
    private NodeStatus status;
    private double cpuTotal;
    private double cpuUsage;
    private double memoryTotal;
    private double memoryUsage;
    private double gpuTotal;
    private double gpuUsage;
    private double networkInUsage;
    private double networkOutUsage;
    private int networkId;
}
