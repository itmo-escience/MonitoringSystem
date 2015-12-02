/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package entities;

import entities.Data;
import baseEntities.BaseDataLayer;
import java.time.LocalDateTime;

/**
 *
 * @author user
 */
public class DataChange {
    private int id;
    private int dataId;
    private Data data;
    private LocalDateTime startTimestamp;
    private LocalDateTime finishTimestamp;
    private int startLayerId;
    private BaseDataLayer startLayer;
    private int finishLayerId;
    private BaseDataLayer finishLayer;
}
