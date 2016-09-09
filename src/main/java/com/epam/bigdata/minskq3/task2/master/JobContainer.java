package com.epam.bigdata.minskq3.task2.master;

import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.util.Records;

public class JobContainer {
    private int numVCores;
    private int memory;
    private int priority;
    private String inputFileName;

    public JobContainer(int numVCores, int memory, int priority) {
        this.numVCores = numVCores;
        this.memory = memory;
        this.priority = priority;
    }

    public AMRMClient.ContainerRequest generateRequest(){
        Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(this.memory);
        capability.setVirtualCores(this.numVCores);

        Priority priority = Records.newRecord(Priority.class);
        priority.setPriority(this.priority);
        return new AMRMClient.ContainerRequest(capability, null, null, priority);
    }

    public void adjustAvaliableResouces(Resource resources){
        int aMemory = resources.getMemory();
        int aCores = resources.getVirtualCores();

        if(aMemory < this.memory){
            this.memory = aMemory;
        }
        if (aCores < this.numVCores){
            this.numVCores = aCores;
        }
    }
}
