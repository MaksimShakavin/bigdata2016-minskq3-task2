package com.epam.bigdata.minskq3.task2.master;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

@Component
public class AppMaster implements AMRMClientAsync.CallbackHandler {
    private NMClient nmClient;
    private AMRMClientAsync<AMRMClient.ContainerRequest> rmClient;
    private Configuration configuration;
    private Consumer<List<Container>> onContainerAllocated;
    private Consumer<List<ContainerStatus>> onContainerCompleted;


    @Autowired
    public AppMaster(NMClient nmClient, Configuration configuration) {
        this.configuration = configuration;

        this.nmClient = nmClient;
        this.rmClient = AMRMClientAsync.createAMRMClientAsync(100,this);
    }

    public void init(){
        nmClient.start();
        rmClient.init(configuration);
        rmClient.start();
    }

    public RegisterApplicationMasterResponse registerMaster() throws Exception{
        return rmClient.registerApplicationMaster("", 0, "");
    }

    public void unergisterMaster() throws Exception{
        rmClient.unregisterApplicationMaster(
                FinalApplicationStatus.SUCCEEDED, "", "");
    }

    public void allocateContainers(List<JobContainer> containers,
                                   Consumer<List<Container>> onAllocated,
                                   Consumer<List<ContainerStatus>> onCompleted){
        onContainerAllocated = onAllocated;
        onContainerCompleted = onCompleted;
        containers.stream()
                .map(JobContainer::generateRequest)
                .forEach(containerRequest -> rmClient.addContainerRequest(containerRequest));
    }

    public void startContainer(Container container,ContainerLaunchContext ctx) throws Exception{
        nmClient.startContainer(container,ctx);
    }

    @Override
    public void onContainersCompleted(List<ContainerStatus> list) {
        Optional.ofNullable(onContainerCompleted).ifPresent(c -> c.accept(list));
    }

    @Override
    public void onContainersAllocated(List<Container> list) {
        Optional.ofNullable(onContainerAllocated)
                .ifPresent(consumer -> consumer.accept(list));
    }

    @Override
    public void onShutdownRequest() {

    }

    @Override
    public void onNodesUpdated(List<NodeReport> list) {

    }

    @Override
    public float getProgress() {
        return 0;
    }

    @Override
    public void onError(Throwable throwable) {

    }

}
