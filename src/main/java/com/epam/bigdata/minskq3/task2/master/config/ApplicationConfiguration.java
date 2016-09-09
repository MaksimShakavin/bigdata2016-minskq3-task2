package com.epam.bigdata.minskq3.task2.master.config;

import com.epam.bigdata.minskq3.task2.master.JobContainer;
import org.apache.hadoop.yarn.client.api.NMClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

@Configuration
@ComponentScan("com.hortonworks")
public class ApplicationConfiguration {

    @Bean
    public org.apache.hadoop.conf.Configuration yarnConfig(){
        return new YarnConfiguration();
    }



    @Bean
    public NMClient nodeManagerClient(org.apache.hadoop.conf.Configuration yarnConfig){
        NMClient nmClient= NMClient.createNMClient();
        nmClient.init(yarnConfig);
        return nmClient;
    }

    @Bean
    @Scope("prototype")
    public JobContainer jobContainer(){
        return new JobContainer(2,256,0);
    }



}
