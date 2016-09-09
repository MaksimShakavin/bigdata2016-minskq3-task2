package com.epam.bigdata.minskq3.task2.master;

import com.epam.bigdata.minskq3.task2.master.config.ApplicationConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

public class EntryPoint {
    public static void main(String[] args) throws Exception{
        System.out.println("Staring Application Master");
        ApplicationContext context = new AnnotationConfigApplicationContext(ApplicationConfiguration.class);

        String inputFile = args[0];
        int numbOfContainers= Integer.valueOf(args[1]);

        AppMaster appMaster= context.getBean(AppMaster.class);
        appMaster.init();

        System.out.println("Registering app Master");
        JobRunner runner = new JobRunner(appMaster,new Configuration(),inputFile,numbOfContainers);
        runner.run();

    }
}
