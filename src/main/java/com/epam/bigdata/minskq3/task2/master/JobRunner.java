package com.epam.bigdata.minskq3.task2.master;


import com.epam.bigdata.minskq3.task2.Constants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;

import java.io.*;
import java.net.URI;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class JobRunner {

    private static final String CONTAINER_FILE = "container";

    private Configuration configuration;
    private String inputFile;
    private int numbOfContainers;
    private AppMaster appMaster;

    private AtomicInteger doneContainers;
    private AtomicInteger allocatedContainers;

    public JobRunner(AppMaster appMaster, Configuration configuration, String inputFile, int numbOfContainers) {
        this.appMaster = appMaster;
        this.configuration = configuration;
        this.inputFile = inputFile;
        this.numbOfContainers = numbOfContainers;

        doneContainers = new AtomicInteger(0);
        allocatedContainers = new AtomicInteger(0);
    }

    public void run() throws Exception{
        RegisterApplicationMasterResponse response = appMaster.registerMaster();
        splitFile();
        requestContainers(response);
        while (!containersDone()){
            Thread.sleep(100);
        }
        combineResults();
        deleteTmpDir();
        appMaster.unergisterMaster();

    }

    public void requestContainers(RegisterApplicationMasterResponse response) {
        List<JobContainer> containers = IntStream.range(0, numbOfContainers)
                .mapToObj((i) -> new JobContainer(2,256,0))
                .peek(container -> container.adjustAvaliableResouces(response.getMaximumResourceCapability()))
                .collect(Collectors.toList());

        appMaster.allocateContainers(containers,this::containersAllocated,this::containerCompleted);
    }

    public void containersAllocated(List<Container> containers){
        containers.forEach(container -> {
            String inputPath =Constants.HDFS_ROOT_PATH + Constants.HSFS_TMP_PATH+ CONTAINER_FILE +allocatedContainers.incrementAndGet();
            String outputPath =Constants.HDFS_ROOT_PATH + Constants.HSFS_TMP_PATH+ "out/"+CONTAINER_FILE +allocatedContainers.get();
            try{
                ContainerLaunchContext ctx =
                        Records.newRecord(ContainerLaunchContext.class);
                ctx.setCommands(
                        Collections.singletonList(
                                "$JAVA_HOME/bin/java" +
                                        " -Xms256M -Xmx512M" +
                                        " com.hortonworks.simpleyarnapp.master.WordCountJob" +
                                        " " + inputPath +
                                        " " + outputPath +
                                        " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout" +
                                        " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr"
                        ));

                Map<String, String> containerEnv = new HashMap<>();
                containerEnv.put("CLASSPATH", "./*");
                ctx.setEnvironment(containerEnv);

                LocalResource appMasterJar = Records.newRecord(LocalResource.class);
                setupAppMasterJar(Constants.HDFS_MY_APP_JAR_PATH, appMasterJar);
                ctx.setLocalResources(Collections.singletonMap("simple-app.jar", appMasterJar));

                System.out.println("[AM] Launching container " + container.getId());
                appMaster.startContainer(container,ctx);
            }
            catch (Exception ex){
                throw new RuntimeException(ex);
            }
        });

    }

    public void containerCompleted(List<ContainerStatus> statuses) {
        for (ContainerStatus status : statuses) {
            System.out.println("[AM] Completed container " + status.getContainerId());
            doneContainers.incrementAndGet();
        }
    }

    private void setupAppMasterJar(Path jarPath, LocalResource appMasterJar) throws IOException {
        FileStatus jarStat = FileSystem.get(configuration).getFileStatus(jarPath);
        appMasterJar.setResource(ConverterUtils.getYarnUrlFromPath(jarPath));
        appMasterJar.setSize(jarStat.getLen());
        appMasterJar.setTimestamp(jarStat.getModificationTime());
        appMasterJar.setType(LocalResourceType.FILE);
        appMasterJar.setVisibility(LocalResourceVisibility.PUBLIC);
    }

    private void splitFile() {
        long lines = countLines(new Path(Constants.HDFS_ROOT_PATH + inputFile));
        long[] splits = getNumbOfLinesPerContainer(lines);

        BufferedReader inputBr = null;
        try {
            Path path = new Path(Constants.HDFS_ROOT_PATH + inputFile);
            FileSystem fileSystem = FileSystem.get(path.toUri(), configuration);
            CompressionCodecFactory factory = new CompressionCodecFactory(configuration);
            CompressionCodec codec = factory.getCodec(path);
            InputStream stream;
            stream = codec == null ? fileSystem.open(path) : codec.createInputStream(fileSystem.open(path));

            inputBr = new BufferedReader(new InputStreamReader(stream));

            FileSystem fsOut = FileSystem.get(new URI(Constants.HDFS_ROOT_PATH + Constants.HSFS_TMP_PATH), configuration);
            for (int i = 0; i < splits.length; i++) {
                Path tmpPath = new Path(Constants.HDFS_ROOT_PATH + Constants.HSFS_TMP_PATH + CONTAINER_FILE + (i+1));
                BufferedWriter brOut = new BufferedWriter(new OutputStreamWriter(fsOut.create(tmpPath, true)));

                for (int j = 0; j < splits[i]; j++) {
                    brOut.append(inputBr.readLine()).append("\n");
                }
                brOut.close();
            }
            inputBr.close();

        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    private long countLines(Path location) {
        long num = 0L;
        try {
            FileSystem fileSystem = FileSystem.get(location.toUri(), configuration);
            CompressionCodecFactory factory = new CompressionCodecFactory(configuration);

            CompressionCodec codec = factory.getCodec(location);
            InputStream stream;
            stream = codec == null ? fileSystem.open(location) : codec.createInputStream(fileSystem.open(location));

            BufferedReader inputBr = new BufferedReader(new InputStreamReader(stream));
            num = inputBr.lines().count();

        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }

        return num;
    }

    private long[] getNumbOfLinesPerContainer(long totalLines) {
        long nubmOfLines = totalLines / numbOfContainers;
        long lastDif = totalLines % (nubmOfLines * numbOfContainers);

        long[] numbers = new long[numbOfContainers];
        for (int i = 0; i < numbOfContainers; i++) {
            numbers[i] = nubmOfLines;
            if (i == numbOfContainers - 1) numbers[i] += lastDif;
        }
        return numbers;
    }

    private boolean containersDone(){
        return doneContainers.get() == numbOfContainers;
    }

    private void combineResults(){
        Path tmpDir = new Path(Constants.HDFS_ROOT_PATH + Constants.HSFS_TMP_PATH + "out/");
        Path result = new Path(Constants.HDFS_ROOT_PATH + inputFile+".out");
        try(FileSystem fileSystem = FileSystem.get(new Path(Constants.HDFS_ROOT_PATH).toUri(), configuration);
            BufferedWriter br= new BufferedWriter(new OutputStreamWriter(fileSystem.create(result)))
        ){
            FileStatus[] items = fileSystem.listStatus(tmpDir);
            for(FileStatus item : items){
                if(!item.getPath().getName().startsWith(CONTAINER_FILE)){
                    continue;
                }
                BufferedReader reader = new BufferedReader(new InputStreamReader(fileSystem.open(item.getPath())));
                String line = reader.readLine();
                while (line != null){
                    br.append(line).append("\n");
                    line = reader.readLine();
                }
                reader.close();
            }
        }
        catch (Exception ex){
            throw  new RuntimeException(ex);
        }
    }

    private void deleteTmpDir(){
        Path tmpDir = new Path(Constants.HDFS_ROOT_PATH+Constants.HSFS_TMP_PATH);
        try(FileSystem fileSystem = FileSystem.get(configuration)){
            fileSystem.delete(tmpDir,true);
        }
        catch (Exception ex){
            throw new RuntimeException(ex);
        }
    }


}
