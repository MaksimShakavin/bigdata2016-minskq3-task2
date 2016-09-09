package com.epam.bigdata.minskq3.task2.master;

import com.epam.bigdata.minskq3.task2.Constants;
import com.epam.bigdata.minskq3.task2.master.utils.HttpClient;
import com.epam.bigdata.minskq3.task2.master.utils.LogLine;
import com.epam.bigdata.minskq3.task2.master.utils.Tuple;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.RootLogger;
import org.jsoup.HttpStatusException;
import rx.Observable;


import java.io.*;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.reverseOrder;
import static java.util.stream.Collectors.counting;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toList;

public class WordCountJob {
    final static Logger LOG = Logger.getLogger(RootLogger.class);
    private final AtomicInteger processed = new AtomicInteger(0);


    private void execute(String inputFile,String outputFile) {
        LOG.debug("reading file");

        Configuration conf = new Configuration();
        conf.set("fs.hdfs.impl",
                org.apache.hadoop.hdfs.DistributedFileSystem.class.getName()
        );
        conf.set("fs.file.impl",
                org.apache.hadoop.fs.LocalFileSystem.class.getName()
        );

        Path input = new Path(inputFile);
        Path output = new Path(outputFile);
        try (FileSystem fileSystem = FileSystem.get(new Path(Constants.HDFS_ROOT_PATH).toUri(), conf);
             Stream<String> inputLines = new BufferedReader(new InputStreamReader(fileSystem.open(input))).lines();
             BufferedWriter outBr = new BufferedWriter(new OutputStreamWriter(fileSystem.create(output,true)))
        )
        {
            inputLines.map(this::parseLine)
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .map(logLine -> {
                        String topWords = Stream.of(logLine.getUrl())
                                .map(this::getTextByHttp)
                                .map(this::extractWords)
                                .flatMap(words -> getTopWords(words, 10).stream())
                                .collect(Collectors.joining(", "));
                        logLine.setTopWords(topWords);
                        return logLine;
                    })
                    .forEach(logLine -> appendLineToFile(outBr,logLine));

        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }


    public static void main(String[] args) {
        String inputFile = args[0];
        String outputFile = args[1];

        WordCountJob job = new WordCountJob();
        LOG.debug("Starting job");
        job.execute(inputFile,outputFile);
    }


    private void appendLineToFile(Writer wr, LogLine logLine) {
        try {
            LOG.debug("writing "+ (processed.incrementAndGet()) + ": "+logLine.toString());
            wr.append(logLine.toString()).append("\n");
        }
        catch (IOException ex){
            throw new RuntimeException(ex);
        }
    }

    private Optional<LogLine> parseLine(String line) {
        String[] arr = Pattern.compile("\\s").split(line);
        if (arr[arr.length - 1].startsWith("http://")) {
            String id = arr[0];
            String midle = Arrays.stream(arr)
                    .skip(1)
                    .limit(arr.length - 2)
                    .collect(Collectors.joining("\t"));
            String url = arr[arr.length - 1];
            return Optional.of(new LogLine(id, midle, url));
        } else {
            return Optional.empty();
        }
    }

    private String getTextFromUrl(String url) {
        return url.split(".com/")[1].replace(".html", "").replace("-", " ");
    }

    private List<String> getTopWords(Stream<String> words, int num) {
        return words
                .map(String::toLowerCase)
                .collect(groupingBy(Function.identity(), counting()))
                .entrySet().stream()
                .sorted(Map.Entry.<String, Long>comparingByValue(reverseOrder()).thenComparing(Map.Entry.comparingByKey()))
                .limit(num + Constants.STOP_WORDS.size())
                .map(Map.Entry::getKey)
                .collect(toList());
    }

    private Stream<String> extractWords(String text) {
        return Pattern.compile("\\W").splitAsStream(text)
                .filter((s -> !s.isEmpty()))
                .filter(w -> !Pattern.compile("\\d+").matcher(w).matches());
    }

    private String getTextByHttp(String url) {

        return HttpClient.makeGetRequest(url)
                .doOnError(ex -> LOG.warn(ex.getMessage() + " error while processing " + url + ". Trying to recover")) // log an error
                .retryWhen(attempts -> attempts // try to recover an error
                        .zipWith(Observable.range(1, 4), Tuple::new) // repeat 3 times wit increasing delay
                        .flatMap(pair -> {
                            if (pair.one() instanceof HttpStatusException && ((HttpStatusException) pair.one()).getStatusCode() == 404) { //if 404 throw error
                                LOG.error("404 status code on url " + url);                                                                // else retry with delay
                                return Observable.<Long>error(pair.one());
                            }
                            LOG.warn("delay retry by " + pair.two() + " second(s)");
                            return pair.two() == 4 ? Observable.<Long>error(pair.one()) : Observable.timer(pair.two(), TimeUnit.SECONDS);
                        }))
                .onErrorReturn(error -> getTextFromUrl(url)) // if error get
                .toBlocking().single();

    }


}
