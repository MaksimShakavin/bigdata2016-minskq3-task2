package com.epam.bigdata.minskq3.task2.master.utils;


public class LogLine {
    String id;
    String topWords;
    String middle;
    String url;

    public LogLine(String id, String middle, String url) {
        this.id = id;
        this.middle = middle;
        this.url = url;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getMiddle() {
        return middle;
    }

    public void setMiddle(String middle) {
        this.middle = middle;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getTopWords() {
        return topWords;
    }

    public void setTopWords(String topWords) {
        this.topWords = topWords;
    }

    @Override
    public String toString() {
        return id + "\t" + topWords + "\t" + middle + "\t" + url;
    }
}
