package com.epam.bigdata.minskq3.task2.master.utils;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import rx.Observable;


public class HttpClient {


    public static Observable<String> makeGetRequest(String url) {
        return Observable.create(s -> {
            Document doc;
            StringBuilder sb = new StringBuilder();
            try{
                doc= Jsoup.connect(url).get();
                Elements children = doc.body().children();
                for (Element child : children) {
                    String text = child.text();
                    if (!text.contains("div")) {
                        sb.append(text);
                        sb.append(" ");
                    }
                    else {
                        text = Jsoup.parse(child.outerHtml()).text();
                        if (!text.contains("div")) {
                            sb.append(text);
                            sb.append(" ");
                        }
                    }
                }
                s.onNext(sb.toString());
                s.onCompleted();
            }
            catch (Exception ex){
                s.onError(ex);
            }

        });


    }
}
