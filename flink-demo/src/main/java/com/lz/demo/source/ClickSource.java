package com.lz.demo.source;

import com.lz.demo.entity.Event;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Calendar;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class ClickSource implements SourceFunction<Event> {
    private boolean running = true;

    @Override
    public void run(SourceContext<Event> sourceContext) throws InterruptedException {
        Random random = new Random();
        String[] users = {"Mary", "Alice", "Bob", "Jerry"};
        String[] urls = {"./home", "./cart", "./prod", "./fav"};

        while (running) {
            String user = users[random.nextInt(users.length)];
            String url = urls[random.nextInt(urls.length)];
            Long timestamp = Calendar.getInstance().getTimeInMillis();
            sourceContext.collect(new Event(user, url, timestamp));
            TimeUnit.SECONDS.sleep(1);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
