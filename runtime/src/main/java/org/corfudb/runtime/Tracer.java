package org.corfudb.runtime;

import com.github.benmanes.caffeine.cache.LoadingCache;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by box on 12/18/17.
 */
@Slf4j
public class Tracer {
 
    @Getter
    final static Tracer tracer = new Tracer();

    final static int batch = 3_000_000;

    final static ArrayList scratch = new ArrayList(batch);

    final static BlockingQueue<String> events = new LinkedBlockingQueue();

    public void log(String event) {
        long ts = System.nanoTime();
        events.add(ts + " " + Thread.currentThread().getId() +  " " +
                Thread.currentThread().getName()+ " " + event);
    }

    public void log(LoadingCache cache) {
        long ts = System.nanoTime();
        events.add(ts + " " + Thread.currentThread().getId() + " " + cache.stats().toString() + " size " + cache.estimatedSize());
    }

    public void dump(String path) throws Exception {

        StringBuilder stringBuilder = new StringBuilder();

        int elements = events.drainTo(scratch, batch);

        for (int x = 0; x < elements; x++) {
            stringBuilder.append(scratch.get(x) + "\n");
        }

        scratch.clear();

        if (elements > 0) {
            Path targetPath = Paths.get(path);
            byte[] bytes = stringBuilder.toString().getBytes(StandardCharsets.UTF_8);
            Files.write(targetPath, bytes, StandardOpenOption.APPEND, StandardOpenOption.WRITE,
                    StandardOpenOption.CREATE);
            log.info("Tracer-dump: dumped {} entries ", elements);
        } else {
            log.info("Tracer-dump: nothing to dump!");
        }
    }

}
