package io.activedata;

import okhttp3.*;
import org.apache.commons.lang3.time.StopWatch;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class TEst {
    private OkHttpClient client = new OkHttpClient.Builder()
            .build();

    @Test
    public void test1() throws IOException {
        multiExecute(10, "http://www.baidu.com");
        multiExecute(10, "http://www.baidu.com");

        multiAsyncExecute(10, "http://www.baidu.com");
        multiAsyncExecute(10, "http://www.baidu.com");

        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    class XXCallback implements Callback {
        @Override
        public void onFailure(Call call, IOException e) {

        }

        @Override
        public void onResponse(Call call, Response response) throws IOException {
            System.err.println(response.code());
        }
    }

    private void multiExecute(int times, String url) throws IOException {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        for (int i = 0; i < times; i++) {
            Request request = new Request.Builder().url("http://www.baidu.com").get().build();
            Response resp = client.newCall(request).execute();
        }
        stopWatch.stop();
        System.err.println("use time: " + stopWatch);
    }

    private void multiAsyncExecute(int times, String url) throws IOException {
        StopWatch stopWatch = new StopWatch();
        stopWatch.start();
        List<CompletableFuture> futureList = new ArrayList<>();
        for (int i = 0; i < times; i++){
            CompletableFuture future = CompletableFuture.runAsync(() -> {
                Request request = new Request.Builder().url("http://www.baidu.com").get().build();
                try {
                    Response resp = client.newCall(request).execute();
                    System.err.println(resp.code());
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
            futureList.add(future);
        }
        CompletableFuture[] array = new CompletableFuture[]{};
        CompletableFuture.allOf(futureList.toArray(array)).whenComplete((t, u) -> {
            stopWatch.stop();
            System.err.println("use time: " + stopWatch);
        });

    }
}
