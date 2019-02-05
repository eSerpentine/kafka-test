package com.eserpentine.kafka.utils;

import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public abstract class CommonUtils {

    public static void waitBeforeSend(int seconds) {
        try {
            TimeUnit.SECONDS.sleep(seconds);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    public static void threadStart(Runnable runnable) {
        new Thread(runnable).start();
    }
}
