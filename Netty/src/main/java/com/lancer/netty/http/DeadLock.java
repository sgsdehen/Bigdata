package com.lancer.netty.http;

import java.util.ArrayList;

/**
 * @Author lancer
 * @Date 2022/3/24 5:10 下午
 * @Description 死锁案例
 */
public class DeadLock implements Runnable {

    int flag;

    public DeadLock(int flag) {
        this.flag = flag;
    }

    static final Object o1 = new Object();
    static final Object o2 = new Object();


    @Override
    public void run() {
        if (flag == 0) {
            synchronized (o1) {
                try {
                    System.out.println("我是" + Thread.currentThread().getName() + "锁住o1, 进入睡眠");
                    Thread.sleep(3000);
                    System.out.println(Thread.currentThread().getName() + "醒来->准备获取o2");
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                synchronized (o2) {
                    System.out.println(Thread.currentThread().getName() + "拿到o2");
                }
            }
        }
        if (flag == 1) {
            synchronized (o2) {
                try {
                    System.out.println("我是" + Thread.currentThread().getName() + "锁住o2，进入睡眠");
                    Thread.sleep(3000);
                    System.out.println(Thread.currentThread().getName() + "醒来——>准备获取o1");
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                synchronized (o1) {
                    System.out.println(Thread.currentThread().getName() + "拿到o1");
                }
            }
        }
    }

    public static void main(String[] args) {
        DeadLock deadLock1 = new DeadLock(1);
        DeadLock deadLock2 = new DeadLock(0);

        new Thread(deadLock1).start();
        new Thread(deadLock2).start();
    }
}
