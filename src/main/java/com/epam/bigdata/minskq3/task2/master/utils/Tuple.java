package com.epam.bigdata.minskq3.task2.master.utils;


public class Tuple<X, Y> {
    private final X one;
    private final Y two;
    public Tuple(X x, Y y) {
        this.one = x;
        this.two = y;
    }

    public X one() {
        return one;
    }

    public Y two() {
        return two;
    }
}
