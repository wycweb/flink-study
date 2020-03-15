package com.wangyichao.bigdata.flink01.bean;

public class WorldCount {

    public String word;
    public int count;

    public WorldCount() {

    }

    public WorldCount(String word, int count) {
        this.word = word;
        this.count = count;
    }

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }


    @Override
    public String toString() {
        return "WorldCount{" +
                "word='" + word + '\'' +
                ", count=" + count +
                '}';
    }
}
