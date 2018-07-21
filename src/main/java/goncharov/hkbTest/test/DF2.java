package goncharov.hkbTest.test;

import java.io.Serializable;

public class DF2 implements Serializable {

    private int num;
    private String value1;
    private String value2;

    public DF2(int num, String value1, String value2) {
        this.num = num;
        this.value1 = value1;
        this.value2 = value2;
    }

    public int getNum() {
        return num;
    }

    public void setNum(int num) {
        this.num = num;
    }

    public String getValue1() {
        return value1;
    }

    public void setValue1(String value1) {
        this.value1 = value1;
    }

    public String getValue2() {
        return value2;
    }

    public void setValue2(String value2) {
        this.value2 = value2;
    }
}
