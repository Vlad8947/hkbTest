package goncharov.hkbTest.test;

import java.io.Serializable;
import java.util.List;

public class DF1 implements Serializable {

    private int num;
    private String value;
//    private List<DF1> list;

    public DF1(int num, String value) {
        this.num = num;
        this.value = value;
    }

    public int getNum() {
        return num;
    }

    public void setNum(int num) {
        this.num = num;
    }

    public String getValue() {
        return value;
    }

    public void setValue(String value) {
        this.value = value;
    }

//    public List<DF1> getList() {
//        return list;
//    }
//
//    public void setList(List<DF1> list) {
//        this.list = list;
//    }
}
