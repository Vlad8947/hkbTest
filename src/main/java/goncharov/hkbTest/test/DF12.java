package goncharov.hkbTest.test;

import java.util.List;

public class DF12 extends DF1 {

    private List<DF1> list;

    public DF12(int num, String value) {
        super(num, value);
    }

    public List<DF1> getList() {
        return list;
    }

    public void setList(List<DF1> list) {
        this.list = list;
    }

    @Override
    public String getValue() {
        return super.getValue() + "_butNo";
    }
}
