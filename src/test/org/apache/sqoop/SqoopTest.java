package org.apache.sqoop;

import org.junit.Before;
import org.junit.Test;

public class SqoopTest {

    private String[] args;

    @Before
    public void setUp() throws Exception {
//        String params = "import --connect jdbc:oracle:thin:@10.1.0.242:1521:ywxx --username bishow --password bishow -m 4 --split-by 'product_id' --query 'select sum_date,product_name,product_id from cqx_test2 where $CONDITIONS' --target-dir '/cqx/hivetable/cqx_test2/' --fields-terminated-by '|' --as-textfile --delete-target-dir --null-string '' --null-non-string ''";
//        args = params.split(" ", -1);
        String[] arg = {"import", "--connect", "jdbc:oracle:thin:@10.1.0.242:1521:ywxx",
                "--username", "bishow", "--password", "C%MuhN#q$4", "-m", "4", "--split-by", "product_id", "--query",
                "select sum_date,product_name,product_id from cqx_test2 where $CONDITIONS",
                "--target-dir", "/cqx/hivetable/cqx_test2/", "--fields-terminated-by", "|", "--as-textfile",
                "--delete-target-dir", "--null-string", "", "--null-non-string", ""};
        args = arg;
        System.out.println("args：");
        for (String p : args) {
            System.out.print(p+" ");
        }
    }

    @Test
    public void run() {
        int ret = Sqoop.runTool(args);
        System.out.println("ret：" + ret);
    }
}