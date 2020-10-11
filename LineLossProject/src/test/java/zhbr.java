import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.regression.LinearRegressionModel;
import org.apache.spark.mllib.regression.LinearRegressionWithSGD;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.*;


public class LinearRegression {
    public static void main(final String[] args) throws Exception {
        SparkConf conf = new SparkConf().setAppName("LinearRegressionWithSGD");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);
        //加载配置文件
        InputStream in = LinearRegression.class.getResourceAsStream("/config.properties");
        Properties properties = new Properties();
        properties.load(in);
        String dqsUrl = properties.getProperty("dqsurl");
        String sysUrl = properties.getProperty("sysurl");
        sqlContext.read().jdbc(dqsUrl, "tb_analyse_mining_plan", properties).registerTempTable("tb_analyse_mining_plan");
        List<Row> planList = sqlContext.sql("select * from tb_analyse_mining_plan where FLAG = 0").collectAsList();
        List<String> calcList = new ArrayList<>();
        //jdbc
        Class.forName(properties.getProperty("driverClass"));
        Connection con = DriverManager.getConnection(dqsUrl, properties.getProperty("user"), properties.getProperty("password"));
        Statement st = con.createStatement();
        for (Row plan : planList) {
            String id = plan.getString(0);
            String calcTime = plan.getString(1);
            calcList.add(calcTime);
            //将flag修改为1
            String sql = "update tb_analyse_mining_plan set flag = 1 where id = '" + id + "'";
            st.execute(sql);
        }
        /** 特别注释  删除此段代码就不会加上当天数据，可以按半个小时调度*/
        //加上当天日期
        // Calendar calendar = Calendar.getInstance();
        // SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
        // String nowDay = sdf.format(calendar.getTime());
        // calcList.add(nowDay);
        /** 特别注释 */
        //读取数据源
        DataFrame tb_xsjs_fxtjxs_day = sqlContext.read().jdbc(dqsUrl, "tb_xsjs_fxtjxs_day", properties).cache();
        DataFrame tb_analyse_mining_factor = sqlContext.read().jdbc(dqsUrl, "tb_analyse_mining_factor", properties).cache();
        DataFrame tb_sys_department_superid = sqlContext.read().jdbc(sysUrl, "tb_sys_department_superid", properties).cache();
        DataFrame tb_qb = sqlContext.read().jdbc(dqsUrl, "tb_qb", properties).cache();
        tb_xsjs_fxtjxs_day.registerTempTable("tb_xsjs_fxtjxs_day");
        tb_analyse_mining_factor.registerTempTable("tb_analyse_mining_factor");
        tb_sys_department_superid.registerTempTable("tb_sys_department_superid");
        tb_qb.registerTempTable("tb_qb");
        System.out.println("算法开始训练");

        for (String day : calcList) {
            sqlContext.sql("select t1.SSDDDWID,t1.SSDDDWMC,t1.LINE_ID,t1.LOSS,t2.TRAN_RATE,t2.POWER_RATE,t2.RADIUS,t2.PHASE_RATE,t2.LOAD_CENTER,t2.VOLTAGE_RATE,t1.QB " +
                    "from tb_xsjs_fxtjxs_day t1 " +
                    "join tb_analyse_mining_factor t2 on t1.SSDDDWID = t2.SSDDDWID and t1.LINE_ID = t2.LINE_ID and t1.QB = t2.QB " +
                    "join tb_qb t3 on t1.QB = t3.QB where t3.QS = " + day).registerTempTable("target");
            //自定义获取供电单位名称函数
            final Map<String, String> map = new HashMap<>();
            for (Row row : sqlContext.sql("select distinct SSDDDWID,SSDDDWMC from tb_analyse_mining_factor").collectAsList()) {
                map.put(row.getString(0), row.getString(1));
            }
            final Broadcast<Map<String, String>> bc = sc.broadcast(map);
            sqlContext.udf().register("getgddwmc", new UDF1<String, String>() {
                @Override
                public String call(String s) throws Exception {
                    return bc.value().get(s);
                }
            }, DataTypes.StringType);
            //自定义获取期别函数
            final Map<Integer, Integer> qbMap = new HashMap<>();
            for (Row row : sqlContext.sql("select distinct QS,QB from tb_qb").collectAsList()) {
                qbMap.put(row.getInt(0), row.getInt(1));
            }
            final Broadcast<Map<Integer, Integer>> qbBc = sc.broadcast(qbMap);
            sqlContext.udf().register("getqb", new UDF1<Integer, Integer>() {
                @Override
                public Integer call(Integer s) throws Exception {
                    return qbBc.value().get(s);
                }
            }, DataTypes.IntegerType);
            DataFrame target = sqlContext.sql("select distinct SSDDDWID from target");
            List<String> gddwList = new ArrayList<>();
            for (Row row : target.collectAsList()) {
                gddwList.add(row.getString(0));
            }
            System.out.println("日期" + day + "，共有" + gddwList.size() + "个供电单位");
            for (int i = 0; i < gddwList.size(); i++) {
                String gddw = gddwList.get(i);
                DataFrame df = sqlContext.sql("select * from target t1 " +
                        "join (select ID from tb_sys_department_superid  where PID = '" + gddw + "') t2 on t1.SSDDDWID = t2.ID");
                JavaRDD<String> data = df.toJavaRDD().map(new Function<Row, String>() {
                    @Override
                    public String call(Row row) throws Exception {
                        BigDecimal xsl = row.getAs("LOSS");
                        String TRAN_RATE = row.getAs("TRAN_RATE");
                        String POWER_RATE = row.getAs("POWER_RATE");
                        String RADIUS = row.getAs("RADIUS");
                        String PHASE_RATE = row.getAs("PHASE_RATE");
                        String LOAD_CENTER = row.getAs("LOAD_CENTER");
                        String VOLTAGE_RATE = row.getAs("VOLTAGE_RATE");
                        return xsl + "," + TRAN_RATE + " " + POWER_RATE + " " + RADIUS + " " + PHASE_RATE + " " + LOAD_CENTER + " " + VOLTAGE_RATE;
                    }
                });
                //读取样本数据
                JavaRDD<LabeledPoint> examples = data.map(new Function<String, LabeledPoint>() {
                    @Override
                    public LabeledPoint call(String line) throws Exception {
                        String[] parts = line.split(",");
                        String[] arr = parts[1].split(" ");
                        double[] doubles = new double[arr.length];
                        for (int i = 0; i < arr.length; i++) {
                            doubles[i] = Double.valueOf(arr[i]);
                        }
                        return new LabeledPoint(Double.valueOf(parts[0]), Vectors.dense(doubles));
                    }
                });
                //开始训练
                int numIterations = 100;
                double stepSize = 0.1;
                int miniBatchFraction = 1;
                LinearRegressionWithSGD lrs = new LinearRegressionWithSGD();
                lrs.setIntercept(false);
                lrs.optimizer().setStepSize(stepSize);
                lrs.optimizer().setNumIterations(numIterations);
                lrs.optimizer().setMiniBatchFraction(miniBatchFraction);
                LinearRegressionModel model = lrs.run(JavaRDD.toRDD(examples));
                double[] resultArr = model.weights().toArray();

                if (Double.isNaN(resultArr[0])
                        ||Double.isNaN(resultArr[1])
                        ||Double.isNaN(resultArr[2])
                        ||Double.isNaN(resultArr[3])
                        ||Double.isNaN(resultArr[4])
                        ||Double.isNaN(resultArr[5])) {

                    continue;
                }

                // 保留小数
                String TRAN_RATE = "0";
                String POWER_RATE = "0";
                String RADIUS = "0";
                String PHASE_RATE = "0";
                String LOAD_CENTER = "0";
                String VOLTAGE_RATE = "0";
                if ((String.valueOf(resultArr[0]).length()>= 8)) {

                    TRAN_RATE = String.valueOf(resultArr[0]).substring(0,5);

                } else {

                    TRAN_RATE = String.valueOf(resultArr[0]);
                }

                if ((String.valueOf(resultArr[1]).length()>= 8)) {

                    POWER_RATE = String.valueOf(resultArr[1]).substring(0,5);

                } else {

                    POWER_RATE = String.valueOf(resultArr[1]);
                }

                if ((String.valueOf(resultArr[2]).length()>= 8)) {

                    RADIUS = String.valueOf(resultArr[2]).substring(0,5);

                } else {

                    RADIUS = String.valueOf(resultArr[2]);
                }

                if ((String.valueOf(resultArr[3]).length()>= 8)) {

                    PHASE_RATE = String.valueOf(resultArr[3]).substring(0,5);

                } else {

                    PHASE_RATE = String.valueOf(resultArr[3]);
                }

                if ((String.valueOf(resultArr[4]).length()>= 8)) {

                    LOAD_CENTER = String.valueOf(resultArr[4]).substring(0,5);

                } else {

                    LOAD_CENTER = String.valueOf(resultArr[4]);
                }

                if ((String.valueOf(resultArr[5]).length()>= 8)) {

                    VOLTAGE_RATE = String.valueOf(resultArr[5]).substring(0,5);

                } else {

                    VOLTAGE_RATE = String.valueOf(resultArr[5]);
                }

                String res = TRAN_RATE + " TRAN_RATE," + POWER_RATE + " POWER_RATE," + RADIUS + " RADIUS," + PHASE_RATE + " PHASE_RATE," + LOAD_CENTER + " LOAD_CENTER," + VOLTAGE_RATE + " VOLTAGE_RATE";
                if (i == 0) {
                    sqlContext.sql("select '" + gddw + "' SSDDDWID,getgddwmc('" + gddw + "') SSDDDWMC," + res + ",getqb(" + day + ") QB").registerTempTable("tempResult");
                } else {
                    sqlContext.sql("select * from tempResult union all select '" + gddw + "' SSDDDWID,getgddwmc('" + gddw + "') SSDDDWMC," + res + ",getqb(" + day + ") QB").registerTempTable("tempResult");
                }
            }
            if (gddwList.size() > 0) {
                sqlContext.sql("select * from tempResult").write().mode("append").jdbc(dqsUrl, "tb_analyse_mining_result", properties);
            }
            //将flag修改为2
            for (Row plan : planList) {
                String id = plan.getString(0);
                String sql = "update tb_analyse_mining_plan set flag = 2 where id = '" + id + "'";
                st.execute(sql);
            }
        }
        System.out.println("训练结束");
    }
}
