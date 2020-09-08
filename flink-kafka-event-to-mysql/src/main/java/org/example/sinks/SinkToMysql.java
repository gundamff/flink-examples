package org.example.sinks;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.example.model.DataEvent;

import com.zaxxer.hikari.HikariDataSource;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SinkToMysql extends RichSinkFunction<DataEvent> {

    private static final long serialVersionUID = 6740264938248631726L;

    PreparedStatement ps;
    HikariDataSource dataSource;
    private Connection connection;

    AtomicInteger openCount = new AtomicInteger(0);
    AtomicInteger colseCount = new AtomicInteger(0);

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        if (null == dataSource) {
            dataSource = new HikariDataSource();

            getDataSource(dataSource);
        }

        connection = dataSource.getConnection();
        System.out.println("获取连接" + openCount.incrementAndGet());
        String sql = "insert into `DataEvent`(`id`, `eventTime`,`value`) values(?, ?, ?);";
        if (connection != null) {
            ps = this.connection.prepareStatement(sql);
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        // 关闭连接和释放资源
        if (connection != null) {
            System.out.println("关闭连接" + colseCount.incrementAndGet());
            connection.close();
        }
        if (ps != null) {
            ps.close();
        }

        dataSource.close();
    }

    @Override
    public void invoke(DataEvent value, Context context) throws Exception {
        if (ps == null) {
            return;
        }

        ps.setString(1, value.getId());
        ps.setTimestamp(2, new java.sql.Timestamp(value.getEventTime().getTime()));
        ps.setInt(3, value.getValue());

        ps.execute();
        log.info("成功了插入了 {} 数据 openCount:{},closeCount:{} ", value, openCount.get(), colseCount.get());
    }

    private static void getDataSource(HikariDataSource dataSource) {
        dataSource.setDriverClassName("com.mysql.jdbc.Driver");
        // 注意，替换成自己本地的 mysql 数据库地址和用户名、密码
        dataSource.setJdbcUrl(
            "jdbc:mysql://localhost:3306/test?useSSL=false&useUnicode=true&characterEncoding=UTF8&serverTimezone=GMT");
        dataSource.setUsername("root");
        dataSource.setPassword("123456");
        // 设置连接池的一些参数
        dataSource.setConnectionTimeout(30000);
        // 连接池中允许的最大连接数。缺省值：10；推荐的公式：((core_count * 2) + effective_spindle_count)
        dataSource.setMaximumPoolSize(15);
        dataSource.setMaxLifetime(1800000);
    }
}
