package org.example.sinks;

import java.sql.Connection;
import java.sql.PreparedStatement;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.example.model.DataEvent;

import com.zaxxer.hikari.HikariDataSource;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SinkToMysql extends RichSinkFunction<DataEvent> {

    PreparedStatement ps;
    HikariDataSource dataSource;
    private Connection connection;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        dataSource = new HikariDataSource();
        connection = getConnection(dataSource);
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
            connection.close();
        }
        if (ps != null) {
            ps.close();
        }
    }

    @Override
    public void invoke(DataEvent value, Context context) throws Exception {
        if (ps == null) {
            return;
        }

        ps.setString(1, value.getId());
        ps.setDate(2, new java.sql.Date(value.getEventTime().getTime()));
        ps.setInt(3, value.getValue());

        ps.execute();
        log.info("成功了插入了 {} 数据", value);
    }

    private static Connection getConnection(HikariDataSource dataSource) {
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

        Connection con = null;
        try {
            con = dataSource.getConnection();
            log.info("创建连接池：{}", con);
        } catch (Exception e) {
            log.error("-----------mysql get connection has exception , msg = {}", e.getMessage());
        }
        return con;
    }
}
