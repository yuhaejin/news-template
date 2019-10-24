package com.tachyon.news.template.config;

import com.github.mustachejava.DefaultMustacheFactory;
import com.github.mustachejava.Mustache;
import com.github.mustachejava.MustacheFactory;
import com.netflix.client.DefaultLoadBalancerRetryHandler;
import com.netflix.client.RetryHandler;
import com.netflix.loadbalancer.ILoadBalancer;
import com.netflix.loadbalancer.LoadBalancerBuilder;
import com.netflix.loadbalancer.Server;
import com.netflix.loadbalancer.reactive.LoadBalancerCommand;
import com.tachyon.crawl.kind.util.LoadBalancerCommandHelper;
import com.tachyon.crawl.kind.util.ProxyHelper;
import com.tachyon.news.template.repository.TemplateMapper;
import com.tachyon.news.template.telegram.TachyonMonitoringBot;
import com.tachyon.news.template.telegram.TachyonNewsFlashBot;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHost;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.impl.nio.conn.PoolingNHttpClientConnectionManager;
import org.apache.http.impl.nio.reactor.DefaultConnectingIOReactor;
import org.apache.http.nio.reactor.ConnectingIOReactor;
import org.apache.http.nio.reactor.IOReactorException;
import org.elasticsearch.action.bulk.*;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.telegram.telegrambots.ApiContextInitializer;
import org.telegram.telegrambots.meta.TelegramBotsApi;
import org.telegram.telegrambots.meta.exceptions.TelegramApiRequestException;

import javax.annotation.PostConstruct;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Slf4j
@Configuration
public class NewsConfig {
    @Autowired
    private MyContext myContext;
    @Autowired
    private TemplateMapper templateMapper;

    @PostConstruct
    public void init() {
//        try {
//            ApiContextInitializer.init();
//            TelegramBotsApi telegramBotsApi = new TelegramBotsApi();
//            telegramBotsApi.registerBot(tachyonNewsFlashBot());
//            log.info("initialized ... TelegramBotsApi");
//        } catch (TelegramApiRequestException e) {
//            log.error(e.getMessage(),e);
//        }
    }

//    @Bean
//    public ConnectionFactory connectionFactory(){
//        ConnectionFactory factory = new ConnectionFactory();
//        factory.setHost("localhost");
//        factory.setPort(5672);
//        factory.setUsername("lloydkwon");
//        factory.setPassword("miso2miso");
//
//        return factory;
//    }

    @Bean
    public TaskExecutor taskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(myContext.getCorePoolSize());
        executor.setMaxPoolSize(myContext.getMaxPoolSize());
        executor.setQueueCapacity(myContext.getQueueCapacity());
        return executor;
    }




    @Bean(destroyMethod = "close")
    public RestHighLevelClient restHighLevelClient() {
        return new RestHighLevelClient(RestClient.builder(new HttpHost(myContext.getElasticUrl(), myContext.getElasticPort(), "http")));
    }

    @Bean
    public BulkProcessor.Listener listener() {
        BulkProcessor.Listener listener = new BulkProcessor.Listener() {
            int count = 0;

            @Override
            public void beforeBulk(long l, BulkRequest bulkRequest) {
                count = count + bulkRequest.numberOfActions();
                log.info("indexing...  accumulatedCount=" + count + " size(mb)=" + bulkRequest.estimatedSizeInBytes() / (1024 * 1024) + " acionts=" + bulkRequest.numberOfActions());
            }

            @Override
            public void afterBulk(long l, BulkRequest bulkRequest, BulkResponse bulkResponse) {
                if (bulkResponse.hasFailures()) {
                    for (BulkItemResponse bulkItemResponse : bulkResponse) {
                        if (bulkItemResponse.isFailed()) {
                            BulkItemResponse.Failure failure = bulkItemResponse.getFailure();
                            log.error(bulkItemResponse.getOpType().toString() + " " + failure.toString());
                        }
                    }
                }
            }

            @Override
            public void afterBulk(long l, BulkRequest bulkRequest, Throwable throwable) {
                log.error(throwable.toString());
            }
        };

        return listener;
    }


    @Bean(destroyMethod = "close")
    public BulkProcessor bulkProcessor() {
        BulkProcessor.Builder builder = BulkProcessor.builder(
                (request, bulkListener) ->
                        restHighLevelClient().bulkAsync(request, RequestOptions.DEFAULT, bulkListener),
                listener());
        builder.setBulkActions(myContext.getBulkActionCount());
        builder.setFlushInterval(TimeValue.timeValueSeconds(myContext.getFlushInterval()));
        builder.setBulkSize(new ByteSizeValue(myContext.getBulkSize(), ByteSizeUnit.MB));
        builder.setBackoffPolicy(BackoffPolicy
                .constantBackoff(TimeValue.timeValueSeconds(1L), 3));
        BulkProcessor bulkProcessor = builder.build();

        return bulkProcessor;
    }

    @Bean
    public LoadBalancerCommandHelper loadBalancerCommandHelper() {
        LoadBalancerCommand<Object> balancerCommand = loadBalancerCommand();
        if (balancerCommand == null) {
            return null;
        } else {
            return new LoadBalancerCommandHelper(balancerCommand);
        }
    }

    @Bean
    public LoadBalancerCommand<Object> loadBalancerCommand() {
        ILoadBalancer iLoadBalancer = loadBalancer();
        if (iLoadBalancer == null) {
            return null;
        } else {

            return LoadBalancerCommand.<Object>builder()
                    .withLoadBalancer(iLoadBalancer)
                    .withRetryHandler(retryHandler())
                    .build();
        }
    }

    /**
     * @return
     */
    @Bean
    public RetryHandler retryHandler() {
        return new DefaultLoadBalancerRetryHandler(1, 3, true);
    }

    @Bean
    public ILoadBalancer loadBalancer() {
        List<Server> servers = serverList();
        if (servers.size() == 0) {
            return null;
        } else {

            return LoadBalancerBuilder.newBuilder().buildFixedServerListLoadBalancer(servers);
        }
    }

    @Bean
    public List<Server> serverList() {
        List<Server> servers = new ArrayList<>();
        String hosts = ProxyHelper.findProxyHost();
        int port = ProxyHelper.findProxyPort();
        String[] strings = StringUtils.splitByWholeSeparatorPreserveAllTokens(hosts, ",");
        for (String s : strings) {
            servers.add(new Server(s, port));
        }

        return servers;
    }


    @Bean(destroyMethod = "close")
    public CloseableHttpAsyncClient asyncClient() {
        try {
            ConnectingIOReactor ioReactor = new DefaultConnectingIOReactor();
            PoolingNHttpClientConnectionManager cm = new PoolingNHttpClientConnectionManager(ioReactor);
            CloseableHttpAsyncClient client = HttpAsyncClients.custom().setConnectionManager(cm).build();
            client.start();
            return client;
        } catch (IOReactorException e) {
            log.error(e.getMessage(),e);
            return null;
        }

    }

    @Bean
    public TachyonNewsFlashBot tachyonNewsFlashBot() {
        return new TachyonNewsFlashBot(templateMapper);
    }
    @Bean
    public TachyonMonitoringBot tachyonMonitoringBot() {
        return new TachyonMonitoringBot();
    }
    @Bean
    public Mustache mustache() {
        MustacheFactory mf = new DefaultMustacheFactory();

        String c =
//                "봇이름: tachyonnews_bot\n" +
                        "중요공시 속보\n" +
                        "발생시간: {{time}}\n" +
                        "키워드: <b>{{keyword}}</b>\n" +
                        "기업명: <b>{{company}}</b>\n" +
                        "공시명: <a href=\"{{docUrl}}\">{{docNm}}</a>\n" +
                        "기초공시명: <a href=\"{{acptUrl}}\">{{acptNm}}</a>";
        Mustache mustache = mf.compile(new StringReader(c), "example");
        return mustache;
    }
    @Bean
    public Mustache investmentMustache() {
        MustacheFactory mf = new DefaultMustacheFactory();

        String c =
//                "봇이름: tachyonnews_mining_bot\n" +
                        "투자자 투자속보\n" +
                        "투자자: <b>{{investor}}</b>\n" +
                        "발생시간: {{time}}\n" +
                        "종목: <b>{{company}}</b>\n" +
                        "변동전: <b>{{before}}</b>\n" +
                        "변동후: <b>{{after}}</b>\n" +
                        "단가: <b>{{price}}</b>\n" +
                        "실현액: <b>{{sum}}</b>\n" +
                        "공시명: <a href=\"{{docUrl}}\">{{docNm}}</a>\n" +
                        "기초공시명: <a href=\"{{acptUrl}}\">{{acptNm}}</a>";
        Mustache mustache = mf.compile(new StringReader(c), "example2");
        return mustache;
    }
    @Bean
    public Mustache relativeMustache() {
        MustacheFactory mf = new DefaultMustacheFactory();

        String c =
//                "봇이름: tachyonnews_mining_bot\n" +
                        "새로운 친인척 속보\n" +
                        "회사: {{company}}\n" +
                        "발생시간: {{time}}\n" +
                        "친인척: <b>{{name}}</b>\n" +
                        "생일: <b>{{birth}}</b>\n" +
                        "성별: <b>{{gender}}</b>\n" +
                        "국내외: <b>{{homeabroad}}</b>\n" +
                        "국적: <b>{{nationality}}</b>\n" +
                        "관계: <b>{{relationHim}}</b>\n" +
                        "회사와의관계: <b>{{relationCom}}</b>\n" +
                        "직업: <b>{{job}}</b>\n" +
                        "공시명: <a href=\"{{docUrl}}\">{{docNm}}</a>\n" +
                        "기초공시명: <a href=\"{{acptUrl}}\">{{acptNm}}</a>";
        Mustache mustache = mf.compile(new StringReader(c), "example2");
        return mustache;
    }
    @Bean(destroyMethod = "shutdown")
    public ExecutorService groupThreadPool() {
        return Executors.newFixedThreadPool(2);
    }

    @Bean(destroyMethod = "shutdown")
    public ExecutorService noGroupThreadPool() {
        return Executors.newFixedThreadPool(5);
    }
}
