package com.example.alok;


import org.apache.commons.io.output.ByteArrayOutputStream;
import org.springframework.amqp.core.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ImageBanner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.core.io.FileSystemResource;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.integration.dsl.IntegrationFlows;
import org.springframework.integration.dsl.channel.MessageChannels;
import org.springframework.integration.dsl.file.Files;
import org.springframework.integration.file.FileHeaders;
import org.springframework.integration.ftp.session.DefaultFtpSessionFactory;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.integration.transformer.GenericTransformer;
import org.springframework.messaging.Message;
import org.springframework.messaging.MessageChannel;
import org.springframework.util.ReflectionUtils;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;

@SpringBootApplication
public class FileIntegrationApplication {

    @Configuration
    public  static class FtpConfig {
        @Bean
        DefaultFtpSessionFactory defaultftpSessionFactory(
                @Value("${ftp.port:21}") int port,
                @Value("${ftp.username:alokkulkarni}") String username,
                @Value("${ftp.password:Sheetli102*}") String password
        ) {
            DefaultFtpSessionFactory ftpSessionFactory = new DefaultFtpSessionFactory();
            ftpSessionFactory.setPort(port);
            ftpSessionFactory.setUsername(username);
            ftpSessionFactory.setPassword(password);
            ftpSessionFactory.setHost("localhost");

            return ftpSessionFactory;
        }
    }

    @Configuration
    public static class AmqpConfiguration {


        private final String ascii = "ascii";

        @Bean
        Exchange exchange() {
            return ExchangeBuilder.directExchange(this.ascii).durable().build();
        }


        @Bean
        Queue queue() {
            return QueueBuilder.durable(this.ascii).build();
        }


        @Bean
        Binding binding() {
            return BindingBuilder.bind(queue()).to(exchange()).with(this.ascii).noargs();
        }
    }

    @Bean
    IntegrationFlow files(@Value("${input-directory:${HOME}/Documents/in}")
                                  File in,
                                  Environment environment
                                  ) {

        GenericTransformer<File, Message<String>> filesStringGenericTransformer = (File source) -> {
            try (   ByteArrayOutputStream baos = new ByteArrayOutputStream();
                    PrintStream printStream = new PrintStream(baos)) {

                ImageBanner imageBanner = new ImageBanner(new FileSystemResource(source));
                imageBanner.printBanner(environment, getClass(),printStream);
                return MessageBuilder.withPayload(new String(baos.toByteArray()))
                        .setHeader(FileHeaders.FILENAME, source.getAbsoluteFile().getName())
                        .build();
            } catch (IOException e) {
                ReflectionUtils.rethrowRuntimeException(e);
            }
            return null;
        };

        return IntegrationFlows
                .from(Files.inboundAdapter(in).autoCreateDirectory(true).preventDuplicates(true).patternFilter("*.jpg"), poller-> poller.poller(p->p.fixedRate(1000)))
                .transform(File.class, filesStringGenericTransformer)
                .channel(this.asciiProcessors())
                .get();
    }

    @Bean
    IntegrationFlow amqp(AmqpTemplate amqpTemplate) {
        return IntegrationFlows.from(this.asciiProcessors())
                .handleWithAdapter(adapters -> adapters.amqp(amqpTemplate)
                    .exchangeName("ascii")
                    .routingKey("ascii"))
                .get();
    }

    @Bean
    IntegrationFlow ftp(DefaultFtpSessionFactory ftpSessionFactory) {
        return IntegrationFlows.from(this.asciiProcessors())
                               .handleWithAdapter(adapters -> adapters.ftp(ftpSessionFactory)
                                    .remoteDirectory("ftpDir")
                                    .fileNameGenerator(message -> {
                                        Object o = message.getHeaders().get(FileHeaders.FILENAME);
                                        String fileName = String.class.cast(o);
                                        return fileName.split("\\.")[0] + ".txt";
                                    })
                               )
                               .get();
    }


    @Bean
    MessageChannel asciiProcessors() {
        return MessageChannels.publishSubscribe().get();
    }


    public static void main(String[] args) {
        SpringApplication.run(FileIntegrationApplication.class, args);
    }
}
