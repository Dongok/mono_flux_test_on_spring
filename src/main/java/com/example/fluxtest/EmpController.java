package com.example.fluxtest;

import java.net.ConnectException;
import java.time.Duration;
import java.util.concurrent.TimeoutException;
import java.util.logging.Logger;

import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.ResponseEntity.HeadersBuilder;
import org.springframework.http.client.reactive.ClientHttpConnector;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientRequestException;
import org.springframework.web.server.ResponseStatusException;

import io.micrometer.common.util.StringUtils;
import io.netty.handler.logging.LogLevel;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.logging.Level;

@RestController
@RequestMapping("/emp")
public class EmpController {

    @GetMapping(value = "/list", produces = MediaType.APPLICATION_JSON_VALUE)
    public Flux<String> getAllEmployees() {
        //return Flux.just("A","B","C").delayElements(Duration.ofSeconds(1)).log().delaySubscription(Duration.ofSeconds(1)).log().;
        return Flux.interval(Duration.ofSeconds(1)).take(5).map(i -> String.valueOf(i));
    }

    @GetMapping(value = "/list_stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<String> getAllEmployeesStream() {
        //return Flux.just("A","B","C").delayElements(Duration.ofSeconds(1)).log().delaySubscription(Duration.ofSeconds(1)).log().;
        return Flux.interval(Duration.ofSeconds(1)).take(5).map(i -> String.valueOf(i));
    }

    @GetMapping(value = "/get_api_1")
    public Mono<ResponseEntity> getApi1() {
        MultiValueMap<String, String> hd = new LinkedMultiValueMap<>();
        hd.add("Content-Type", "application/json");
        hd.add("x-a", "ab");
        hd.add("x-b", "bc");

        return Mono.just(new ResponseEntity<>("", hd, HttpStatus.OK));
    }

    @GetMapping(value = "/get_api_2")
    public Mono<ResponseEntity> getWebClient() {
        Logger log = Logger.getLogger(this.getClass().getName());
        return WebClient.builder().baseUrl("http://127.0.0.1:1921/signal_play").build().get().exchangeToMono(resp -> {
                            if (HttpStatus.OK == resp.statusCode()) {
                                return resp.bodyToMono(String.class);
                            } else {
                                return resp.createError();
                            }
                        }).take(Duration.ofMillis(100))
                        .onErrorMap(TimeoutException.class, e -> new ResponseStatusException(HttpStatus.REQUEST_TIMEOUT, "TIMEOUT"))
                        .onErrorMap(WebClientRequestException.class, e -> new ResponseStatusException(HttpStatus.GATEWAY_TIMEOUT, "CONNECT_ERROR"))
                        .log().doOnEach(signal -> {
                    log.info(signal.toString());
                }).map(v -> new ResponseEntity<>(v, null, HttpStatus.OK));
    }

    @GetMapping(value = "/get_api_3")
    public Mono<ResponseEntity> getWebClient3() {
        WebClient wc = WebClient.builder().baseUrl("http://127.0.0.1:1921/signal_play").build();

        return Mono.delay(Duration.ofMillis(10)).then(wc.get().exchangeToMono(resp -> {
                       if (HttpStatus.OK == resp.statusCode()) {
                           return resp.bodyToMono(String.class);
                       } else {
                           return resp.createError();
                       }
                   })).onErrorMap(TimeoutException.class, e -> new ResponseStatusException(HttpStatus.REQUEST_TIMEOUT, "TIMEOUT"))
                   .onErrorMap(WebClientRequestException.class, e -> new ResponseStatusException(HttpStatus.GATEWAY_TIMEOUT, "CONNECT_ERROR"))
                   .log().map(v -> new ResponseEntity<>(v, null, HttpStatus.OK));
    }

    @GetMapping(value = "/get_api_4")
    public Mono<ResponseEntity> getWebClient4() {
        WebClient wc = WebClient.builder().baseUrl("http://127.0.0.1:1921/signal_play").build();

        return Mono.delay(Duration.ofMillis(200)).then(wc.get().exchangeToMono(resp -> {
                       if (HttpStatus.OK == resp.statusCode()) {
                           return resp.bodyToMono(String.class);
                       } else {
                           return resp.createError();
                       }
                   })).take(Duration.ofMillis(100)).onErrorMap(TimeoutException.class, e -> new ResponseStatusException(HttpStatus.REQUEST_TIMEOUT, "TIMEOUT"))
                   .onErrorMap(WebClientRequestException.class, e -> new ResponseStatusException(HttpStatus.GATEWAY_TIMEOUT, "CONNECT_ERROR"))
                   .map(v -> new ResponseEntity<>(v, null, HttpStatus.OK));
    }

    @GetMapping(value = "/get_api_5")
    public Mono<ResponseEntity> getWebClient5() {
        Logger logger = Logger.getLogger(this.getClass().getName());
        WebClient wc = WebClient.builder().baseUrl("http://127.0.0.1:1921/signal_play").build();
        logger.info(wc.toString());
        return wc.get().exchangeToMono(resp -> {
                     if (HttpStatus.OK == resp.statusCode()) {
                         return resp.bodyToMono(String.class);
                     } else {
                         return resp.createError();
                     }
                 }).timeout(Duration.ofMillis(100))
                 .doOnError(v -> {
                     logger.log(Level.WARNING, v.toString());
                 })
                 .onErrorMap(TimeoutException.class, e -> new ResponseStatusException(HttpStatus.REQUEST_TIMEOUT, "TIMEOUT"))
                 .onErrorMap(WebClientRequestException.class, e -> new ResponseStatusException(HttpStatus.REQUEST_TIMEOUT, "CONNECT_ERROR"))
                 .map(v -> new ResponseEntity<>(v, null, HttpStatus.OK));
    }
}
