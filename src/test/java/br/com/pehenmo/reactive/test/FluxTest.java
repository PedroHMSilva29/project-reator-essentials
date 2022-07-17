package br.com.pehenmo.reactive.test;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.blockhound.BlockHound;
import reactor.blockhound.BlockingOperationError;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.ConnectableFlux;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

@Slf4j
public class FluxTest {

    @BeforeAll
    public static void setup(){
        BlockHound.install();
    }

    @Test
    public void blockHoundWorks() {
        try {
            FutureTask<?> task = new FutureTask<>(() -> {
                Thread.sleep(0);
                return "";
            });
            Schedulers.parallel().schedule(task);

            task.get(10, TimeUnit.SECONDS);
            Assertions.fail("should fail");
        } catch (Exception e) {
            Assertions.assertTrue(e.getCause() instanceof BlockingOperationError);
        }
    }

    @Test
    public void fluxSubscribe() {
        Flux<String> flux = Flux.just("Pedro Henrique", "Luis Galonetti").log();

        StepVerifier.create(flux)
                .expectNext("Pedro Henrique", "Luis Galonetti")
                .verifyComplete();
    }

    @Test
    public void fluxSubscribeNumbers() {
        Flux<Integer> flux = Flux.range(1, 5).log();

        flux.subscribe(s -> log.info("subscribed {}", s));
        log.info("------------------------------------------");

        StepVerifier.create(flux)
                .expectNext(1, 2, 3, 4, 5)
                .verifyComplete();
    }

    @Test
    public void fluxSubscribeNumbersError() {
        Flux<Integer> flux = Flux.range(1, 5).log()
                .map(s -> {
                    if (s == 4) throw new ArithmeticException("error ArithmeticException");
                    return s;
                });

        flux.subscribe(s -> log.info("subscribed {}", s), Throwable::printStackTrace, () -> log.info("done!"));
        log.info("------------------------------------------");

        StepVerifier.create(flux)
                .expectNext(1, 2, 3)
                .expectError(ArithmeticException.class)
                .verify();
    }

    @Test
    public void fluxSubscribeNumbersRequest3() {
        Flux<Integer> flux = Flux.range(1, 5).log()
                .map(s -> {
                    if (s == 4) throw new ArithmeticException("error ArithmeticException");
                    return s;
                });

        flux.subscribe(s -> log.info("subscribed {}", s),
                Throwable::printStackTrace, () -> log.info("done!"),
                subscription -> subscription.request(3));
        log.info("------------------------------------------");

        StepVerifier.create(flux)
                .expectNext(1, 2, 3)
                .expectError(ArithmeticException.class)
                .verify();
    }


    @Test
    public void fluxSubscribeUglyNumbers() {
        Flux<Integer> flux = Flux.range(1, 10).log();

        flux.subscribe(new Subscriber<Integer>() {
            private int count = 0;
            private int subscriptionNumber = 2;
            private Subscription subscription;

            @Override
            public void onSubscribe(Subscription subscription) {
                this.subscription = subscription;
                subscription.request(subscriptionNumber);
            }

            @Override
            public void onNext(Integer integer) {
                count++;
                if (count >= subscriptionNumber) {
                    count = 0;
                    subscription.request(subscriptionNumber);
                }

            }

            @Override
            public void onError(Throwable throwable) {

            }

            @Override
            public void onComplete() {

            }
        });
        log.info("------------------------------------------");

        StepVerifier.create(flux)
                .expectNext(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
                .verifyComplete();
    }

    @Test
    public void fluxSubscribeNotSoUglyNumbers() {
        Flux<Integer> flux = Flux.range(1, 10).log();

        flux.subscribe(new BaseSubscriber<Integer>() {
            private int count = 0;
            private final int requestCount = 2;

            @Override
            protected void hookOnSubscribe(Subscription subscription) {
                request(requestCount);
            }

            @Override
            protected void hookOnNext(Integer value) {
                count++;
                if (count >= requestCount) {
                    count = 0;
                    request(requestCount);
                }
            }
        });
        log.info("------------------------------------------");

        StepVerifier.create(flux)
                .expectNext(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
                .verifyComplete();
    }

    @Test
    public void fluxSubscribePrettyBackpressure() {
        Flux<Integer> flux = Flux.range(1, 5)
                .log()
                .limitRate(3);

        flux.subscribe(s -> log.info("subscribed {}", s));
        log.info("------------------------------------------");

        StepVerifier.create(flux)
                .expectNext(1, 2, 3, 4, 5)
                .verifyComplete();
    }


    @Test
    public void fluxSubscriberIntervalOne() throws InterruptedException {
        Flux<Long> interval = Flux.interval(Duration.ofMillis(100))
                .take(10).log();

        interval.subscribe(i -> log.info("Number {}", i));
        Thread.sleep(3000);
    }

    @Test
    public void fluxSubscriberIntervalTwo()  {
        StepVerifier.withVirtualTime(this::createInterval)
                .expectSubscription()
                .expectNoEvent(Duration.ofDays(1))
                .thenAwait(Duration.ofDays(1))
                .expectNext(0L)
                .thenAwait(Duration.ofDays(1))
                .expectNext(1L)
                .thenCancel()
                .verify();
    }

    public Flux<Long> createInterval(){
        return Flux.interval(Duration.ofDays(1)).log();
    }

    @Test
    public void connectableFlux() throws InterruptedException {
        ConnectableFlux<Integer> connectableFlux = Flux.range(1,10)
                .log()
                .delayElements(Duration.ofMillis(100))
                .publish();

        connectableFlux.connect();
        log.info("thread sleeping for 300ms");
        Thread.sleep(300);
        connectableFlux.subscribe(i -> log.info("number {}", i));
    }

    @Test
    public void connectableFluxTwo() throws InterruptedException {
        ConnectableFlux<Integer> connectableFlux = Flux.range(1,10)
                .log()
                .delayElements(Duration.ofMillis(100))
                .publish();

        connectableFlux.connect();

        StepVerifier.create(connectableFlux)
                .then(connectableFlux::connect)
                .thenConsumeWhile(i -> i>2)
                .expectNext(1,2)
                .expectNext(3);

    }

    @Test
    public void connectableFluxAutoConnect() throws InterruptedException {
        Flux<Integer> fluxAutoConnect = Flux.range(1,5)
                .log()
                .delayElements(Duration.ofMillis(100))
                .publish()
                .autoConnect(3);

        StepVerifier.create(fluxAutoConnect)
                .then(fluxAutoConnect::subscribe)
                .then(fluxAutoConnect::subscribe)
                .expectNext(1,2,3,4,5)
                .verifyComplete();

    }





}
