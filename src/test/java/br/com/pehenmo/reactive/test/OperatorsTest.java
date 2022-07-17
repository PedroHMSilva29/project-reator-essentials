package br.com.pehenmo.reactive.test;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import reactor.blockhound.BlockHound;
import reactor.blockhound.BlockingOperationError;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
public class OperatorsTest {

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
    public void subscribeOnSimple() throws InterruptedException {
        Flux<Integer> flux = Flux.range(1,5)
                .map(i -> {
                    log.info("Map 1 - Number {} on Thread {}", i, Thread.currentThread().getName());
                    return i;
                })
                .subscribeOn(Schedulers.single())
                .map(i -> {
                    log.info("Map 2 - Number {} on Thread {}", i, Thread.currentThread().getName());
                    return i;
                });

        StepVerifier.create(flux)
                .expectSubscription()
                .expectNext(1,2,3,4,5)
                .verifyComplete();

    }

    @Test
    public void publishOnSimple() throws InterruptedException {
        Flux<Integer> flux = Flux.range(1,5)
                .map(i -> {
                    log.info("Map 1 - Number {} on Thread {}", i, Thread.currentThread().getName());
                    return i;
                })
                .publishOn(Schedulers.single())
                .map(i -> {
                    log.info("Map 2 - Number {} on Thread {}", i, Thread.currentThread().getName());
                    return i;
                });

        StepVerifier.create(flux)
                .expectSubscription()
                .expectNext(1,2,3,4,5)
                .verifyComplete();

    }

    @Test
    public void publishAndSubscribeOnSimple() throws InterruptedException {
        Flux<Integer> flux = Flux.range(1,5)
                .publishOn(Schedulers.single())
                .map(i -> {
                    log.info("Map 1 - Number {} on Thread {}", i, Thread.currentThread().getName());
                    return i;
                })
                .subscribeOn(Schedulers.boundedElastic())
                .map(i -> {
                    log.info("Map 2 - Number {} on Thread {}", i, Thread.currentThread().getName());
                    return i;
                });

        StepVerifier.create(flux)
                .expectSubscription()
                .expectNext(1,2,3,4,5)
                .verifyComplete();

    }

    @Test
    public void subscribeAndPublishOnSimple() throws InterruptedException {
        Flux<Integer> flux = Flux.range(1,5)
                .subscribeOn(Schedulers.single())
                .map(i -> {
                    log.info("Map 1 - Number {} on Thread {}", i, Thread.currentThread().getName());
                    return i;
                })
                .publishOn(Schedulers.boundedElastic())
                .map(i -> {
                    log.info("Map 2 - Number {} on Thread {}", i, Thread.currentThread().getName());
                    return i;
                });

        StepVerifier.create(flux)
                .expectSubscription()
                .expectNext(1,2,3,4,5)
                .verifyComplete();

    }

    @Test
    public void subscribeOnIO()  {
        Mono<List<String>> list = Mono.fromCallable(() -> Files.readAllLines(Paths.get("text-file")))
                .log()
                .subscribeOn(Schedulers.boundedElastic());

        //list.subscribe(s -> log.info("{}",s));

        StepVerifier.create(list)
                .expectSubscription()
                .thenConsumeWhile(l -> {
                    Assertions.assertFalse(l.isEmpty());
                    log.info("size: {}", l.size());
                    return true;

                })
                .expectComplete()
                .verify();
    }

    @Test
    public void switchIfEmptyOperator()  {
        Flux<Object> flux = Flux.empty()
                .switchIfEmpty(Flux.just("not empty now!"))
                .log();

        StepVerifier.create(flux)
                .expectSubscription()
                .expectNext("not empty now!")
                .expectComplete()
                .verify();
    }

    @Test
    public void deferOperator() throws InterruptedException {
        Mono<Long> mono = Mono.just(System.currentTimeMillis())
                .log();

        Mono<Long> defer = Mono.defer(() -> Mono.just(System.currentTimeMillis()));

        defer.subscribe(l -> log.info("{}", l));
        Thread.sleep(200);

        defer.subscribe(l -> log.info("{}", l));
        Thread.sleep(200);

        defer.subscribe(l -> log.info("{}", l));
        Thread.sleep(200);

        AtomicLong atomicLong = new AtomicLong();
        defer.subscribe(atomicLong::set);
        Assertions.assertTrue(atomicLong.get() > 0);

    }

    @Test
    public void concatOperator() {
        Flux<Integer> flux1 = Flux.just(1,2);
        Flux<Integer> flux2 = Flux.just(3,4);

        Flux<Integer> fluxConcat = Flux.concat(flux1, flux2).log();

        StepVerifier.create(fluxConcat)
                .expectSubscription()
                .expectNext(1,2,3,4)
                .expectComplete()
                .verify();

    }

    @Test
    public void concatWithOperator() {
        Flux<Integer> flux1 = Flux.just(1,2);
        Flux<Integer> flux2 = Flux.just(3,4);

        Flux<Integer> fluxConcat = flux1.concatWith(flux2)
                .log();

        StepVerifier.create(fluxConcat)
                .expectSubscription()
                .expectNext(1,2,3,4)
                .expectComplete()
                .verify();

    }

    @Test
    public void combineLatestOperator() {
        Flux<String> flux1 = Flux.just("a","b");
        Flux<String> flux2 = Flux.just("c","d");

        Flux<String> fluxConcat = Flux.combineLatest(flux1, flux2,
                (s1, s2) -> s1.toUpperCase() + s2.toUpperCase()).log();

        StepVerifier.create(fluxConcat)
                .expectSubscription()
                .expectNext("BC", "BD")
                .expectComplete()
                .verify();

    }

    @Test
    public void mergeOperator() {
        Flux<Integer> flux1 = Flux.just(1,2);
        Flux<Integer> flux2 = Flux.just(3,4);

        Flux<Integer> fluxMerge = Flux.merge(flux1, flux2).log();

        StepVerifier.create(fluxMerge)
                .expectSubscription()
                .expectNext(1,2,3,4)
                .expectComplete()
                .verify();

    }

    @Test
    public void mergeWithOperator() {
        Flux<Integer> flux1 = Flux.just(1,2).delayElements(Duration.ofMillis(100));
        Flux<Integer> flux2 = Flux.just(3,4);

        Flux<Integer> fluxMerge = Flux.merge(flux1, flux2).log();

        StepVerifier.create(fluxMerge)
                .expectSubscription()
                .expectNext(3,4,1,2)
                .expectComplete()
                .verify();

    }

    @Test
    public void mergeSequentialOperator() {
        Flux<Integer> flux1 = Flux.just(1,2).delayElements(Duration.ofMillis(100));
        Flux<Integer> flux2 = Flux.just(3,4);

        Flux<Integer> fluxMerge = Flux.mergeSequential(flux1, flux2).log();

        StepVerifier.create(fluxMerge)
                .expectSubscription()
                .expectNext(1,2,3,4)
                .expectComplete()
                .verify();

    }

    @Test
    public void mergeOperatorError() {
        Flux<Integer> flux1 = Flux.just(1,2).map(s -> {
            if(s == 2) throw new IllegalArgumentException("Illegal Argument Exception");
            return s;
        })
                .doOnError(s -> log.error("do something here"));
        Flux<Integer> flux2 = Flux.just(3,4);

        Flux<Integer> fluxMerge = Flux.mergeDelayError(1, flux1, flux2).log();

        StepVerifier.create(fluxMerge)
                .expectSubscription()
                .expectNext(1,3,4)
                .expectError(IllegalArgumentException.class)
                .verify();

    }

    @Test
    public void concatOperatorError() {
        Flux<Integer> flux1 = Flux.just(1,2).map(s -> {
            if(s == 2) throw new IllegalArgumentException("Illegal Argument Exception");
            return s;
        })
                .doOnError(s -> log.error("do something here"));
        Flux<Integer> flux2 = Flux.just(3,4);

        Flux<Integer> fluxMerge = Flux.concatDelayError( flux1, flux2).log();
        StepVerifier.create(fluxMerge)
                .expectSubscription()
                .expectNext(1,3,4)
                .expectError(IllegalArgumentException.class)
                .verify();

    }

    @Test
    public void flatMapOperator() {
        Flux<String> flux = Flux.just("a","b");

        Flux<String> flatFlux = flux.flatMap(this::findByName).log();

        StepVerifier.create(flatFlux)
                .expectSubscription()
                .expectNext("ValueB", "ValueA")
                .expectComplete()
                .verify();

    }

    private Flux<String> findByName(String name){
        if("a".equals(name)) return Flux.just("ValueA").delayElements(Duration.ofMillis(100));
        return Flux.just("ValueB");
    }

    @Test
    public void flatMapSequentialOperator() {
        Flux<String> flux = Flux.just("a","b");

        Flux<String> flatFlux = flux.flatMapSequential(this::findByName).log();

        StepVerifier.create(flatFlux)
                .expectSubscription()
                .expectNext("ValueA", "ValueB")
                .expectComplete()
                .verify();

    }

    @Test
    public void zipOperator() {
        Flux<String> titleFlux = Flux.just("Grand Blue","Attack on Titan");
        Flux<String> studioFlux = Flux.just("Gihle","TKM");
        Flux<Integer> episodesFlux = Flux.just(50, 25);

        Flux<Anime> zipFlux = Flux.zip(titleFlux, studioFlux, episodesFlux)
                .flatMap(s -> Flux.just(new Anime(s.getT1(),s.getT2(), s.getT3()))).log();

        StepVerifier.create(zipFlux)
                .expectSubscription()
                .expectNext(new Anime("Grand Blue","Gihle", 50))
                .expectNext(new Anime("Attack on Titan","TKM", 25))
                .expectComplete()
                .verify();

    }

    @Test
    public void zipWithOperator() {
        Flux<String> titleFlux = Flux.just("Grand Blue","Attack on Titan");
        Flux<String> studioFlux = Flux.just("Gihle","TKM");


        Flux<Anime> zipWithFlux = titleFlux.zipWith(studioFlux)
                .flatMap(s -> Flux.just(new Anime(s.getT1(),s.getT2(), null))).log();

        StepVerifier.create(zipWithFlux)
                .expectSubscription()
                .expectNext(new Anime("Grand Blue","Gihle", null))
                .expectNext(new Anime("Attack on Titan","TKM", null))
                .expectComplete()
                .verify();

    }

    @AllArgsConstructor
    @Data
    @ToString
    @EqualsAndHashCode
    public class Anime{
        private String title;
        private String studio;
        private Integer episodes;
    }

}
