package br.com.pehenmo.reactive.test;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscription;
import reactor.blockhound.BlockHound;
import reactor.blockhound.BlockingOperationError;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

@Slf4j
/**
 * 1. Asynchronously
 * 2. Non-Blocking
 * 3. Backpressure
 * Publish <- (subscribe)  <- Subcriber
 * Subscription is created
 * Publish -> (onSubscribe with the subscription) -> Subscriber
 * Subscription <- (request N) Subscriber
 * Publish -> (onNext) Subscriber
 * Until-
 * 1. Publisher sends all objects requested
 * 2. Publiser sends all objects requested it has. (onComplete) subscribers and subscription will be canceled.
 * 3. There is an error. (onError) subscribers and subscription will be canceled.
 */
public class MonoTest {

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
    public void monoSubscriberConsumerError(){
        String nome = "Pedro Henrique";
        Mono<String>  mono = Mono.just(nome).map(s -> {
            throw new RuntimeException("error");
        });
        mono.subscribe(s -> log.info("Name {}", s), s -> log.error("something bad happed"));
        mono.subscribe(s -> log.info("Name {}", s), Throwable::printStackTrace);
        log.info("---------------");
        StepVerifier.create(mono).expectError(RuntimeException.class).verify();
    }

    @Test
    public void monoSubscriberConsumer(){
        String nome = "Pedro Henrique";
        Mono<String>  mono = Mono.just(nome).log();
        mono.subscribe(s -> log.info("Name {}", s));
        log.info("---------------");
        StepVerifier.create(mono).expectNext(nome).verifyComplete();
    }

    @Test
    public void monoSubscriberConsumerComplete(){
        String nome = "PEDRO HENRIQUE";
        Mono<String>  mono = Mono.just(nome)
                .log()
                .map(s -> s.toUpperCase());

        mono.subscribe(s -> log.info("Name {}", s),
                Throwable::printStackTrace,
                () -> log.info("finished!"));

        log.info("------------------------------");

        StepVerifier.create(mono)
                .expectNext(nome)
                .verifyComplete();
    }

    @Test
    public void monoSubscriberConsumerCompleteSubscription(){
        String nome = "PEDRO HENRIQUE";
        Mono<String>  mono = Mono.just(nome)
                .log()
                .map(s -> s.toUpperCase());

        mono.subscribe(s -> log.info("Name {}", s),
                Throwable::printStackTrace,
                () -> log.info("finished!"),
                Subscription::cancel);

        log.info("------------------------------");

        StepVerifier.create(mono)
                .expectNext(nome)
                .verifyComplete();
    }

    @Test
    public void monoOnMethods(){
        String nome = "pedro henrique";
        Mono<String>  mono = Mono.just(nome)
                .map(s -> s.toUpperCase())
                .doOnSubscribe(s -> log.info("Subscribed!"))
                .doOnRequest(s -> log.info("Request received. Starting doing something "))
                .flatMap(s -> Mono.just("teste"))
                .doOnNext(s -> log.info("executing onNext {}", s))
                .doOnSuccess(s -> log.info("onSuccess executed {}", s))
                .log();


        mono.subscribe(s -> log.info("Name {}", s),
                Throwable::printStackTrace,
                () -> log.info("finished!"));

        log.info("----------------------------------");

        StepVerifier.create(mono)
                .expectNext("teste")
                .expectComplete()
                .verify();



    }



    @Test
    public void monoOnError(){
        String nome = "PEDRO HENRIQUE";
        Mono<Object>  mono = Mono.error(new IllegalAccessError("error IllegalAccessError"))
                .doOnError(e -> MonoTest.log.error("error message {}", e.getMessage()));
                //.doOnNext(e -> log.info("teste")).log();

        StepVerifier.create(mono)
                .expectError(IllegalAccessError.class)
                .verify();

    }

    @Test
    public void monoOnErrorResume(){
        String nome = "PEDRO HENRIQUE";
        Mono<Object>  mono = Mono.error(new IllegalAccessError("error IllegalAccessError"))
                .onErrorResume(e -> {
                    log.info("inside On Error Resume");
                    return Mono.just(nome);
                }   );

        StepVerifier.create(mono)
                .expectNext(nome)
                .verifyComplete();

    }

    @Test
    public void monoOnErrorResumeReturn(){
        String nome = "PEDRO HENRIQUE";
        Mono<Object>  mono = Mono.error(new IllegalAccessError("error IllegalAccessError"))
                .onErrorReturn("fallback")
                .onErrorResume(e -> {
                    log.info("inside On Error Resume");
                    return Mono.just(nome);
                }   );

        StepVerifier.create(mono)
                .expectNext("fallback")
                .verifyComplete();

    }
}
