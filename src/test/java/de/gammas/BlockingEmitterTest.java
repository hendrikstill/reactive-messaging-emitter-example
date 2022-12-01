package de.gammas;

import io.quarkus.test.junit.QuarkusTest;
import org.eclipse.microprofile.reactive.messaging.Channel;
import org.eclipse.microprofile.reactive.messaging.Emitter;
import org.eclipse.microprofile.reactive.messaging.Message;
import org.junit.jupiter.api.Test;

import javax.inject.Inject;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;

import static org.junit.jupiter.api.Assertions.assertThrows;

@QuarkusTest
public class BlockingEmitterTest {

    @Inject
    @Channel("words-out")
    Emitter<String> emitter;


    @Test
    public void assertThatNAckWillThrowExceptionWhenKafkaIsUnavailable() {

        var message = Message.of("my-payload");

        assertThrows(CompletionException.class, () -> {
            send(message).join(); //Block here until ack or nack/exception
        });
    }

    private CompletableFuture<Void> send(Message<String> message) {
        var completableFuture = new CompletableFuture<Void>();
        var messageWithAckAndNack = message.withAck(() -> {
            completableFuture.complete(null);
            return completableFuture;
        }).withNack(throwable -> {
            completableFuture.completeExceptionally(throwable);
            return completableFuture;
        });

        emitter.send(messageWithAckAndNack);
        return completableFuture;
    }
}
