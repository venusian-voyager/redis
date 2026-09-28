<?php

namespace Voyager\Redis\Lists;

use Closure;
use InvalidArgumentException;
use Voyager\Contracts\IOPools\Loop;
use Voyager\Contracts\IOPools\Pumpable;
use Voyager\IOPools\Resources\WakeSource;
use Voyager\IOPools\Waiter\Wakes\Readable;
use Voyager\IOPools\Waiter\Wakes\Writable;
use Voyager\Redis\RedisMessage;
use Voyager\Redis\Sockets\RedisEndpoint;
use Voyager\Redis\Sockets\RedisSocket;
use Voyager\Redis\Sockets\RedisSocketException;
use Voyager\Redis\Sockets\ReplyError;

/**
 * Pops one list as the loop's mail. A BLPOP is always waiting on this resource's own socket, and the
 * socket is in the loop's wait, so the loop wakes the moment a value arrives. Each pop pipelines an
 * LPOP for up to $batch more behind it, then the next BLPOP. pump() hands every popped value over
 * as a RedisMessage, raw bytes as Redis stored them.
 *
 * A popped value is gone from Redis, so the socket is never left with a BLPOP nobody will read:
 * release() hands it back when the loop stops, when the process exits, or when you call it after
 * forgetting the resource. The next wakes() connects and arms again.
 */
final class ListPop extends WakeSource implements Pumpable
{
    private ?RedisSocket $socket = null;

    private ?int $client_id = null;

    private bool $releasing = false;

    /** @var list<RedisMessage> popped, not yet pumped */
    private array $received = [];

    public function __construct(
        private readonly RedisEndpoint $endpoint,
        private readonly string $key,
        private readonly Loop $loop,
        private readonly int $batch = 64,
    ) {
        if ($batch < 1) {
            throw new InvalidArgumentException("A list pop takes at least one value per LPOP, {$batch} given.");
        }

        $this->loop->onStop($this->release(...));
        register_shutdown_function($this->release(...));
    }

    public function wakes(): array
    {
        $socket = $this->armed();
        $wakes = [new Readable($socket->stream())];

        if ($socket->wantsWrite()) {
            $wakes[] = new Writable($socket->stream());
        }

        return $wakes;
    }

    public function woke(array $fired): void
    {
        if (is_null($this->socket)) {
            return;
        }

        $this->socket->flush();
        $this->socket->read();

        // The server dropped us: the next wakes() connects and arms again.
        if ($this->socket->closed()) {
            $this->socket->close();
            [$this->socket, $this->client_id] = [null, null];
        }
    }

    public function pump(): array
    {
        [$mail, $this->received] = [$this->received, []];

        return $mail;
    }

    /**
     * Stops popping and leaves nothing behind in the socket. The waiting BLPOP is unblocked from a
     * second connection, every reply already on its way is read, and whatever was popped but not
     * yet pumped goes back to the head of the list, in its order.
     */
    public function release(): void
    {
        if (! is_null($this->socket)) {
            $this->releasing = true;
            $this->unblock($this->socket);
            $this->socket->close();
            [$this->socket, $this->client_id, $this->releasing] = [null, null, false];
        }

        if ($this->received === []) {
            return;
        }

        $requeue = new RedisSocket($this->endpoint);

        try {
            $requeue->call(['LPUSH', $this->endpoint->prefix.$this->key, ...array_reverse(array_map(
                fn (RedisMessage $message): string => $message->raw,
                $this->received,
            ))]);
            $this->received = [];
        } finally {
            $requeue->close();
        }
    }

    private function armed(): RedisSocket
    {
        if (is_null($this->socket)) {
            $this->socket = new RedisSocket($this->endpoint);
            $this->socket->send(['CLIENT', 'ID'], function (mixed $reply): void {
                $this->client_id = is_int($reply) ? $reply : null;
            });
            $this->arm($this->socket);
        }

        return $this->socket;
    }

    private function arm(RedisSocket $socket): void
    {
        $socket->send(['BLPOP', $this->endpoint->prefix.$this->key, '0'], function (mixed $reply) use ($socket): void {
            $this->take('BLPOP', $reply, fn (array $popped): array => [$popped[1]]);

            if ($this->releasing) {
                return;
            }

            // Whatever queued up behind it comes in one LPOP; the BLPOP after it waits for more.
            $socket->send(['LPOP', $this->endpoint->prefix.$this->key, (string) $this->batch], function (mixed $reply): void {
                $this->take('LPOP', $reply, fn (array $popped): array => $popped);
            });

            $this->arm($socket);
        });
    }

    /**
     * @param Closure(array): list<string> $values the popped values in a non-nil reply
     */
    private function take(string $command, mixed $reply, Closure $values): void
    {
        if ($reply instanceof ReplyError) {
            throw new RedisSocketException("{$command} {$this->key} failed: {$reply->message}");
        }

        foreach (is_array($reply) ? $values($reply) : [] as $raw) {
            $this->received[] = new RedisMessage($this->key, $raw);
        }
    }

    /**
     * CLIENT UNBLOCK only reaches a BLPOP Redis has started waiting on. One still on its way gets
     * a moment to arrive or be answered, then the unblock goes again.
     */
    private function unblock(RedisSocket $socket): void
    {
        $socket->settle(fn (): bool => ! is_null($this->client_id), $this->endpoint->timeout);

        if (is_null($this->client_id)) {
            return;
        }

        $control = new RedisSocket($this->endpoint);

        try {
            while ($socket->waiting() > 0 && ! $socket->closed()) {
                $unblocked = $control->call(['CLIENT', 'UNBLOCK', (string) $this->client_id]);
                $socket->settle(fn (): bool => $socket->waiting() === 0, $unblocked === 1 ? $this->endpoint->timeout : 0.01);
            }
        } finally {
            $control->close();
        }
    }
}
