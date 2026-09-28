<?php

namespace Voyager\Redis\Sockets;

use Voyager\Contracts\IOPools\Loop;
use Voyager\Contracts\IOPools\Promise;
use Voyager\IOPools\Resources\WakeSource;
use Voyager\IOPools\Waiter\Wakes\Readable;
use Voyager\IOPools\Waiter\Wakes\Writable;

/**
 * Commands on a socket of its own, without blocking: send() queues one and returns a promise for
 * its reply. Replies come back in the order the commands went out. The pipe is on the loop only
 * while a reply is due, the way a pool registers a worker only while it holds a gig, so an idle
 * pipe never keeps run() alive.
 */
final class RedisPipe extends WakeSource
{
    private ?RedisSocket $socket = null;

    /** @var list<array{promise: Promise, describe: string}> commands waiting for their reply, oldest first */
    private array $pending = [];

    private readonly string $name;

    public function __construct(
        private readonly RedisEndpoint $endpoint,
        private readonly Loop $loop,
    ) {
        $this->name = 'redis.pipe.'.spl_object_id($this);
    }

    /** The connection's key prefix: every key sent through the pipe starts with it. */
    public function prefix(): string
    {
        return $this->endpoint->prefix;
    }

    /**
     * @param list<string> $arguments the command and its arguments, keys already prefixed
     * @param string $describe how a failure names the command, e.g. "RPUSH jobs"
     * @return Promise the reply: string, int, list or null
     * @throws RedisSocketException the socket could not connect
     */
    public function send(array $arguments, string $describe): Promise
    {
        $this->socket ??= new RedisSocket($this->endpoint);
        $promise = $this->loop->promise();

        if ($this->pending === []) {
            $this->loop->resource($this->name, $this);
        }

        $this->pending[] = ['promise' => $promise, 'describe' => $describe];

        $this->socket->send($arguments, function (mixed $reply) use ($promise, $describe): void {
            array_shift($this->pending);

            $reply instanceof ReplyError
                ? $promise->reject(new RedisSocketException("{$describe} failed: {$reply->message}"))
                : $promise->resolve($reply);
        });

        return $promise;
    }

    public function wakes(): array
    {
        if (is_null($this->socket) || $this->socket->closed()) {
            return [];
        }

        $wakes = [new Readable($this->socket->stream())];

        if ($this->socket->wantsWrite()) {
            $wakes[] = new Writable($this->socket->stream());
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

        if ($this->socket->closed()) {
            [$lost, $this->pending] = [$this->pending, []];

            foreach ($lost as ['promise' => $promise, 'describe' => $describe]) {
                $promise->reject(new RedisSocketException(
                    "Redis closed the connection before it answered {$describe}: it may or may not have run."
                ));
            }

            $this->socket->close();
            $this->socket = null;
        }

        if ($this->pending === []) {
            $this->loop->forget($this->name);
        }
    }
}
