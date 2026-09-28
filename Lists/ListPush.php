<?php

namespace Voyager\Redis\Lists;

use Voyager\Contracts\IOPools\Loop;
use Voyager\Contracts\IOPools\Promise;
use Voyager\IOPools\Resources\WakeSource;
use Voyager\IOPools\Waiter\Wakes\Readable;
use Voyager\IOPools\Waiter\Wakes\Writable;
use Voyager\Redis\Sockets\RedisEndpoint;
use Voyager\Redis\Sockets\RedisSocket;
use Voyager\Redis\Sockets\RedisSocketException;
use Voyager\Redis\Sockets\ReplyError;

/**
 * RPUSH onto one list without blocking: push() queues the command on this resource's own socket and
 * returns a promise for the list's new length. The resource is on the loop only while a push waits
 * for its reply, the way a pool registers a worker only while it holds a gig, so an idle pusher
 * never keeps run() alive. Values go as raw bytes: the connection's serializer and compression
 * options don't apply.
 */
final class ListPush extends WakeSource
{
    private ?RedisSocket $socket = null;

    /** @var list<Promise> pushes waiting for their reply, oldest first */
    private array $pending = [];

    private readonly string $name;

    public function __construct(
        private readonly RedisEndpoint $endpoint,
        private readonly string $key,
        private readonly Loop $loop,
    ) {
        $this->name = 'redis.list-push.'.spl_object_id($this);
    }

    /**
     * @return Promise the list's length once Redis has taken the values
     * @throws RedisSocketException the socket could not connect
     */
    public function push(string $value, string ...$values): Promise
    {
        $this->socket ??= new RedisSocket($this->endpoint);
        $promise = $this->loop->promise();

        if ($this->pending === []) {
            $this->loop->resource($this->name, $this);
        }

        $this->pending[] = $promise;

        $this->socket->send(['RPUSH', $this->endpoint->prefix.$this->key, $value, ...$values], function (mixed $reply) use ($promise): void {
            array_shift($this->pending);

            $reply instanceof ReplyError
                ? $promise->reject(new RedisSocketException("RPUSH {$this->key} failed: {$reply->message}"))
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

            foreach ($lost as $promise) {
                $promise->reject(new RedisSocketException(
                    "Redis closed the connection before it acknowledged RPUSH {$this->key}: the value may or may not be in the list."
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
