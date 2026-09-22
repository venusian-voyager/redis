<?php

namespace Voyager\Redis;

use Throwable;
use Voyager\Contracts\IOPools\Event;
use Voyager\Contracts\IOPools\Pumpable;
use Voyager\Contracts\IOPools\Tickable;
use Voyager\Redis\Connections\Connection;
use Voyager\Redis\Connections\PredisConnection;

/**
 * A Redis list as a loop resource: post() pushes, the loop pops, pump() hands the pops over as mail.
 * The shape depends on the client. phpredis hides its socket, so it polls LPOP each tick.
 * Predis exposes it, so BLPOP's socket joins the select and the loop wakes on the message.
 */
abstract class RedisQueueResource implements Tickable, Pumpable
{
    /** @var Event[] */
    protected array $received = [];

    final public function __construct(
        protected readonly Connection $connection,
        protected readonly string $key,
        protected readonly int $batch = 64,
    ) {}

    public static function make(Connection $connection, string $key, int $batch = 64): static
    {
        return $connection instanceof PredisConnection
            ? new WatchedRedisQueueResource($connection, $key, $batch)
            : new PollingRedisQueueResource($connection, $key, $batch);
    }

    public function post(Event $event, ?string $key = null): void
    {
        $this->connection->rpush($key ?? $this->key, json_encode(['class' => $event::class, 'data' => $event->toData()]));
    }

    public function pump(): array
    {
        [$mail, $this->received] = [$this->received, []];

        return $mail;
    }

    protected function reconstitute(string $raw): Event
    {
        $decoded = json_decode($raw, true);
        $class = is_array($decoded) ? ($decoded['class'] ?? null) : null;

        if (is_string($class) && is_subclass_of($class, Event::class))
        {
            try { return $class::fromData($decoded['data'] ?? []); } catch (Throwable) { /* fall through: it's foreign after all */ }
        }

        return new RedisMessage($this->key, $raw);
    }
}
