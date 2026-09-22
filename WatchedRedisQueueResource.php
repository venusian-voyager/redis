<?php

namespace Voyager\Redis;

use Predis\Command\CommandInterface;
use Predis\Connection\NodeConnectionInterface;
use Voyager\Contracts\IOPools\StreamWatchable;

/**
 * Predis: a BLPOP in flight, its socket in the loop's select. tick() runs only when a reply is
 * readable, reads it, and re-arms. Nothing is sent while idle.
 */
final class WatchedRedisQueueResource extends RedisQueueResource implements StreamWatchable
{
    private ?CommandInterface $in_flight = null;

    /** @var resource|null */
    private $socket = null;

    public function streams(): array
    {
        $this->arm();

        return [$this->socket];
    }

    public function tick(): void
    {
        if (is_null($this->in_flight)) {
            return;                                  // a stray wake before the first arm
        }

        $reply = $this->node()->readResponse($this->in_flight);
        $this->in_flight = null;

        // BLPOP answers [key, value]; null on a timeout we never asked for
        if (is_array($reply) && isset($reply[1])) {
            $this->received[] = $this->reconstitute($reply[1]);
        }

        // drain what queued up behind it without another round trip through the select
        for ($i = 1; $i < $this->batch; $i++)
        {
            $raw = $this->connection->client()->lpop($this->key);
            if (! is_string($raw)) break;
            $this->received[] = $this->reconstitute($raw);
        }

        $this->arm();
    }

    private function arm(): void
    {
        if (! is_null($this->in_flight)) {
            return;
        }

        $node = $this->node();
        $node->connect();

        $this->in_flight = $this->connection->client()->createCommand('blpop', [$this->key, 0]);
        $node->writeRequest($this->in_flight);

        $this->socket ??= $this->rawSocket($node);
    }

    private function node(): NodeConnectionInterface
    {
        return $this->connection->client()->getConnection();
    }

    /**
     * Predis 2 hands the resource out; Predis 3 wraps it and only offers detach(), which would
     * strip the wrapper the reader still needs. Reach past it.
     * @return resource
     */
    private function rawSocket(NodeConnectionInterface $node)
    {
        $resource = $node->getResource();

        return is_resource($resource)
            ? $resource
            : (fn () => $this->stream)->call($resource);
    }
}
