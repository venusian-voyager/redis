<?php

namespace Voyager\Redis;

/** phpredis: one LPOP per message, up to $batch per tick. Idle cost: one round trip per turn. */
final class PollingRedisQueueResource extends RedisQueueResource
{
    public function tick(): void
    {
        for ($i = 0; $i < $this->batch; $i++)
        {
            $raw = $this->connection->lpop($this->key);

            if (! is_string($raw)) {
                return;
            }

            $this->received[] = $this->reconstitute($raw);
        }
    }
}
