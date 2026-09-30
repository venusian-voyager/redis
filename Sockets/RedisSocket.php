<?php

namespace Voyager\Redis\Sockets;

use Closure;

/**
 * One connection to Redis that the loop can wait on. Commands queue their bytes and a callback;
 * flush() writes what the socket takes without blocking, read() takes what has arrived and hands
 * each reply to the command that asked, oldest first. The TCP (or TLS) connect blocks, the way any
 * client's connect does; the login rides ahead of the first command, so it never waits on a reply.
 */
final class RedisSocket
{
    /** @var resource */
    private $stream;

    private string $outbound = '';

    /** @var list<Closure(string|int|array|ReplyError|null): void> */
    private array $waiting = [];

    private readonly RespReader $reader;

    private bool $closed = false;

    public function __construct(private readonly RedisEndpoint $endpoint)
    {
        $stream = @stream_socket_client(
            $endpoint->address, $errno, $errstr, $endpoint->timeout,
            STREAM_CLIENT_CONNECT, stream_context_create($endpoint->context),
        );

        if ($stream === false) {
            throw new RedisSocketException("Could not connect to Redis at {$endpoint->address}: {$errstr}");
        }

        stream_set_blocking($stream, false);
        $this->stream = $stream;
        $this->reader = new RespReader();

        if (! is_null($endpoint->password)) {
            $this->send(
                is_null($endpoint->username) ? ['AUTH', $endpoint->password] : ['AUTH', $endpoint->username, $endpoint->password],
                self::expectOk('AUTH'),
            );
        }

        if ($endpoint->database !== 0) {
            $this->send(['SELECT', (string) $endpoint->database], self::expectOk('SELECT'));
        }
    }

    /** @return resource */
    public function stream(): mixed
    {
        return $this->stream;
    }

    /**
     * @param list<string> $arguments the command and its arguments
     * @param Closure(string|int|array|ReplyError|null): void $on_reply
     */
    public function send(array $arguments, Closure $on_reply): void
    {
        $this->outbound .= '*'.count($arguments)."\r\n";

        foreach ($arguments as $argument) {
            $this->outbound .= '$'.strlen($argument)."\r\n{$argument}\r\n";
        }

        $this->waiting[] = $on_reply;
        $this->flush();
    }

    public function wantsWrite(): bool
    {
        return ! $this->closed && $this->outbound !== '';
    }

    /** How many commands are still waiting on their reply. */
    public function waiting(): int
    {
        return count($this->waiting);
    }

    public function closed(): bool
    {
        return $this->closed;
    }

    /** Writes as much of the queued bytes as the socket takes right now. */
    public function flush(): void
    {
        if ($this->closed || $this->outbound === '') {
            return;
        }

        $written = @fwrite($this->stream, $this->outbound);

        if ($written === false) {
            $this->closed = true;
            return;
        }

        $this->outbound = substr($this->outbound, $written);
    }

    /** Reads everything that has arrived and hands each whole reply to its command, oldest first. */
    public function read(): void
    {
        // Drained to empty: a TLS stream holds decrypted bytes the kernel no longer reports as readable.
        while (! $this->closed) {
            $chunk = @fread($this->stream, 65536);

            if ($chunk === false || $chunk === '') {
                if (feof($this->stream)) {
                    $this->closed = true;
                }

                break;
            }

            $this->reader->feed($chunk);
        }

        $this->dispatch();
    }

    /**
     * Blocks until $done() holds, the socket closes, or $timeout seconds pass. For the moments no
     * loop turns: a consumer handing its socket back.
     *
     * @return bool whether $done() holds
     */
    public function settle(Closure $done, float $timeout): bool
    {
        $deadline = hrtime(true) + (int) ($timeout * 1e9);

        while (! $done() && ! $this->closed) {
            $left = $deadline - hrtime(true);

            if ($left <= 0) {
                return false;
            }

            $this->flush();
            $read = [$this->stream];
            $write = $this->wantsWrite() ? [$this->stream] : null;
            $except = null;

            if (@stream_select($read, $write, $except, intdiv($left, 1_000_000_000), intdiv($left % 1_000_000_000, 1_000)) > 0) {
                $this->read();
            }
        }

        return $done();
    }

    /**
     * One command, answered before this returns.
     *
     * @param list<string> $arguments
     * @throws RedisSocketException Redis refused it, or didn't answer within the endpoint's timeout
     */
    public function call(array $arguments): string|int|array|null
    {
        $answered = false;
        $reply = null;

        $this->send($arguments, function (mixed $answer) use (&$answered, &$reply): void {
            [$answered, $reply] = [true, $answer];
        });

        if (! $this->settle(function () use (&$answered): bool { return $answered; }, $this->endpoint->timeout)) {
            throw new RedisSocketException("Redis did not answer {$arguments[0]} within {$this->endpoint->timeout}s.");
        }

        if ($reply instanceof ReplyError) {
            throw new RedisSocketException("{$arguments[0]} failed: {$reply->message}");
        }

        return $reply;
    }

    public function close(): void
    {
        $this->closed = true;

        if (is_resource($this->stream)) {
            fclose($this->stream);
        }
    }

    private function dispatch(): void
    {
        while ($this->waiting !== [] && $this->reader->take($reply)) {
            $on_reply = array_shift($this->waiting);
            $on_reply($reply);
        }
    }

    /** @return Closure(string|int|array|ReplyError|null): void */
    private static function expectOk(string $command): Closure
    {
        return function (mixed $reply) use ($command): void {
            if ($reply instanceof ReplyError) {
                throw new RedisSocketException("{$command} failed: {$reply->message}");
            }
        };
    }
}
