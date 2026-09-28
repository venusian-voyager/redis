<?php

namespace Voyager\Redis\Sockets;

/**
 * Parses RESP2 replies as their bytes arrive, whole replies only. Simple strings and bulk strings
 * come back as strings, integers as ints, nil as null, arrays as lists, errors as ReplyError.
 */
final class RespReader
{
    private string $buffer = '';

    public function feed(string $bytes): void
    {
        $this->buffer .= $bytes;
    }

    /**
     * Takes the oldest whole reply off the buffer.
     *
     * @param string|int|array|ReplyError|null $reply set when a whole reply was there
     * @return bool false until a whole reply has arrived
     */
    public function take(mixed &$reply): bool
    {
        $offset = 0;
        $parsed = $this->parse($offset);

        if (is_null($parsed)) {
            return false;
        }

        $this->buffer = substr($this->buffer, $offset);
        $reply = $parsed[0];

        return true;
    }

    /**
     * @return array{0: string|int|array|ReplyError|null}|null the reply at $offset, or null until it is whole
     */
    private function parse(int &$offset): ?array
    {
        $end = strpos($this->buffer, "\r\n", $offset);

        if ($end === false) {
            return null;
        }

        $type = $this->buffer[$offset];
        $line = substr($this->buffer, $offset + 1, $end - $offset - 1);
        $next = $end + 2;

        switch ($type) {
            case '+':
                $offset = $next;
                return [$line];

            case '-':
                $offset = $next;
                return [new ReplyError($line)];

            case ':':
                $offset = $next;
                return [(int) $line];

            case '$':
                $length = (int) $line;

                if ($length === -1) {
                    $offset = $next;
                    return [null];
                }

                if (strlen($this->buffer) < $next + $length + 2) {
                    return null;
                }

                $offset = $next + $length + 2;
                return [substr($this->buffer, $next, $length)];

            case '*':
                $count = (int) $line;

                if ($count === -1) {
                    $offset = $next;
                    return [null];
                }

                $items = [];
                $cursor = $next;

                for ($i = 0; $i < $count; $i++) {
                    $item = $this->parse($cursor);

                    if (is_null($item)) {
                        return null;
                    }

                    $items[] = $item[0];
                }

                $offset = $cursor;
                return [$items];

            default:
                throw new RedisSocketException('Redis sent a reply that is not RESP2: '.json_encode(substr($this->buffer, $offset, 64), JSON_INVALID_UTF8_SUBSTITUTE));
        }
    }
}
