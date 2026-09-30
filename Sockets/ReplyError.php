<?php

namespace Voyager\Redis\Sockets;

/** A "-ERR …" reply: Redis refused the command. */
final readonly class ReplyError
{
    public function __construct(public string $message) {}
}
