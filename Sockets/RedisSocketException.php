<?php

namespace Voyager\Redis\Sockets;

use RuntimeException;

/** Redis refused a command sent over a RedisSocket, or the socket itself failed. */
class RedisSocketException extends RuntimeException
{
}
