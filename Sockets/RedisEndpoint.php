<?php

namespace Voyager\Redis\Sockets;

/**
 * Where a RedisSocket connects and how it logs in, read from the same connection config the
 * phpredis and Predis connectors use, prefix included, so a socket and a Connection on the same
 * name agree on every key.
 */
final readonly class RedisEndpoint
{
    /**
     * @param array<string, array<string, mixed>> $context stream context options
     */
    public function __construct(
        public string $address,
        public ?string $username,
        public ?string $password,
        public int $database,
        public float $timeout,
        public array $context,
        public string $prefix,
    ) {}

    /**
     * @param array<string, mixed> $config one parsed connection config
     */
    public static function fromConfig(array $config, string $prefix): self
    {
        $host = (string) ($config['host'] ?? '127.0.0.1');
        $port = (int) ($config['port'] ?? 6379);
        $scheme = (string) ($config['scheme'] ?? 'tcp');

        $address = match (true) {
            str_starts_with($host, 'unix://') => $host,
            str_starts_with($host, '/') => 'unix://'.$host,
            str_contains($host, '://') => "{$host}:{$port}",
            default => "{$scheme}://{$host}:{$port}",
        };

        $filled = fn (mixed $value): ?string => is_string($value) && $value !== '' ? $value : null;

        // phpredis takes TLS options under "stream"; a PHP stream context wants them under "ssl".
        $tls = $config['context']['stream'] ?? $config['context']['ssl'] ?? [];

        return new self(
            $address,
            $filled($config['username'] ?? null),
            $filled($config['password'] ?? null),
            (int) ($config['database'] ?? 0),
            ((float) ($config['timeout'] ?? 0)) ?: (float) ini_get('default_socket_timeout'),
            $tls === [] ? [] : ['ssl' => $tls],
            $prefix,
        );
    }
}
