<?php

namespace Voyager\Redis;

use Closure;
use Voyager\Contracts\Redis\Factory;
use Voyager\Contracts\IOPools\Loop;
use Voyager\Redis\Lists\ListPop;
use Voyager\Redis\Lists\ListPush;
use Voyager\Redis\Sockets\RedisEndpoint;
use Voyager\Redis\Sockets\RedisPipe;
use Voyager\Redis\Connections\Connection;
use Voyager\Redis\Connectors\PhpRedisConnector;
use Voyager\Redis\Connectors\PredisConnector;
use Voyager\NutsAndBolts\DataObjects\Arr;
use Voyager\NutsAndBolts\ConfigurationUrlParser;
use InvalidArgumentException;

use function Voyager\NutsAndBolts\Helpers\enum_value;

/**
 * @mixin \Voyager\Redis\Connections\Connection
 */
class RedisManager implements Factory
{
    /**
     * The application instance.
     *
     * @var \Voyager\Contracts\System\Application
     */
    protected $app;

    /**
     * The name of the default driver.
     *
     * @var string
     */
    protected $driver;

    /**
     * The registered custom driver creators.
     *
     * @var array
     */
    protected $customCreators = [];

    /**
     * The Redis server configurations.
     *
     * @var array
     */
    protected $config;

    /**
     * The Redis connections.
     *
     * @var mixed
     */
    protected $connections;

    /**
     * Indicates whether event dispatcher is set on connections.
     *
     * @var bool
     */
    protected $events = false;

    /**
     * Create a new Redis manager instance.
     *
     * @param  \Voyager\Contracts\System\Application  $app
     * @param  string  $driver
     * @param  array  $config
     */
    public function __construct($app, $driver, array $config)
    {
        $this->app = $app;
        $this->driver = $driver;
        $this->config = $config;
    }

    /**
     * Get a Redis connection by name.
     *
     * @param  \UnitEnum|string|null  $name
     * @return \Voyager\Redis\Connections\Connection
     */
    public function connection($name = null)
    {
        $name = enum_value($name) ?: 'default';

        if (isset($this->connections[$name])) {
            return $this->connections[$name];
        }

        return $this->connections[$name] = $this->configure(
            $this->resolve($name), $name
        );
    }

    /**
     * Resolve the given connection by name.
     *
     * @param  string|null  $name
     * @return \Voyager\Redis\Connections\Connection
     *
     * @throws \InvalidArgumentException
     */
    public function resolve($name = null)
    {
        $name = $name ?: 'default';

        $options = $this->config['options'] ?? [];

        if (isset($this->config[$name])) {
            return $this->connector()->connect(
                $this->parseConnectionConfiguration($this->config[$name]),
                array_merge(Arr::except($options, 'parameters'), ['parameters' => Arr::get($options, 'parameters.'.$name, Arr::get($options, 'parameters', []))])
            );
        }

        if (isset($this->config['clusters'][$name])) {
            return $this->resolveCluster($name);
        }

        throw new InvalidArgumentException("Redis connection [{$name}] not configured.");
    }

    /**
     * RPUSH onto one list without blocking the loop, through its own pipe.
     *
     * @param  string  $key
     * @param  \UnitEnum|string|null  $connection
     * @return ListPush
     */
    public function listPush(string $key, \UnitEnum|string|null $connection = null): ListPush
    {
        return new ListPush($this->pipe($connection), $key);
    }

    /**
     * Commands on a socket of their own that don't block the loop, each answered by a promise.
     *
     * @param  \UnitEnum|string|null  $connection
     * @return RedisPipe
     */
    public function pipe(\UnitEnum|string|null $connection = null): RedisPipe
    {
        return new RedisPipe($this->endpoint($connection), $this->app->make(Loop::class));
    }

    /**
     * A resource that pops one list as loop mail. Register it: $loop->resource($name, $pop).
     *
     * @param  string  $key
     * @param  \UnitEnum|string|null  $connection
     * @param  int  $batch  how many more values each pop takes from behind the first
     * @return ListPop
     */
    public function listPop(string $key, \UnitEnum|string|null $connection = null, int $batch = 64): ListPop
    {
        return new ListPop($this->endpoint($connection), $key, $this->app->make(Loop::class), $batch);
    }

    /**
     * Where a list resource's own socket connects: the named connection's config and key prefix,
     * the same prefix the connector gives that connection.
     *
     * @param  \UnitEnum|string|null  $name
     * @return RedisEndpoint
     *
     * @throws \InvalidArgumentException
     */
    protected function endpoint(\UnitEnum|string|null $name): RedisEndpoint
    {
        $name = enum_value($name) ?: 'default';

        if (isset($this->config['clusters'][$name])) {
            throw new InvalidArgumentException("Redis list resources need a single-node connection, and [{$name}] is a cluster.");
        }

        if (! isset($this->config[$name])) {
            throw new InvalidArgumentException("Redis connection [{$name}] not configured.");
        }

        $config = $this->parseConnectionConfiguration($this->config[$name]);

        return RedisEndpoint::fromConfig(
            $config,
            (string) ($config['prefix'] ?? $config['options']['prefix'] ?? $this->config['options']['prefix'] ?? ''),
        );
    }

    /**
     * Resolve the given cluster connection by name.
     *
     * @param  string  $name
     * @return \Voyager\Redis\Connections\Connection
     */
    protected function resolveCluster($name)
    {
        return $this->connector()->connectToCluster(
            array_map(function ($config) {
                return $this->parseConnectionConfiguration($config);
            }, $this->config['clusters'][$name]),
            $this->config['clusters']['options'] ?? [],
            $this->config['options'] ?? []
        );
    }

    /**
     * Configure the given connection to prepare it for commands.
     *
     * @param  \Voyager\Redis\Connections\Connection  $connection
     * @param  string  $name
     * @return \Voyager\Redis\Connections\Connection
     */
    protected function configure(Connection $connection, $name)
    {
        $connection->setName($name);

        if ($this->events && $this->app->isBound('signals')) {
            $connection->setEventDispatcher($this->app->make('signals'));
        }

        return $connection;
    }

    /**
     * Get the connector instance for the current driver.
     *
     * @return \Voyager\Contracts\Redis\Connector|null
     */
    protected function connector()
    {
        $customCreator = $this->customCreators[$this->driver] ?? null;

        if ($customCreator) {
            return $customCreator();
        }

        return match ($this->driver) {
            'predis' => new PredisConnector,
            'phpredis' => new PhpRedisConnector,
            default => null,
        };
    }

    /**
     * Parse the Redis connection configuration.
     *
     * @param  mixed  $config
     * @return array
     */
    protected function parseConnectionConfiguration($config)
    {
        $parsed = (new ConfigurationUrlParser)->parseConfiguration($config);

        $driver = strtolower($parsed['driver'] ?? '');

        if (in_array($driver, ['tcp', 'tls'])) {
            $parsed['scheme'] = $driver;
        }

        return array_filter($parsed, function ($key) {
            return $key !== 'driver';
        }, ARRAY_FILTER_USE_KEY);
    }

    /**
     * Return all of the created connections.
     *
     * @return array
     */
    public function connections()
    {
        return $this->connections;
    }

    /**
     * Enable the firing of Redis command events.
     *
     * @return void
     */
    public function enableEvents()
    {
        $this->events = true;
    }

    /**
     * Disable the firing of Redis command events.
     *
     * @return void
     */
    public function disableEvents()
    {
        $this->events = false;
    }

    /**
     * Set the default driver.
     *
     * @param  string  $driver
     * @return void
     */
    public function setDriver($driver)
    {
        $this->driver = $driver;
    }

    /**
     * Disconnect the given connection and remove from local cache.
     *
     * @param  string|null  $name
     * @return void
     */
    public function purge($name = null)
    {
        $name = $name ?: 'default';

        unset($this->connections[$name]);
    }

    /**
     * Register a custom driver creator Closure.
     *
     * @param  string  $driver
     * @param  \Closure  $callback
     *
     * @param-closure-this  $this  $callback
     *
     * @return $this
     */
    public function extend($driver, Closure $callback)
    {
        $this->customCreators[$driver] = $callback->bindTo($this, $this);

        return $this;
    }

    /**
     * Pass methods onto the default Redis connection.
     *
     * @param  string  $method
     * @param  array  $parameters
     * @return mixed
     */
    public function __call($method, $parameters)
    {
        return $this->connection()->{$method}(...$parameters);
    }
}
