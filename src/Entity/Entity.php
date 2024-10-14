<?php

declare(strict_types=1);

namespace Tarantool\Temporal\Entity;

use Tarantool\Mapper\Space;

class Entity
{
    public function __construct(
        public int $id,
        public string $name,
    ) {
    }

    public static function getSpaceName()
    {
        return '_temporal_entity';
    }

    public static function initSchema(Space $space)
    {
        $space->addIndex(['name']);
    }
}
