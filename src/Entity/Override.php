<?php

declare(strict_types=1);

namespace Tarantool\Temporal\Entity;

use Tarantool\Mapper\Space;

class Override
{
    public function __construct(
        public int $entity,
        public int $id,
        public int $begin,
        public int $end,
        public int $timestamp,
        public int $actor,
        public array $data,
        public int $idle = 0,
    ) {
    }

    public static function getSpaceName()
    {
        return '_temporal_override';
    }

    public static function initSchema(Space $space)
    {
        $space->addIndex(['entity', 'id', 'begin', 'timestamp', 'actor']);
    }
}
