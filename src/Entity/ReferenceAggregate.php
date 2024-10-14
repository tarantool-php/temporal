<?php

declare(strict_types=1);

namespace Tarantool\Temporal\Entity;

use Tarantool\Mapper\Space;

class ReferenceAggregate
{
    public function __construct(
        public int $entity,
        public int $id,
        public int $source,
        public int $begin,
        public int $end,
        public array $data,
    ) {
    }

    public static function getSpaceName()
    {
        return '_temporal_reference_aggregate';
    }

    public static function initSchema(Space $space)
    {
        $space->addIndex(['entity', 'id', 'source', 'begin']);
    }
}
