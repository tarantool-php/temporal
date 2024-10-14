<?php

declare(strict_types=1);

namespace Tarantool\Temporal\Entity;

use Tarantool\Mapper\Space;

class Link
{
    public function __construct(
        public int $id,
        public int $parent,
        public int $entity,
        public int $entityId,
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
        return '_temporal_link';
    }

    public static function initSchema(Space $space)
    {
        $space->addIndex(['entity', 'entityId', 'parent', 'begin', 'timestamp', 'actor']);
        $space->addIndex(['parent'], ['unique' => false]);
    }
}
