<?php

declare(strict_types=1);

namespace Tarantool\Temporal\Entity;

use Tarantool\Mapper\Space;

class Reference
{
    public function __construct(
        public int $idle,
        public int $entity,
        public int $id,
        public int $begin,
        public int $end,
        public int $target,
        public int $targetId,
        public int $timestamp,
        public int $actor,
    ) {
    }

    public static function getSpaceName()
    {
        return '_temporal_reference';
    }

    public static function initSchema(Space $space)
    {
        $space->addIndex(['entity', 'id', 'target', 'begin', 'timestamp', 'targetId', 'actor']);
    }
}
