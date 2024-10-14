<?php

declare(strict_types=1);

namespace Tarantool\Temporal\Entity;

use Tarantool\Mapper\Space;

class ReferenceState
{
    public function __construct(
        public int $entity,
        public int $id,
        public int $target,
        public int $begin,
        public int $end,
        public int $targetId,
    ) {
    }

    public static function getSpaceName()
    {
        return '_temporal_reference_state';
    }

    public static function initSchema(Space $space)
    {
        $space->addIndex(['entity', 'id', 'target', 'begin']);
        $space->addIndex(['target', 'targetId', 'entity', 'begin', 'id']);
    }
}
