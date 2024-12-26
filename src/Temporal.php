<?php

declare(strict_types=1);

namespace Tarantool\Temporal;

use Carbon\Carbon;
use Exception;
use ReflectionClass;
use Tarantool\Client\Schema\Criteria;
use Tarantool\Mapper\Api;
use Tarantool\Mapper\Mapper;
use Tarantool\Mapper\Space;
use Tarantool\Temporal\Entity\Entity;
use Tarantool\Temporal\Entity\Link;
use Tarantool\Temporal\Entity\LinkAggregate;
use Tarantool\Temporal\Entity\Override;
use Tarantool\Temporal\Entity\OverrideAggregate;
use Tarantool\Temporal\Entity\Reference;
use Tarantool\Temporal\Entity\ReferenceAggregate;
use Tarantool\Temporal\Entity\ReferenceState;

class Temporal
{
    use Api;

    private int $actor;
    private array $timestamps = [];

    public readonly Aggregator $aggregator;

    public function __construct(
        public readonly Mapper $mapper,
    ) {
        $this->aggregator = new Aggregator($mapper);
        $mapper->registerClass(Entity::class);
        $mapper->registerClass(Link::class);
        $mapper->registerClass(LinkAggregate::class);
        $mapper->registerClass(Override::class);
        $mapper->registerClass(Override::class);
        $mapper->registerClass(OverrideAggregate::class);
        $mapper->registerClass(Reference::class);
        $mapper->registerClass(ReferenceAggregate::class);
        $mapper->registerClass(ReferenceState::class);
    }

    public function getEntityName(int $id): string
    {
        return $this->get(Entity::class, $id)->name;
    }

    public function getEntityId(string $name): int
    {
        return $this->findOrCreate(Entity::class, ['name' => $name])->id;
    }

    public function getSpace(object|int|string $id): Space
    {
        return $this->mapper->getSpace($id);
    }

    public function hasSpace(string $class): bool
    {
        return $this->mapper->hasSpace($class);
    }

    public function getReference(int|string $entity, int $id, int|string $target, int|string $date): ?int
    {
        if (!$this->hasSpace(ReferenceState::class)) {
            return null;
        }

        $entity = $this->getEntityId($entity);
        $target = $this->getEntityId($target);
        $date = $this->getTimestamp($date);

        $state = $this->findOne(
            ReferenceState::class,
            Criteria::key([$entity, $id, $target, $date])->andLimit(1)->andLeIterator()
        );

        if ($state instanceof ReferenceState) {
            if ($state->entity == $entity && $state->id == $id && $state->target == $target) {
                if (!$state->end || $state->end >= $date) {
                    return $state->targetId;
                }
            }
        }

        return null;
    }

    public function getReferenceLog(int|string $entity, int $id, string|int $target): array
    {
        if (!$this->hasSpace(Reference::class)) {
            return [];
        }

        return $this->find(Reference::class, [
            'entity' => $this->getEntityId($entity),
            'id' => $id,
            'target' => $this->getEntityId($target),
        ]);
    }

    public function getReferenceStates(
        int|string $entity,
        int $entityId,
        int|string $target,
        int $begin,
        int $end
    ): array {
        if (!$this->hasSpace(ReferenceState::class)) {
            return [];
        }

        $states = $this->find(ReferenceState::class, [
            'entity' => $this->getEntityId($entity),
            'id' => $entityId,
            'target' => $this->getEntityId($target),
        ]);

        $begin = $this->getTimestamp($begin);
        $end = $this->getTimestamp($end);

        $slices = [];
        foreach ($states as $state) {
            if ($state->begin < $end && ($begin < $state->end || !$state->end)) {
                $slices[] = [
                    'begin' => +date('Ymd', max($state->begin, $begin)),
                    'end' => +date('Ymd', min($state->end ?: $end, $end)),
                    'value' => $state->targetId,
                ];
            }
        }

        return $slices;
    }

    public function getReferences(int|string $target, int $id, int|string $source, int|string $date): array
    {
        if (!$this->hasSpace(ReferenceAggregate::class)) {
            return [];
        }

        $target = $this->getEntityId($target);
        $source = $this->getEntityId($source);
        $date = $this->getTimestamp($date);

        $state = $this->findOne(
            ReferenceAggregate::class,
            Criteria::key([$target, $id, $source, $date])->andLimit(1)->andLeIterator()
        );

        if ($state instanceof ReferenceAggregate) {
            if ($state->entity == $target && $state->id == $id && $state->source == $source) {
                if (!$state->end || $state->end > $date) {
                    return $state->data;
                }
            }
        }

        return [];
    }

    public function reference(array $reference): Reference
    {
        $reference = $this->parseConfig($reference);

        foreach ($reference as $k => $v) {
            if (!in_array($k, ['entity', 'id', 'begin', 'end', 'data'])) {
                $reference['entity'] = $k;
                $reference['id'] = $v;
                unset($reference[$k]);
            }
        }

        if (!array_key_exists('entity', $reference)) {
            throw new Exception("no entity defined");
        }

        if (count($reference['data']) != 1) {
            throw new Exception("Invalid reference configuration");
        }

        [$targetName] = array_keys($reference['data']);
        $reference['target'] = $this->getEntityId($targetName);
        $reference['targetId'] = $reference['data'][$targetName];

        // set entity id
        $entityName = $reference['entity'];
        $reference['entity'] = $this->getEntityId($entityName);
        $reference['actor'] = $this->actor;
        $reference['timestamp'] = Carbon::now()->timestamp;
        $reference['idle'] = 0;

        $reference = $this->mapper->create(Reference::class, $reference);

        $this->aggregator->updateReferenceState($reference->entity, $reference->id, $reference->target);

        return $reference;
    }

    public function getLinksLog($entity, $entityId, $filter = []): array
    {
        if (!$this->hasSpace(Link::class)) {
            return [];
        }

        $nodes = $this->find(Link::class, [
            'entity' => $this->getEntityId($entity),
            'entityId' => $entityId,
        ]);

        $links = [];

        foreach ($nodes as $node) {
            foreach ($this->aggregator->getLeafs($node) as $leaf) {
                $entityName = $this->getEntityName($leaf->entity);
                $link = [
                    $entityName => $leaf->entityId,
                    'id' => $leaf->id,
                    'begin' => $leaf->begin,
                    'end' => $leaf->end,
                    'timestamp' => $leaf->timestamp,
                    'actor' => $leaf->actor,
                    'idle' => $leaf->idle,
                ];

                $current = $leaf;
                while ($current->parent) {
                    $current = $this->get(Link::class, $current->parent);
                    $entityName = $this->getEntityName($current->entity);
                    $link[$entityName] = $current->entityId;
                }

                if (count($filter)) {
                    foreach ($filter as $required) {
                        if (!array_key_exists($required, $link)) {
                            continue 2;
                        }
                    }
                }
                $links[] = $link;
            }
        }

        return $links;
    }

    public function getLinks(int|string $entity, int $id, int|string $date): array
    {
        if (!$this->hasSpace(LinkAggregate::class)) {
            return [];
        }

        $links = $this->getData($entity, $id, $date, LinkAggregate::class);
        foreach ($links as $i => $source) {
            $link = array_key_exists(1, $source) ? ['data' => $source[1]] : [];
            foreach ($source[0] as $spaceId => $entityId) {
                $spaceName = $this->get(Entity::class, $spaceId)->name;
                $link[$spaceName] = $entityId;
            }
            $links[$i] = $link;
        }
        return $links;
    }

    public function getState(int|string $entity, int $id, int|string $date): array
    {
        if (!$this->hasSpace(OverrideAggregate::class)) {
            return [];
        }

        return $this->getData($entity, $id, $date, OverrideAggregate::class);
    }

    private function getData(int|string $entity, int $id, int|string $date, string $space): array
    {
        $entity = $this->getEntityId($entity);
        $date = $this->getTimestamp($date);

        $instance = $this->findOne($space, Criteria::key([$entity, $id, $date])->andLimit(1)->andLeIterator());
        if ($instance) {
            $tuple = $this->getSpace($space)->getTuple($instance);
            if ($tuple[0] == $entity && $tuple[1] == $id) {
                if (!$instance->end || $instance->end >= $date) {
                    return $instance->data;
                }
            }
        }

        return [];
    }

    public function getOverrides(string $entityName, int $id): array
    {
        if (!$this->hasSpace(Override::class)) {
            return [];
        }

        return $this->find(Override::class, [
            'entity' => $this->getEntityId($entityName),
            'id' => $id,
        ]);
    }

    public function override(array $override): void
    {
        $override = $this->parseConfig($override);

        foreach ($override as $k => $v) {
            if (!in_array($k, ['entity', 'id', 'begin', 'end', 'data'])) {
                $override['entity'] = $k;
                $override['id'] = $v;
                unset($override[$k]);
            }
        }

        if (!array_key_exists('entity', $override)) {
            throw new Exception("no entity defined");
        }

        // set entity id
        $entityName = $override['entity'];
        $override['entity'] = $this->getEntityId($entityName);
        $override['actor'] = $this->actor;
        $override['timestamp'] = Carbon::now()->timestamp;
        $override['idle'] = 0;

        $this->mapper->create(Override::class, $override);
        $this->aggregator->updateOverrideAggregation($override['entity'], $override['id']);
    }

    public function setLinkIdle(int $id, bool $flag): void
    {
        $link = $this->get(Link::class, $id);
        if ($link->idle > 0 == $flag) {
            return;
        }

        $this->mapper->update($link, ['idle' => $link->idle ? 0 : time()]);

        $this->aggregator->updateLinkAggregation($link);
    }

    public function setReferenceIdle(
        int|string $entity,
        int $id,
        int|string $target,
        int $targetId,
        int $begin,
        int $actor,
        int $timestamp,
        bool $flag,
    ) {
        $reference = $this->findOrFail(Reference::class, [
            'entity' => $this->getEntityId($entity),
            'id' => $id,
            'target' => $this->getEntityId($target),
            'targetId' => $targetId,
            'begin' => $begin,
            'actor' => $actor,
            'timestamp' => $timestamp,
        ]);

        if ($reference->idle > 0 == $flag) {
            return;
        }

        $this->mapper->update($reference, ['idle' => $reference->idle ? 0 : time()]);

        $this->aggregator->updateReferenceState($reference->entity, $id, $reference->target);
    }

    public function setOverrideIdle(
        int|string $entity,
        int $id,
        int $begin,
        int $actor,
        int $timestamp,
        bool $flag
    ) {
        $override = $this->findOrFail(Override::class, [
            'entity' => $this->getEntityId($entity),
            'id' => $id,
            'begin' => $begin,
            'actor' => $actor,
            'timestamp' => $timestamp,
        ]);

        if ($override->idle > 0 == $flag) {
            return;
        }

        $this->mapper->update($override, ['idle' => $flag ? time() : 0]);

        $this->aggregator->updateOverrideAggregation($override->entity, $override->id);
    }

    public function setReferenceEnd(
        int|string $entity,
        int $id,
        int|string $target,
        int $targetId,
        int $begin,
        int $actor,
        int $timestamp,
        int $end,
    ) {
        $reference = $this->findOrFail(Reference::class, [
            'entity' => $this->getEntityId($entity),
            'id' => $id,
            'target' => $this->getEntityId($target),
            'targetId' => $targetId,
            'begin' => $begin,
            'actor' => $actor,
            'timestamp' => $timestamp,
        ]);

        if ($reference->end != $end) {
            $this->mapper->update($reference, ['end' => $end]);
            $this->aggregator->updateReferenceState($reference->entity, $id, $target);
        }
    }

    public function setOverrideEnd(
        int|string $entity,
        int $id,
        int $begin,
        int $actor,
        int $timestamp,
        int $end
    ) {
        $override = $this->findOrFail(Override::class, [
            'entity' => $this->getEntityId($entity),
            'id' => $id,
            'begin' => $begin,
            'actor' => $actor,
            'timestamp' => $timestamp,
        ]);

        if ($override->end != $end) {
            $this->mapper->update($override, ['end' => $end]);
            $this->aggregator->updateOverrideAggregation($entity, $id);
        }
    }


    public function link(array $link): void
    {
        $link = $this->parseConfig($link);

        $config = [];
        foreach ($link as $entity => $id) {
            if (!in_array($entity, ['begin', 'end', 'data'])) {
                $config[$entity] = $id;
            }
        }

        ksort($config);
        $node = null;

        foreach (array_keys($config) as $i => $entity) {
            $id = $config[$entity];
            $spaceId = $this->getEntityId($entity);
            $params = [
                'entity' => $spaceId,
                'entityId' => $id,
                'parent' => $node ? $node->id : 0,
                'data' => [],
            ];
            if (count($config) == $i + 1) {
                $params['begin'] = $link['begin'];
                $params['timestamp'] = 0;
            }
            $node = $this->findOrCreate(Link::class, [
                'entity' => $spaceId,
                'entityId' => $id,
                'parent' => $node ? $node->id : 0,
                'begin' => array_key_exists('begin', $params) ? $params['begin'] : 0,
                'timestamp' => 0,
                'actor' => $this->actor

            ], $params);
        }

        if (!$node || !$node->parent) {
            throw new Exception("Invalid link configuration");
        }

        $this->mapper->update($node, [
            'begin' => $link['begin'],
            'end' => $link['end'],
            'actor' => $this->actor,
            'timestamp' => Carbon::now()->timestamp,
        ]);

        if (array_key_exists('data', $link)) {
            $this->mapper->update($node, ['data' => $link['data']]);
        }

        $this->aggregator->updateLinkAggregation($node);
    }

    public function getActor(): int
    {
        return $this->actor;
    }

    public function setActor(int $actor): self
    {
        $this->actor = $actor;
        return $this;
    }

    private function getTimestamp(int|string $string): int
    {
        if (Carbon::hasTestNow() || !array_key_exists($string, $this->timestamps)) {
            if (strlen('' . $string) == 8 && is_numeric($string)) {
                $value = Carbon::createFromFormat('Ymd', $string)->setTime(0, 0, 0)->timestamp;
            } else {
                $value = Carbon::parse($string)->timestamp;
            }
            if (Carbon::hasTestNow()) {
                return $value;
            }
            $this->timestamps[$string] = $value;
        }
        return $this->timestamps[$string];
    }

    private function parseConfig(array $data): array
    {
        if (!$this->actor) {
            throw new Exception("actor is undefined");
        }

        if (array_key_exists('actor', $data)) {
            throw new Exception("actor is defined");
        }

        if (array_key_exists('timestamp', $data)) {
            throw new Exception("timestamp is defined");
        }

        foreach (['begin', 'end'] as $field) {
            if (array_key_exists($field, $data) && $data[$field]) {
                $data[$field] = $this->getTimestamp($data[$field]);
            } else {
                $data[$field] = 0;
            }
        }

        return $data;
    }
}
