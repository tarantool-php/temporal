<?php

declare(strict_types=1);

namespace Tarantool\Temporal;

use Tarantool\Mapper\Mapper;
use Tarantool\Temporal\Entity\Link;
use Tarantool\Temporal\Entity\LinkAggregate;
use Tarantool\Temporal\Entity\OverrideAggregate;
use Tarantool\Temporal\Entity\Reference;
use Tarantool\Temporal\Entity\ReferenceAggregate;
use Tarantool\Temporal\Entity\ReferenceState;

class Aggregator
{
    public function __construct(
        public readonly Mapper $mapper,
        public bool $createReferenceAggregate = true,
    ) {
    }

    public function setReferenceAggregation(bool $value): self
    {
        $this->createReferenceAggregate = $value;
        return $this;
    }

    public function getLeafs(Link $link): array
    {
        if ($link->timestamp) {
            return [$link];
        }

        $leafs = [];
        foreach ($this->mapper->find(Link::class, ['parent' => $link->id]) as $child) {
            foreach ($this->getLeafs($child) as $leaf) {
                $leafs[] = $leaf;
            }
        }
        return $leafs;
    }

    public function updateReferenceState(int $entity, int $id, int $target): array
    {
        $params = [
            'entity' => $entity,
            'id' => $id,
            'target' => $target,
        ];

        $changes = $this->mapper->find(Reference::class, $params);
        $states = $this->generateStates($changes, function ($state, $change) {
            $state->data = $change->targetId;
        });

        $affected = [];
        foreach ($this->mapper->find(ReferenceState::class, $params) as $state) {
            $this->mapper->delete(ReferenceState::class, $state);
        }

        foreach ($states as $state) {
            $entity = $this->mapper->create(ReferenceState::class, array_merge($params, [
                'begin' => $state->begin,
                'end' => $state->end,
                'targetId' => $state->data,
            ]));
            if (!in_array([$entity->target, $entity->targetId], $affected)) {
                $affected[] = [$entity->target, $entity->targetId];
            }
        }

        if (!$this->createReferenceAggregate) {
            return $affected;
        }

        foreach ($affected as $affect) {
            list($entity, $entityId) = $affect;
            $changes = $this->mapper->find(ReferenceState::class, [
                'target' => $entity,
                'targetId' => $entityId,
                'entity' => $params['entity'],
            ]);
            $aggregates = $this->generateStates($changes, function ($state, $change) {
                if (!in_array($change->id, $state->data)) {
                    $state->data[] = $change->id;
                }
                $state->exists = false;
            });

            $aggregateParams = [
                'entity' => $entity,
                'id' => $entityId,
                'source' => $params['entity']
            ];
            foreach ($this->mapper->find(ReferenceAggregate::class, $aggregateParams) as $aggregate) {
                foreach ($aggregates as $candidate) {
                    if ($candidate->begin == $aggregate->begin && $candidate->end == $aggregate->end) {
                        if ($candidate->data == $aggregate->data) {
                            $candidate->exists = true;
                            continue 2;
                        }
                    }
                }
                $this->mapper->delete(ReferenceAggregate::class, $aggregate);
            }
            foreach ($aggregates as $aggregate) {
                if ($aggregate->exists) {
                    continue;
                }
                $this->mapper->create(ReferenceAggregate::class, array_merge($aggregateParams, [
                    'begin' => $aggregate->begin,
                    'end' => $aggregate->end,
                    'data' => $aggregate->data,
                ]));
            }
        }

        return $affected;
    }


    public function updateLinkAggregation(Link $node): void
    {
        $todo = [
            $node->entity => $node->entityId,
        ];

        $current = $node;
        while ($current->parent) {
            $current = $this->mapper->findOne(Link::class, ['id' => $current->parent]);
            $todo[$current->entity] = $current->entityId;
        }

        foreach ($todo as $entity => $id) {
            $spaceId = $entity;
            $source = $this->mapper->find(Link::class, [
                'entity'   => $spaceId,
                'entityId' => $id,
            ]);

            $leafs = [];
            foreach ($source as $node) {
                foreach ($this->getLeafs($node) as $detail) {
                    $leafs[] = $detail;
                }
            }

            $changeaxis = [];

            foreach ($leafs as $leaf) {
                $current = $leaf;
                $ref = [];

                if (property_exists($leaf, 'idle') && $leaf->idle) {
                    continue;
                }

                while ($current) {
                    if ($current->entity != $spaceId) {
                        $ref[$current->entity] = $current->entityId;
                    }
                    if ($current->parent) {
                        $current = $this->mapper->findOne(Link::class, ['id' => $current->parent]);
                    } else {
                        $current = null;
                    }
                }

                $data = [$ref];
                if (property_exists($leaf, 'data') && $leaf->data) {
                    $data[] = $leaf->data;
                }

                if (!array_key_exists($leaf->timestamp, $changeaxis)) {
                    $changeaxis[$leaf->timestamp] = [];
                }
                $changeaxis[$leaf->timestamp][] = (object) [
                    'begin' => $leaf->begin,
                    'end' => $leaf->end,
                    'data' => $data
                ];
            }

            $params = [
                'entity' => $spaceId,
                'id'     => $id,
            ];

            $timeaxis = [];
            foreach ($changeaxis as $timestamp => $changes) {
                foreach ($changes as $change) {
                    foreach (['begin', 'end'] as $field) {
                        if (!array_key_exists($change->$field, $timeaxis)) {
                            $timeaxis[$change->$field] = (object) [
                                'begin' => $change->$field,
                                'end'   => $change->$field,
                                'data'  => [],
                            ];
                        }
                    }
                }
            }

            ksort($changeaxis);
            ksort($timeaxis);

            $nextSliceId = null;
            foreach (array_reverse(array_keys($timeaxis)) as $timestamp) {
                if ($nextSliceId) {
                    $timeaxis[$timestamp]->end = $nextSliceId;
                } else {
                    $timeaxis[$timestamp]->end = 0;
                }
                $nextSliceId = $timestamp;
            }

            $states = [];
            foreach ($timeaxis as $state) {
                foreach ($changeaxis as $changes) {
                    foreach ($changes as $change) {
                        if ($change->begin > $state->begin) {
                            // future override
                            continue;
                        }
                        if ($change->end && ($change->end < $state->end || !$state->end)) {
                            // complete override
                            continue;
                        }
                        $state->data[] = $change->data;
                    }
                }
                if (count($state->data)) {
                    $states[] = array_merge(get_object_vars($state), $params);
                }
            }

            // merge states
            $clean = false;
            while (!$clean) {
                $clean = true;
                foreach ($states as $i => $state) {
                    if (array_key_exists($i + 1, $states)) {
                        $next = $states[$i + 1];
                        if (json_encode($state['data']) == json_encode($next['data'])) {
                            $states[$i]['end'] = $next['end'];
                            unset($states[$i + 1]);
                            $states = array_values($states);
                            $clean = false;
                            break;
                        }
                    }
                }
            }

            foreach ($this->mapper->find(LinkAggregate::class, $params) as $state) {
                $this->mapper->delete(LinkAggregate::class, $state);
            }

            foreach ($states as $state) {
                $this->mapper->create(LinkAggregate::class, $state);
            }
        }
    }

    public function updateOverrideAggregation(int $entity, int $id): void
    {
        $params = [
            'entity' => $entity,
            'id'     => $id,
        ];

        $changes = $this->mapper->find('_temporal_override', $params);
        $states = $this->generateStates($changes, function ($state, $change) {
            $state->data = array_merge($state->data, $change->data);
            $state->exists = false;
        });
        foreach ($this->mapper->find(OverrideAggregate::class, $params) as $aggregate) {
            foreach ($states as $state) {
                if ($state->begin == $aggregate->begin && $state->end == $aggregate->end) {
                    if ($state->data == $aggregate->data) {
                        $state->exists = true;
                        continue 2;
                    }
                }
            }
            $this->mapper->delete(OverrideAggregate::class, $aggregate);
        }
        foreach ($states as $aggregate) {
            if ($aggregate->exists) {
                continue;
            }
            $this->mapper->create(OverrideAggregate::class, array_merge($params, [
                'begin' => $aggregate->begin,
                'end' => $aggregate->end,
                'data' => $aggregate->data,
            ]));
        }
    }

    private function generateStates(array $changes, callable $callback): array
    {
        $slices = [];
        foreach ($changes as $i => $change) {
            if (property_exists($change, 'idle') && $change->idle) {
                unset($changes[$i]);
            }
        }
        foreach ($changes as $change) {
            foreach (['begin', 'end'] as $field) {
                if (!array_key_exists($change->$field, $slices)) {
                    $slices[$change->$field] = (object) [
                        'begin'  => $change->$field,
                        'end'    => $change->$field,
                        'data'   => [],
                    ];
                }
            }
        }
        ksort($slices);

        $nextSliceId = null;
        foreach (array_reverse(array_keys($slices)) as $timestamp) {
            if ($nextSliceId) {
                $slices[$timestamp]->end = $nextSliceId;
            } else {
                $slices[$timestamp]->end = 0;
            }
            $nextSliceId = $timestamp;
        }

        // calculate states
        $states = [];
        foreach ($slices as $slice) {
            foreach ($changes as $change) {
                if ($change->begin > $slice->begin) {
                    // future change
                    continue;
                }
                if ($change->end && ($change->end < $slice->end || !$slice->end)) {
                    // complete change
                    continue;
                }
                $callback($slice, $change);
            }
            if (count((array) $slice->data)) {
                $states[] = $slice;
            }
        }

        // merge states
        $clean = false;
        while (!$clean) {
            $clean = true;
            foreach ($states as $i => $state) {
                if (array_key_exists($i + 1, $states)) {
                    $next = $states[$i + 1];
                    if ($state->end && $state->end < $next->begin) {
                        // unmergable
                        continue;
                    }
                    if (json_encode($state->data) == json_encode($next->data)) {
                        $state->end = $next->end;
                        unset($states[$i + 1]);
                        $states = array_values($states);
                        $clean = false;
                        break;
                    }
                }
            }
        }

        return $states;
    }
}
