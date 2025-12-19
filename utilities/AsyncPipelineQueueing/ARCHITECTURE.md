# System Architecture Diagram

## Priority-Based Async Pipeline Queue Flow

```
┌─────────────────────────────────────────────────────────────────────┐
│                    PIPELINE REQUEST FLOW                             │
└─────────────────────────────────────────────────────────────────────┘

Request Entry
    │
    ├── Priority: HIGH ─────┐
    ├── Priority: NORMAL ───┤
    └── Priority: LOW ──────┘
                │
                ▼
        ┌───────────────┐
        │ Check Capacity│
        └───────┬───────┘
                │
        ┌───────┴───────┐
        │               │
    AT CAPACITY    CAPACITY AVAILABLE
        │               │
        ▼               ▼
┌───────────────┐   ┌──────────────┐
│Priority Logic │   │Execute Immed.│
└───────┬───────┘   └──────────────┘
        │
┌───────┴───────────────┐
│                       │
│  HIGH Priority        │  LOW Priority       │  NORMAL Priority
│  Try Preempt          │  Block & Queue      │  Queue & Wait
│      │                │       │             │       │
│      ▼                │       ▼             │       ▼
│ ┌──────────┐         │  ┌────────┐         │  ┌────────┐
│ │Find Low  │         │  │Enqueue │         │  │Enqueue │
│ │Priority  │         │  │& Wait  │         │  │& Wait  │
│ │Running   │         │  └────────┘         │  └────────┘
│ └────┬─────┘         │                     │
│      │               │                     │
│  ┌───┴────┐          │                     │
│  │Cancel  │          │                     │
│  │& Reque │          │                     │
│  └───┬────┘          │                     │
│      │               │                     │
└──────┴───────────────┴─────────────────────┘
        │
        ▼
┌───────────────────┐
│Execute Pipeline   │
│(Semaphore Locked) │
└─────────┬─────────┘
          │
          ▼
┌───────────────────┐
│Pipeline Completed │
└─────────┬─────────┘
          │
          ▼
┌───────────────────┐
│Release Semaphore  │
└─────────┬─────────┘
          │
          ▼
┌───────────────────┐
│Process Next       │
│(Priority Sorted)  │
└───────────────────┘
```

## Preemption Decision Flow

```
┌─────────────────────────────────────────┐
│  HIGH PRIORITY REQUEST AT CAPACITY      │
└──────────────┬──────────────────────────┘
               │
               ▼
    ┌──────────────────────┐
    │Find Candidate to      │
    │Preempt:               │
    │ 1. Lower priority     │
    │ 2. Preempt count < max│
    │ 3. Prefer fewer       │
    │    preemptions        │
    │ 4. Prefer older       │
    └──────────┬────────────┘
               │
         ┌─────┴─────┐
         │           │
    FOUND         NOT FOUND
         │           │
         ▼           ▼
    ┌────────┐  ┌────────┐
    │Cancel  │  │Queue &│
    │Running │  │Wait   │
    │Pipeline│  └────────┘
    └────┬───┘
         │
         ▼
    ┌────────────┐
    │Increment   │
    │Preemption  │
    │Counter     │
    └────┬───────┘
         │
         ▼
    ┌────────────┐
    │Re-queue    │
    │Cancelled   │
    │Request     │
    └────┬───────┘
         │
         ▼
    ┌────────────┐
    │Execute New │
    │Request     │
    └────────────┘
```

## Queue Processing Priority

```
┌──────────────────────────────────────┐
│    WAITING QUEUE (FIFO + Priority)   │
└──────────────┬───────────────────────┘
               │
               ▼
    ┌──────────────────┐
    │Sort by:          │
    │ 1. Priority ↓    │
    │    (High→Low)    │
    │ 2. Created At ↓  │
    │    (New→Old)     │
    │ 3. Preempt # ↑   │
    │    (Few→Many)    │
    └────────┬─────────┘
             │
             ▼
    ┌────────────────┐
    │Select Next     │
    │Request         │
    └────────┬───────┘
             │
             ▼
    ┌────────────────┐
    │Execute         │
    └────────────────┘
```

## Fairness Mechanism

```
Pipeline Request State:
┌──────────────────────────────────────┐
│ Preemption Counter: 0                │ ← Initial state
└──────────────────────────────────────┘
               │ Cancelled by high priority
               ▼
┌──────────────────────────────────────┐
│ Preemption Counter: 1                │ ← Can be preempted again
│ Re-queued for execution              │
└──────────────────────────────────────┘
               │ Cancelled again
               ▼
┌──────────────────────────────────────┐
│ Preemption Counter: 2 (MAX)          │ ← PROTECTED
│ Cannot be preempted anymore!         │ ← Fairness guaranteed
└──────────────────────────────────────┘
               │ Must execute
               ▼
┌──────────────────────────────────────┐
│ EXECUTION GUARANTEED                 │
└──────────────────────────────────────┘
```

## Component Interaction

```
┌──────────────────────────────────────────────────────────┐
│              PriorityAsyncPipelineQueue                  │
│  ┌────────────────────────────────────────────────────┐  │
│  │ Configuration:                                     │  │
│  │  - maxConcurrentPipelines: int (e.g., 5)          │  │
│  │  - maxPreemptions: int (e.g., 2)                  │  │
│  └────────────────────────────────────────────────────┘  │
│                                                           │
│  ┌─────────────────┐  ┌─────────────────────────────┐   │
│  │   Semaphore     │  │  Running Pipelines          │   │
│  │   (Concurrency) │  │  ConcurrentDictionary       │   │
│  │   Max: N slots  │  │  <Guid, PipelineRequest>    │   │
│  └─────────────────┘  └─────────────────────────────┘   │
│                                                           │
│  ┌──────────────────────────────────────────────────┐   │
│  │  Waiting Queue (ConcurrentQueue)                 │   │
│  │  - Sorted by priority, creation time             │   │
│  │  - FIFO within same priority                     │   │
│  └──────────────────────────────────────────────────┘   │
└──────────────────────────────────────────────────────────┘
```

## Thread Safety Guarantees

```
┌────────────────────────────────────────┐
│ Thread-Safe Operations:               │
│                                        │
│ ✓ EnqueuePipelineAsync()              │
│   - Multiple threads can enqueue      │
│                                        │
│ ✓ Running/Queued Count Properties     │
│   - Safe to read from any thread      │
│                                        │
│ ✓ Preemption Logic                    │
│   - Lock-protected critical section   │
│                                        │
│ ✓ Queue Processing                    │
│   - Atomic operations                 │
│                                        │
│ ✓ Dispose                             │
│   - Can be called once, safely        │
└────────────────────────────────────────┘

Protection Mechanisms:
┌────────────────────────────────────────┐
│ • Semaphore (concurrency control)      │
│ • ConcurrentDictionary (running)       │
│ • ConcurrentQueue (waiting)            │
│ • Lock statement (critical sections)   │
│ • Interlocked operations (counters)    │
└────────────────────────────────────────┘
```

## Example Scenario Timeline

```
Time    Event                           Running      Queued
────────────────────────────────────────────────────────────
T0      [Normal A] Enqueue              [A]          []
T1      [Normal B] Enqueue              [A,B]        []
T2      [Low C] Enqueue                 [A,B]        [C]     ← Capacity full
T3      [High D] Enqueue                [A,D]        [C]     ← B preempted
        → B cancelled & re-queued                    [C,B]
T4      [Normal A] Completes            [D]          [C,B]
T5      Process next from queue         [D,C]        [B]     ← C gets slot
T6      [High D] Completes              [C]          [B]
T7      Process next from queue         [C,B]        []      ← B finally runs
T8      All complete                    []           []
```

## Performance Characteristics

```
┌─────────────────────────────────────────────────────┐
│ Operation          │ Time Complexity │ Notes       │
├─────────────────────────────────────────────────────┤
│ Enqueue (direct)   │ O(1)           │ If capacity  │
│ Enqueue (queued)   │ O(Q)           │ With sorting │
│ Dequeue/Process    │ O(Q log Q)     │ Priority sort│
│ Find Preempt       │ O(R)           │ R = running  │
│ Memory Usage       │ O(R + Q)       │ Linear       │
└─────────────────────────────────────────────────────┘

Where:
- Q = Number of queued pipelines
- R = Number of running pipelines (≤ maxConcurrent)
```
