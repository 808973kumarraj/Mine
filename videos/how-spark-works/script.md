# How Apache Spark Works (script draft, ~3 min)

## 1. The problem
1. Say your data won't fit on one computer. Not even close.  
   v: a file grows until it bursts out of a laptop outline.
2. So you split it into pieces and spread them across many machines.  
   v: the file shatters into 3 blocks that fly onto 3 servers.
3. Apache Spark is the engine that runs your code on all those pieces at once. It started at UC Berkeley in 2009.  
   v: Spark logo; "AMPLab 2009 → Apache 2014" chip.

## 2. Who's who
4. Every Spark app has one boss: the driver. It runs your code and makes the plan.  
   v: driver box lights up, code snippet inside.
5. The workers are executors. They do the actual work and hold data in memory.  
   v: 3 executor boxes, each with a small memory bar.
6. A cluster manager, like YARN or Kubernetes, hands out the machines.  
   v: manager box draws lines connecting driver to executors.

## 3. Follow one job
7. Let's count words. First, Spark splits the text into partitions. One task per partition.  
   v: "the cat sat / the dog sat / the cat ran" splits into 3 colored blocks.
8. Now we write map and reduceByKey. Here's the twist: nothing runs yet.  
   v: code types out; a "waiting…" tag pulses.
9. Transformations are lazy. Spark just writes them down, like a recipe.  
   v: steps stack into a recipe card.
10. Only an action, like collect, says: go. Now Spark looks at the whole recipe and plans.  
    v: "collect()" stamps on; recipe turns into a graph.

## 4. Stages and the shuffle
11. The plan is a graph, split into stages. The split happens at a shuffle.  
    v: DAG draws left to right, a dashed wall between two stages.
12. Splitting words can happen right where the data lives. No talking needed.  
    v: each executor turns its line into word chips, locally.
13. But counting "cat" means every "cat" has to meet in one place. That's the shuffle.  
    v: word chips fly across executors, grouping by word.
14. Shuffles cost disk, network, and time. Fewer shuffles, faster jobs.  
    v: network lines glow red; a cost meter ticks up.

## 5. Why it's fast
15. Spark can keep data in memory between steps, instead of writing to disk every time.  
    v: data stays in memory bars; a disk icon greys out.
16. For jobs that loop over the same data, the original paper measured up to 20 times faster than Hadoop.  
    v: two bars, "up to 20x" counts up.
17. Use DataFrames or SQL, and an optimizer called Catalyst rewrites your plan before it runs.  
    v: messy plan reshuffles into a cleaner one.

## 6. When things break
18. And if a machine dies? Spark doesn't keep copies. It keeps the recipe.  
    v: one executor flashes red and vanishes.
19. It replays the steps for just the lost piece, and the job carries on.  
    v: lineage arrows replay; the block rebuilds on another executor.

## 7. Wrap
20. So: a driver plans, executors work, shuffles split stages, memory keeps it fast.  
    v: the four ideas snap into a summary grid.
21. Nothing runs until you ask. And when you do, Spark already knows the fastest way there.  
    v: "collect()" glows; fade to logo.
