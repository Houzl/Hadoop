# Genetic Algorithm Using Hadoop

### There are 4 steps to solve genetic algorithm:

1.	Initialization
I used GA Pool Builder to randomly produce a desired number of valid chromosomes.

2.	Selection
I randomly select 2 chromosomes, which could be considered as Tournament selection with k=1. I also tried to implement Tournament selection with k =5 or 10, and Fitness proportionate selection. They didn’t bring a significate improvement for our problem. And the cost is very expensive.

3.	Genetic operators
They are two operators: Crossover and Mutate
For crossover, it is gene based, after crossover, the new chromosomes are always valid.
For Mutate, it is bit based, it may be invalid. If the new chromosome is invalid, I kept the original chromosome in next generation. So the pool always have the fixed number of chromosomes

4.	Termination
In my implement there are 2 conditions:
a.	A solution is found that satisfies minimum criteria
b.	Fixed number of generations reached, it also called max number of iterations.


### I used 4 MapReduce job to solve this task:

1.	GA Pool Builder
I used a mapper class and a costumed InputFormat, SleepInputFormat.class which is learned from org.apache.hadoop.examples.SleepJob.SleepInputFormat, to use n mapper, produce m chromosomes without input.  I used org.apache.hadoop.mapred.lib.IdentityReducer.class as reduce class, which will only out the key-value pair directly with one task.
In this step, I used n mapper parallel produce n*m chromosomes in total. 
The number of mapper and the number of chromosomes will produce per mapper could be set from command line.

2.	GA Iteration
I used NLineInputFormat.class as InputFormat to let the mapper parallel work. I had n*m chromosomes. I set up mapred.line.input.format.linespermap to m, and let each mapper only work with m lines. Which means I will using n mappers.
Because I will perform Crossover, Mutate and fitnessScore. Those a big mount operations. I need parallel computing. I set setNumReduceTasks to using multiple reduces. 
I using random integer as key and chromosome, after sorted by key, it will be considered as a Tournament selection with k=1 for next Iteration.
There is a problem, although I used IntWritable as key type, but the output is unsorted by key. Furthermore, when in distributed mode, the output will be in several files.  The number of files will equal to the number of reduce. Which is not good for Tournament selection. I didn’t find a way to solve this problem, so I added an extra MapReduce job.

3.	 GA iterate merge Builder
The purpose of this job is to merge multiple output files in one, and sort each line by key.
I used NLineInputFormat.class as InputFormat to let the mapper parallel work. And used org.apache.hadoop.mapred.lib.IdentityReducer.class as reduce to directly output.

4.	GA viewer Builder
Output last generation, the target or the closest will be on top. Order by my modified fitness score. And the the closest chromosome to target will be on top.
I didn’t using fitness Function given in the assignment, because target and target-1 will have the some fitnessScore. Actually, in our task, I think target-i and target+i should have same fitnessScore and target have the highest or lowest fitness score. In my implement, I used Math.abs(target - score) as fitness Function, the more close to the target the more close to 0. And the target’s fitnessscore is 0. 

### Command:

./bin/hadoop jar ./h.GeneticAlgorithm.Runner target chromosomesLength crossoverRate mutateRate maxIteration numMapper numReducer countPerMapper outputPath

1. double target, the chromosome that we want to find.
2. int chromosomesLength, how many genes the chromosome will contain.
3. double crossoverRate, the rate of crossover
4. double mutateRate, the rate of mutate
5. int maxIteration, the max number of iterations it’s termination condition. When it reached, the result will be 6. outputted no matter if the target had been found. 
6. int numMapper, how many mappers will be used in all for jobs.
7. int numReducer, how many reducers will be used in iteration job.
8. int countPerMapper, how many chromosomes will be produced by each mapper in pool builder job.
9. String outputPath, the output path, if it's exist, will delete it first.
