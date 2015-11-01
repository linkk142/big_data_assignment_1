# big_data_assignment_1

## Introduction

Our project is build with Maven, just run maven clean install to build the jar.
Launch the jar located in the target folder with hadoop by using this type of comand :
```
hadoop jar assignment_1-1.1.jar /path/to/input /path/to/ouput
```
Then choose which practice you want to execute or the word count example. 

## Practice 1

#### Question : Is it useful to use the reducer class as a combiner ? Justify.

The combiner is run for each line but with our input file, we won't have one value twice in one line. Furthermore, the purpose of our MapReduce is to compute location wealth, so we only need the value of the second and fourth column 

#### Bonus : Output is not really a CSV file but separator is ; in output file

## Practice 2

#### Bonus : Output is not really a CSV file but separator is ; in output file
