#!/bin/bash
hadoop fs -rm -r /jar/analyzer.jar
hadoop fs -put /DATA/analyzer.jar /jar
exit
