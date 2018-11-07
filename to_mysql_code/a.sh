#!/bin/sh
# gzip SPTCC-20160704.csv.gz -d ./
for file in $(ls ./201607/*.gz)
do
echo $file;
gzip $file -d ./
done

for file in $(ls ./201608/*.gz)
do
echo $file;
gzip $file -d ./
done

for file in $(ls ./201609/*.gz)
do
echo $file;
gzip $file -d ./
done

for file in $(ls ./201801/*.Z)
do
echo $file;
gzip $file -d ./
done

for file in $(ls ./201802/*.Z)
do
echo $file;
gzip $file -d ./
done

for file in $(ls ./201803/*.Z)
do
echo $file;
gzip $file -d ./
done
