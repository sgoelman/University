#!/bin/bash

OIFS=$IFS;
IFS=",";

echo "Read pip dependencies"
pip_dependencies=`cat pip-dependencies.txt`

echo "Read pip3 dependencies"
pip3_dependencies=`cat pip3-dependencies.txt`

echo "Concatenate pip & pip3 depemdencies into a single array"
dependencies_array=("${pip_dependencies[@]}" "${pip3_dependencies[@]}")

for dependency in $dependencies_array
do
	echo "check if $dependency is installed"
	pip3 list | grep -F $dependency
	if [ $? -eq 0 ]
	then
		echo "$dependency is already installed"
	else	
		echo "installing $dependency"
		pip3 install $dependency		
		if [ $? -eq 0 ]
		then
			echo "$dependency has been successfully installed"
		else
			echo "Failed to install $dependency" >&2
			exit 1
		fi
	fi
done

IFS=$OIFS;
