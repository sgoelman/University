#!/bin/bash
script_path=$(dirname $0)

# Installation configurations
python_version=3.7
jdk_version=8
spark_version=2.4.3
hadoop_version=2.7
spark_parent_folder=/opt
spark_dir=$spark_parent_folder/spark
hadoop_package=spark-$spark_version-bin-hadoop$hadoop_version
hadoop_tar=$hadoop_package.tgz

# Installation failure policy
exit_if_cannot_fetch_current_user = 0
exit_if_failed_to_add_users_to_sudoers = 0
exit_if_failed_to_grant_sudo_permissions_to_current_user = 0

echo "Get the current user"
current_user=`whoami`
if [ $? -eq 0 ]
then
	echo "current user is: $current_user."
else
	echo "Failed to get the current user." >&2
	if [ $exit_if_cannot_fetch_current_user -eq 1 ]
	then
		exit 1
	else
		echo "Continue installation..."
	fi
  exit 1
fi

echo "Check if $current_user is a member of sudoers"
getent group sudo | grep $current_user
if [ $? -eq 0 ]
then
	echo "$current_user is already a member of sudoers."
	echo "Continue installation..."
else
	echo "$current_user is not a member of sudoers."
	echo "$Adding $current_user to sudoers."
	sudo adduser $current_user sudo
	if [ $? -eq 0 ]
	then
		echo "$current_user has been successfully added to sudoers."
	else
		echo "Failed to add $current_user to sudoers." >&2
		if [ $exit_if_failed_to_add_users_to_sudoers -eq 1 ]
		then
			exit 1
		else
			echo "Continue installation with password confirmation"
		fi
	fi
fi

sudo sh -c "echo '$current_user ALL=NOPASSWD: ALL' >> /etc/sudoers"
if [ $? -eq 0 ]
then
	echo "$current_user has been granted a permission to run commands without typing passwords."
else
	echo "Failed to grant pemissions for $current_user, to run commands without typing passwords." >&2
	if [ $exit_if_failed_to_grant_sudo_permissions_to_current_user -eq 1 ]
	then
		exit 1
	else
		echo "Continue installation with password confirmation"
	fi
fi

echo "update the package lists for upgrades"
sudo apt-get update -y
if [ $? -eq 0 ]
then
	echo "List of packages for upgrade, have been successfully updated"
else
	echo "Failed to update list of packages for upgrades (sudo apt-get update - failed)" >&2
	exit 1
fi

echo "add OpenJDK's PPA & install jdk $jdk_version"
sudo add-apt-repository ppa:openjdk-r/ppa \
&& sudo apt-get update -q \
&& sudo apt install -y openjdk-$jdk_version-jdk
if [ $? -eq 0 ]
then
	echo "Jdk $jdk_version has been successfully installed"
else
	echo "Failed to install Jdk $jdk_version" >&2
	exit 1
fi

echo "download hadoop $hadoop_version distribution."
cd $spark_parent_folder
sudo wget http://apache.mivzakim.net/spark/spark-$spark_version/$hadoop_tar
if [ $? -eq 0 ]
then
	echo "hadoop$hadoop_version has been successfully downloaded"
else
	echo "Failed to download hadoop$hadoop_version" >&2
	exit 1
fi

echo "extract hadoop$hadoop_version tar file."
sudo tar -xzf $hadoop_tar
if [ $? -eq 0 ]
then
	echo "hadoop$hadoop_version tar file has been successfully extracted"
else
	echo "Failed to extract hadoop$hadoop_version tar file" >&2
	exit 1
fi

echo "move $hadoop_package to $spark_dir directory."
sudo mv $hadoop_package $spark_dir
if [ $? -eq 0 ]
then
	echo "$$hadoop_package has been successfully moved to $spark_dir directory"
else
	echo "Failed to move $hadoop_package to $spark_dir directory" >&2
	exit 1
fi

echo "updating SPARK_HOME environment variable"
echo 'export SPARK_HOME=/opt/spark' >> ~/.bashrc 
echo 'PATH=$SPARK_HOME/bin:$PATH' >> ~/.bashrc

echo "installing python3-pip"
sudo apt install python3-pip -y
if [ $? -eq 0 ]
then
	echo "python3-pip has been successfully installed"
else
	echo "Failed to install python3-pip" >&2
	echo "Removing lock files"
	sudo rm /var/lib/apt/lists/lock
	sudo rm /var/cache/apt/archives/lock
	sudo rm /var/lib/dpkg/lock*
	
	echo "installing python3-pip"
	if [ $? -eq 0 ]
	then
		echo "python3-pip has been successfully installed"
	else
		echo "Failed to install python3-pip" >&2
		exit 1
	fi
fi

echo "installing jupyter"
pip3 install jupyter
if [ $? -eq 0 ]
then
	echo "jupyter has been successfully installed"
else
	echo "Failed to install jupyter" >&2
	exit 1
fi

echo "installin python-$python_version"
sudo add-apt-repository ppa:jonathonf/python-$python_version
sudo apt-get update
sudo apt-get install python$python_version

echo "updating the following environment variables: PYSPARK_PYTHON, PYSPARK_DRIVER_PYTHON, PYSPARK_DRIVER_PYTHON_OPTS"
echo 'export PATH=$PATH:~/.local/bin/' >> ~/.bashrc 
echo 'export PYSPARK_PYTHON=python3' >> ~/.bashrc
echo 'export PYSPARK_DRIVER_PYTHON=jupyter' >> ~/.bashrc 
echo "export PYSPARK_DRIVER_PYTHON_OPTS='notebook'" >> ~/.bashrc

echo "Installing dependencies"
$script_path/install-dependencies.bash
if [ $? -eq 0 ]
then
	echo "dependencies have been successfully installed"
else
	echo "Failed to install dependencies" >&2
	exit 1
fi
