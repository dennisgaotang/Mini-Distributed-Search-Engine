#!/bin/bash

# Define the path to the .pem key
pem_key="/Users/tanggao/Desktop/cis5550_internet_and_web_system/project_EC2/cis5550_pemKey/cis5550.pem"

#launch kvscoordinator

# Define the IP address of your EC2 instance
coordinator_ip="54.234.126.155"

coordinator_ssh_command="ssh ec2-user@$coordinator_ip -i $pem_key"
coordinator_ec2_command="sudo kill \$(sudo lsof -t -i :80) ; cd project_crawl/ ; sudo java -cp bin:lib/webserver.jar:lib/kvs.jar cis5550.kvs.Coordinator 80"

# Create a shell script for launching the kvscoordinator
echo "#!/bin/bash" > kvscoordinator.sh
echo "$coordinator_ssh_command \"$coordinator_ec2_command\"" >> kvscoordinator.sh
# Make the shell script executable
chmod +x kvscoordinator.sh
# Open a new terminal window and execute the shell script
open -a Terminal kvscoordinator.sh
sleep 2


#launching kvsworkers
# Define the number of workers
workers=8
# Define the IP addresses of your EC2 instances
ips=(
    "54.210.236.188"
    "18.212.123.245"
    "54.80.213.221"
    "54.80.145.14"
    "18.232.131.9"
    "54.224.187.206"
    "54.162.195.185"
    "184.72.136.215"
)

# Loop through each worker and create a shell script to execute the command
for ((i=0; i<workers; i++)); do
    # Define the SSH command to connect to the EC2 instance and execute the command
    ssh_command="ssh ec2-user@${ips[$i]} -i $pem_key"

    # Define the command to execute on the EC2 instance
    ec2_command="sudo kill \$(sudo lsof -t -i :80) ; cd project_crawl/ ; sudo java -cp bin:lib/webserver.jar:lib/kvs.jar cis5550.kvs.Worker 80 worker$(($i+1)) 54.234.126.155:80"

    # Create a shell script for the worker
    echo "#!/bin/bash" > kvsworker$i.sh
    echo "$ssh_command \"$ec2_command\"" >> kvsworker$i.sh

    # Make the shell script executable
    chmod +x kvsworker$i.sh

    # Open a new terminal window and execute the shell script
    open -a Terminal kvsworker$i.sh
done
