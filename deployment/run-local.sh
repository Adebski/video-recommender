#!/usr/bin/env bash
#Handy script for running playbooks locally, example: runLocal.sh cleanKafkaCassandra.yml -K 
ansible-playbook $* -i hosts-local --connection=local -e "user=$USER" -e "hosts=local"