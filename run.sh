#!/bin/bash
export PYTHONDONTWRITEBYTECODE="True"
source /opt/ansible/bin/activate
ansible-playbook -i inventory.yml playbooks/test.yml




