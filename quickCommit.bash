#!/bin/bash
git add . -A
git commit -m "$1"
git push master
#ssh bioseq.info 'cd Cosmos; git pull'
