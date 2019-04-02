#!/bin/bash

# Deployer les machines
serveurs=("zztop" "portreal" "tully" "arryn" "dorne" "carpe" "salameche" "rattata" "roucool" "pikachu") #ewok portreal tully stark arryn dorne




for serveur in ${serveurs[@]} 
do
 ssh tdarget@$serveur /home/tdarget/workspace_projet_hidoop/projet_hidoop/scripts/runDN.sh &
 ssh tdarget@$serveur /home/tdarget/workspace_projet_hidoop/projet_hidoop/scripts/runD.sh &
done

