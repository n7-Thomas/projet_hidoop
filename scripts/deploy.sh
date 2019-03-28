#!/bin/bash

# Deployer les machines
#ssh tdarget@yoda /home/tdarget/workspace_projet_hidoop/hidoop/src/runDN.sh &
#ssh tdarget@yoda /home/tdarget/workspace_projet_hidoop/hidoop/src/runD.sh &
#ssh tdarget@zztop /home/tdarget/workspace_projet_hidoop/hidoop/src/runDN.sh &
#ssh tdarget@zztop /home/tdarget/workspace_projet_hidoop/hidoop/src/runD.sh &
#ssh tdarget@chewie /home/tdarget/workspace_projet_hidoop/hidoop/src/runDN.sh &
#ssh tdarget@chewie /home/tdarget/workspace_projet_hidoop/hidoop/src/runD.sh & 
#ssh tdarget@dagobah /home/tdarget/workspace_projet_hidoop/hidoop/src/runDN.sh &
#ssh tdarget@dagobah /home/tdarget/workspace_projet_hidoop/hidoop/src/runD.sh &
#ssh tdarget@ewok /home/tdarget/workspace_projet_hidoop/hidoop/src/runDN.sh &
#ssh tdarget@ewok /home/tdarget/workspace_projet_hidoop/hidoop/src/runD.sh &
#ssh tdarget@portreal /home/tdarget/workspace_projet_hidoop/hidoop/src/runDN.sh &
#ssh tdarget@portreal /home/tdarget/workspace_projet_hidoop/hidoop/src/runD.sh &
#ssh tdarget@tully /home/tdarget/workspace_projet_hidoop/hidoop/src/runDN.sh &
#ssh tdarget@tully /home/tdarget/workspace_projet_hidoop/hidoop/src/runD.sh &
#ssh tdarget@stark /home/tdarget/workspace_projet_hidoop/hidoop/src/runDN.sh &
#ssh tdarget@stark /home/tdarget/workspace_projet_hidoop/hidoop/src/runD.sh &
#ssh tdarget@arryn /home/tdarget/workspace_projet_hidoop/hidoop/src/runDN.sh &
#ssh tdarget@arryn /home/tdarget/workspace_projet_hidoop/hidoop/src/runD.sh &
#ssh tdarget@dorne /home/tdarget/workspace_projet_hidoop/hidoop/src/runDN.sh &
#ssh tdarget@dorne /home/tdarget/workspace_projet_hidoop/hidoop/src/runD.sh &

serveurs=("acdc" "roucool" "pikachu") #ewok portreal tully stark arryn dorne




for serveur in ${serveurs[@]} 
do
 ssh tdarget@$serveur /home/tdarget/workspace_projet_hidoop/projet_hidoop/scripts/runDN.sh &
 ssh tdarget@$serveur /home/tdarget/workspace_projet_hidoop/projet_hidoop/scripts/runD.sh &
done

