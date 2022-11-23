 #!/bin/zsh
 # Usage:
 # This script cleans up all the dead containers, images, cruft that Docker likes to leave around.
 # Reference: https://gist.github.com/bastman/5b57ddb3c11942094f8d0a97d461b430
#prune system
docker system prune --all --volumes --force
#clean network
docker network rm $(docker network ls | grep "bridge" | awk '/ / { print $1 }')
#remove images
docker rmi $(docker images --filter "dangling=true" -q --no-trunc)
docker rmi $(docker images | grep "none" | awk '/ / { print $3 }')
#remove containers
docker rm $(docker ps -qa --no-trunc --filter "status=exited")
echo "Done."

