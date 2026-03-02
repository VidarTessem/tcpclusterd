nothing="[2a03:94e0:205b:d:2::aaaa]"
nothing2="[2a03:94e0:205b:d:2::aaab]"

# Legge til data i cluster
curl -X POST "http://$nothing:8888/cluster/write?array=devagina&key=offline&value=no"

# Lese data fra cluster
#curl "http://$nothing:8888/cluster/read?array=system&key=online"

# Se alle arrays
#curl "http://$nothing:8888/cluster/all"


# Skriv data
#curl -X POST "http://localhost:8888/cluster/write?array=system&key=online&value=yes"

# Les data
#curl "http://localhost:8888/cluster/read?array=system&key=online"

# Se alt
#curl "http://localhost:8888"
