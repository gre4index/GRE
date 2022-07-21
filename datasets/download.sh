for d in covid libio genome osm books fb history planet stack wise
do
wget --no-check-certificate -nc https://www.cse.cuhk.edu.hk/mlsys/gre/$d
done

if [ "$1" = "extra" ]; then
for d in wiki eth gnomad planetways rev
do
wget --no-check-certificate -nc https://www.cse.cuhk.edu.hk/mlsys/gre/$d
done
fi
