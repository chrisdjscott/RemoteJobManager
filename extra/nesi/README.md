# NeSI extras

Script to make sure a funcx endpoint is running on one of NeSI's login nodes.

First upload the file to NeSI by running the following from a NeSI node:

```
wget ...
```

The script can be made to run periodically via scrontab, e.g. by running the
following to edit your scrontab file:

```
scrontab -e
```

and adding something like this:

```
#SCRON -t 05:00
#SCRON -J funcxcheck
#SCRON --mem=64
@hourly /home/csco212/funcx_checker/funcx-endpoint-persist-nesi.sh
```

to run the script hourly.
