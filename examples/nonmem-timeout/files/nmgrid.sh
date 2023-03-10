#!/bin/bash -e
chmod +x nonmem_wrap.sh
nmver=750
nmrun=theopdg_cmd1
nmstdout=theopdg_cmd1.lst
rundata=theopd.dat
nmfepath=nmfe75
nmruncmd="$nmfepath $nmrun.ctl $nmrun.lst -parafile=grid_wrap.pnm [nodes]=4 -maxlim=3 -nmexec=$nmrun.exe -runpdir=temp.dir"
start=$(date +"%s")
$nmruncmd
end=$(date +"%s")
let duration=${end}-${start}
echo "Total NONMEM $nmfepath $nmrun= ${duration} sec">>$nmstdout
rm -rf temp.dir
rm -rf worker*
rm -f *.exe
rm -f fort*
rm -f FDATA.csv
rm -f FREPL
rm -f FORIG
rm -f WK_*
if [ -f $nmrun.log ]; then mv $nmrun.log $nmrun_mpi.log; fi
zip -qrD9 output.zip * -x nul -x $rundata
