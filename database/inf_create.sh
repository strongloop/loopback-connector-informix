#!/bin/bash

. ~/.bashrc
. ~/ifx_dev.env

mkdir /home/informix/loopback
touch /home/informix/loopback/DATADBS01.01
chmod 660 /home/informix/loopback/DATADBS01.01
$INFORMIXDIR/bin/onspaces -c -d datadbs01 -p /home/informix/loopback/DATADBS01.01 -o 0 -s 4194304
echo "create database loopback in datadbs01" | $INFORMIXDIR/bin/dbaccess -
$INFORMIXDIR/bin/dbaccess loopback /home/informix/tables.sql
