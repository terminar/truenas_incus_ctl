#zfs set readonly=off boot-pool/ROOT/25.10.0/usr
#cd /usr/lib/python3/dist-packages
#patch -p3 --dry-run < /root/middlewared-25.10-session-leak.patch && patch -p3 < /root/middlewared-25.10-session-leak.patch
#patch -p3 --dry-run < /root/middlewared-25.10-rate-limit.patch && patch -p3 < /root/middlewared-25.10-rate-limit.patch
#patch -p3 --dry-run < /root/middlewared-25.10-snapshot-query-scope.patch && patch -p3 < /root/middlewared-25.10-snapshot-query-scope.patch
#zfs set readonly=on boot-pool/ROOT/25.10.0/usr
#service middlewared restart

