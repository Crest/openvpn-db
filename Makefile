all: openvpn-db

clean:
	rm -f openvpn-db

openvpn-db: openvpn-db.c
	gcc -o openvpn-db openvpn-db.c -I/usr/local/include -L/usr/local/lib -lsqlite3
